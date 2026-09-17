// Package server is ingestd's HTTP API: runs, health, metrics and pprof
// (docs/PLAN.md §8.6, D-17).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/pool"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

// RunState is what GET /runs/{id} returns.
type RunState struct {
	RunID      string           `json:"run_id"`
	Status     string           `json:"status"` // running | ok | partial | cancelled | failed
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt *time.Time       `json:"finished_at,omitempty"`
	Workers    int              `json:"workers"`
	Records    int              `json:"records"`
	Error      string           `json:"error,omitempty"`
	Manifest   *record.Manifest `json:"manifest,omitempty"`
	cancel     context.CancelFunc
}

// Server owns run state and the HTTP mux.
type Server struct {
	Runner   *pool.Runner
	Recipes  *recipe.Set
	Workers  int
	AsOf     string
	Version  string
	Log      *slog.Logger
	Gatherer prometheus.Gatherer

	mu        sync.Mutex
	runs      map[string]*RunState
	order     []string
	active    sync.WaitGroup
	draining  atomic.Bool
	ready     atomic.Bool
	readyErr  atomic.Value // string
	baseCtx   context.Context
	cancelAll context.CancelFunc
}

// New wires the server; ctx bounds every run started through the API.
func New(ctx context.Context, runner *pool.Runner, set *recipe.Set, workers int, asOf, version string, log *slog.Logger, g prometheus.Gatherer) *Server {
	c, cancel := context.WithCancel(ctx)
	s := &Server{Runner: runner, Recipes: set, Workers: workers, AsOf: asOf, Version: version, Log: log, Gatherer: g,
		runs: map[string]*RunState{}, baseCtx: c, cancelAll: cancel}
	s.readyErr.Store("")
	return s
}

// Handler builds the mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("POST /runs", s.startRun)
	mux.HandleFunc("GET /runs", s.listRuns)
	mux.HandleFunc("GET /runs/{id}", s.getRun)
	mux.HandleFunc("DELETE /runs/{id}", s.cancelRun)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/pprof/{name}", func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler(r.PathValue("name")).ServeHTTP(w, r) // includes goroutineleak on Go 1.27
	})
	return mux
}

// SetReady flips readiness; err != nil records why the pod is not ready.
func (s *Server) SetReady(err error) {
	if err != nil {
		s.readyErr.Store(err.Error())
		s.ready.Store(false)
		return
	}
	s.readyErr.Store("")
	s.ready.Store(true)
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	switch {
	case s.draining.Load():
		http.Error(w, "draining", http.StatusServiceUnavailable)
	case !s.ready.Load():
		http.Error(w, "not ready: "+s.readyErr.Load().(string), http.StatusServiceUnavailable)
	default:
		w.Write([]byte("ready\n"))
	}
}

type startRequest struct {
	Workers int    `json:"workers,omitempty"`
	AsOf    string `json:"as_of,omitempty"`
}

// StartRun launches a run in the background and returns its state.
func (s *Server) StartRun(workers int, asOf string) (*RunState, error) {
	if s.draining.Load() {
		return nil, errors.New("shutting down")
	}
	if workers <= 0 {
		workers = s.Workers
	}
	if asOf == "" {
		asOf = s.AsOf
	}
	ctx, cancel := context.WithCancel(s.baseCtx)
	st := &RunState{RunID: pool.NewRunID(time.Now()), Status: "running", StartedAt: time.Now(), Workers: workers, cancel: cancel}
	s.mu.Lock()
	s.runs[st.RunID] = st
	s.order = append(s.order, st.RunID)
	s.mu.Unlock()

	s.active.Add(1)
	go func() {
		defer s.active.Done()
		defer cancel()
		m, err := s.Runner.Run(ctx, s.Recipes, pool.Options{Workers: workers, AsOf: asOf, RunID: st.RunID, Version: s.Version})
		now := time.Now()
		s.mu.Lock()
		defer s.mu.Unlock()
		st.FinishedAt = &now
		switch {
		case errors.Is(err, pool.ErrCancelled):
			st.Status = "cancelled"
			st.Error = err.Error()
		case err != nil:
			st.Status = "failed"
			st.Error = err.Error()
		default:
			st.Status = m.Status()
			st.Records = m.Totals.Records
			st.Manifest = m
		}
	}()
	return st, nil
}

func (s *Server) startRun(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	st, err := s.StartRun(req.Workers, req.AsOf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Location", "/runs/"+st.RunID)
	w.WriteHeader(http.StatusAccepted)
	s.writeState(w, st)
}

func (s *Server) writeState(w http.ResponseWriter, st *RunState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	st, ok := s.runs[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.writeState(w, st)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*RunState, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.runs[id])
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	st, ok := s.runs[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	st.cancel()
	w.WriteHeader(http.StatusAccepted)
	s.writeState(w, st)
}

// Drain stops accepting runs, flips readiness, cancels active runs and
// waits up to grace for them to abort cleanly (D-16).
func (s *Server) Drain(grace time.Duration) (clean bool) {
	s.draining.Store(true)
	s.cancelAll()
	done := make(chan struct{})
	go func() { s.active.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		s.Log.Error("drain timed out; exiting with runs still aborting", "grace", grace)
		return false
	}
}

// ActiveRuns is used by tests and the shutdown log line.
func (s *Server) ActiveRuns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, st := range s.runs {
		if st.Status == "running" {
			n++
		}
	}
	return n
}
