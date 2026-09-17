// Package pool runs one ingestion: fan-out over sources bounded by a worker
// limit, a fetch → parse → write pipeline inside each source, and fan-in of
// per-source reports into a manifest (docs/PLAN.md §8.2, D-12).
package pool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/metrics"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/parse"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/sink"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/source"
)

// Options configure one run.
type Options struct {
	Workers int
	AsOf    string
	RunID   string // optional; generated when empty
	Version string // ingestd build version stamped into the manifest
}

// Runner holds the dependencies shared by runs.
type Runner struct {
	Sink    sink.Sink
	Client  *http.Client
	Clock   source.Clock
	Log     *slog.Logger
	Metrics *metrics.M
	// NewFetcher lets tests inject deterministic jitter; nil = source.New.
	NewFetcher func(rc recipe.Recipe) *source.Fetcher
	// Channel capacities (D-12): bounded so a slow sink applies backpressure.
	PageBuffer  int // pages in flight ahead of the parser (default 2)
	BatchBuffer int // record batches ahead of the writer (default 4)
	BatchSize   int // records per batch (default 500)
}

// ErrCancelled is returned when the run context was cancelled before the
// manifest could be written; the run is incomplete by design.
var ErrCancelled = errors.New("run cancelled")

// NewRunID returns a sortable, unique run id.
func NewRunID(now time.Time) string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

func (r *Runner) defaults() {
	if r.Client == nil {
		r.Client = &http.Client{}
	}
	if r.Clock == nil {
		r.Clock = source.RealClock{}
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.Metrics == nil {
		r.Metrics = metrics.Nop()
	}
	if r.NewFetcher == nil {
		r.NewFetcher = func(rc recipe.Recipe) *source.Fetcher { return source.New(r.Client, r.Clock, rc) }
	}
	if r.PageBuffer <= 0 {
		r.PageBuffer = 2
	}
	if r.BatchBuffer <= 0 {
		r.BatchBuffer = 4
	}
	if r.BatchSize <= 0 {
		r.BatchSize = 500
	}
}

// Run ingests every recipe and writes the manifest. A source failure is data
// in the manifest, not an error. A cancelled context returns ErrCancelled
// (wrapping ctx.Err()) and writes no manifest.
func (r *Runner) Run(ctx context.Context, set *recipe.Set, opts Options) (*record.Manifest, error) {
	r.defaults()
	if opts.Workers <= 0 {
		opts.Workers = 8
	}
	started := r.Clock.Now()
	if opts.RunID == "" {
		opts.RunID = NewRunID(started)
	}
	log := r.Log.With("run_id", opts.RunID)
	log.Info("run started", "sources", len(set.Recipes), "workers", opts.Workers, "as_of", opts.AsOf)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Workers)                                    // fan-out bound
	reports := make(chan record.SourceReport, len(set.Recipes)) // fan-in

	for _, rc := range set.Recipes {
		g.Go(func() error {
			sctx, scancel := context.WithTimeout(gctx, rc.Timeout.Duration)
			defer scancel()
			r.Metrics.InflightSources.Inc()
			rep := r.runSource(sctx, log, rc, opts)
			r.Metrics.InflightSources.Dec()
			reports <- rep
			return nil // failures are data, never abort the group
		})
	}
	go func() {
		_ = g.Wait()
		close(reports)
	}()

	m := &record.Manifest{
		RunID: opts.RunID, StartedAt: started, Workers: opts.Workers, AsOf: opts.AsOf,
		Sources: map[string]record.SourceReport{}, IngestdVersion: opts.Version, RecipesSHA256: set.SHA256,
	}
	for rep := range reports { // blocks until every source has reported
		m.Sources[rep.SourceID] = rep
		m.Totals.Records += rep.Records
		if rep.Status == record.StatusOK {
			m.Totals.SourcesOK++
		} else {
			m.Totals.SourcesFailed++
		}
		r.Metrics.RecordsTotal.WithLabelValues(rep.SourceID).Add(float64(rep.Records))
		r.Metrics.SourceDuration.WithLabelValues(rep.SourceID, rep.Status).Observe(float64(rep.DurationMs) / 1000)
	}
	m.FinishedAt = r.Clock.Now()

	if err := ctx.Err(); err != nil {
		r.Metrics.RunsTotal.WithLabelValues("cancelled").Inc()
		log.Warn("run cancelled; no manifest written", "records_discarded", m.Totals.Records)
		return nil, fmt.Errorf("%w: %w", ErrCancelled, err)
	}
	if err := r.Sink.WriteManifest(ctx, m); err != nil {
		r.Metrics.RunsTotal.WithLabelValues("failed").Inc()
		return nil, fmt.Errorf("write manifest: %w", err)
	}
	r.Metrics.RunsTotal.WithLabelValues(m.Status()).Inc()
	r.Metrics.RunDuration.WithLabelValues(metrics.Workers(opts.Workers)).Observe(m.FinishedAt.Sub(started).Seconds())
	log.Info("run finished", "status", m.Status(), "records", m.Totals.Records,
		"sources_ok", m.Totals.SourcesOK, "sources_failed", m.Totals.SourcesFailed,
		"duration_ms", m.FinishedAt.Sub(started).Milliseconds())
	return m, nil
}

type page struct {
	number int
	url    string
	resp   *http.Response
}

// runSource runs the three-stage pipeline for one recipe. It never panics
// the run: every failure becomes a SourceReport with Status failed.
func (r *Runner) runSource(ctx context.Context, log *slog.Logger, rc recipe.Recipe, opts Options) (rep record.SourceReport) {
	start := r.Clock.Now()
	log = log.With("source_id", rc.SourceID)
	rep = record.SourceReport{SourceID: rc.SourceID, Status: record.StatusOK, Parts: []string{}}
	defer func() { rep.DurationMs = r.Clock.Now().Sub(start).Milliseconds() }()

	w, err := r.Sink.Open(ctx, opts.RunID, rc.SourceID)
	if err != nil {
		return fail(rep, log, err)
	}

	f := r.NewFetcher(rc)
	f.OnResponse = func(code int) { r.Metrics.HTTPRequests.WithLabelValues(rc.SourceID, strconv.Itoa(code)).Inc() }
	f.OnRetry = func(reason string) {
		r.Metrics.RetriesTotal.WithLabelValues(rc.SourceID, reason).Inc()
		log.Warn("retrying", "reason", reason)
	}
	st := &source.Stats{}

	pages := make(chan page, r.PageBuffer)
	batches := make(chan []*record.RawRecord, r.BatchBuffer)
	nextCh := make(chan string, 1) // parser → fetcher: next page/cursor from the body

	ctx, cancelSource := context.WithCancel(ctx)
	defer cancelSource()
	g, gctx := errgroup.WithContext(ctx)
	pagesParsed := 0 // written by the parse goroutine only; read after g.Wait()

	// Stage 1: fetch. Paginated feeds wait for the parser's "next" before
	// requesting another page (the value lives in the body); the parser and
	// writer keep running meanwhile.
	g.Go(func() error {
		defer close(pages)
		next := f.FirstPage()
		for n := 0; ; n++ {
			if rc.Pagination.Type != "none" && n >= rc.Pagination.MaxPages {
				return fmt.Errorf("pagination exceeded max_pages=%d", rc.Pagination.MaxPages)
			}
			u, err := f.PageURL(next)
			if err != nil {
				return err
			}
			resp, err := f.Get(gctx, u, st)
			if err != nil {
				return err
			}
			select {
			case pages <- page{number: n, url: u, resp: resp}:
			case <-gctx.Done():
				resp.Body.Close()
				return gctx.Err()
			}
			if rc.Pagination.Type == "none" {
				return nil
			}
			select {
			case next = <-nextCh:
			case <-gctx.Done():
				return gctx.Err()
			}
			if next == "" {
				return nil
			}
		}
	})

	// Stage 2: parse, streaming each body into record batches.
	g.Go(func() error {
		defer close(batches)
		defer func() { // on early exit, close bodies the fetcher already queued
			for p := range pages {
				p.resp.Body.Close()
			}
		}()
		for p := range pages {
			batch := make([]*record.RawRecord, 0, r.BatchSize)
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				select {
				case batches <- batch:
				case <-gctx.Done():
					return gctx.Err()
				}
				batch = make([]*record.RawRecord, 0, r.BatchSize)
				return nil
			}
			res, err := parse.Parse(gctx, p.resp.Body, parse.Options{
				Recipe: rc, RunID: opts.RunID, FetchedAt: r.Clock.Now(), Page: p.number, SourceRef: p.url,
			}, func(rec *record.RawRecord) error {
				batch = append(batch, rec)
				if len(batch) >= r.BatchSize {
					return flush()
				}
				return nil
			})
			p.resp.Body.Close()
			if err != nil {
				return fmt.Errorf("page %d: %w", p.number, err)
			}
			if err := flush(); err != nil {
				return err
			}
			pagesParsed++
			if rc.Pagination.Type != "none" {
				next := res.Next
				if rc.Pagination.Type == "page" && res.Records == 0 {
					next = "" // empty page ends page-numbered feeds
				}
				select {
				case nextCh <- next:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			log.Debug("page parsed", "page", p.number, "records", res.Records)
		}
		return nil
	})

	// Stage 3: write (this goroutine owns the sink writer).
	var werr error
	for batch := range batches {
		for _, rec := range batch {
			if werr = w.Write(rec); werr != nil {
				break
			}
			rep.Records++
		}
		if werr != nil {
			break
		}
	}
	if werr != nil {
		cancelSource() // stop fetch/parse, then drain so they can exit
		for range batches {
		}
	}
	err = errors.Join(werr, g.Wait())
	rep.Pages = pagesParsed
	rep.Attempts, rep.Retries = st.Attempts, st.Retries
	if err == nil && ctx.Err() != nil {
		err = ctx.Err() // run cancelled after this source finished: never promote into a run that has no manifest
	}
	if err != nil {
		_ = w.Abort(context.WithoutCancel(ctx))
		rep.Records = 0
		return fail(rep, log, err)
	}
	parts, err := w.Close(ctx)
	if err != nil {
		rep.Records = 0
		return fail(rep, log, err)
	}
	rep.Parts = parts
	log.Info("source finished", "records", rep.Records, "pages", rep.Pages, "attempts", rep.Attempts, "retries", rep.Retries)
	return rep
}

func fail(rep record.SourceReport, log *slog.Logger, err error) record.SourceReport {
	rep.Status = record.StatusFailed
	rep.Error = err.Error()
	rep.Parts = []string{}
	log.Error("source failed", "error", err)
	return rep
}

// SortedSourceIDs is a helper for deterministic manifest rendering.
func SortedSourceIDs(m *record.Manifest) []string {
	ids := make([]string, 0, len(m.Sources))
	for id := range m.Sources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
