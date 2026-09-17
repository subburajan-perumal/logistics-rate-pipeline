// Command mocksources serves the eight synthetic carrier feeds and can dump
// the exact run a healthy ingest lands (the Spark test fixture).
//
//	mocksources serve --listen :8081 [--seed 20260917] [--latency-scale 1.0] [--no-failures]
//	mocksources dump  --recipes ./recipes --out spark/tests/fixtures/raw
//	mocksources ports --out spark/config/ports.csv
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/mock"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/pool"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/shutdown"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/sink"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: mocksources serve|dump|ports [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(os.Args[2:])
	case "dump":
		err = cmdDump(os.Args[2:])
	case "ports":
		err = cmdPorts(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "usage: mocksources serve|dump|ports [flags]")
		os.Exit(2)
	}
	if err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", envOr("MOCKSOURCES_LISTEN", ":8081"), "listen address")
	cfg := mock.Config{Failures: true}
	fs.Uint64Var(&cfg.Seed, "seed", mock.DefaultSeed, "generator seed")
	fs.StringVar(&cfg.AsOf, "as-of", "2026-09-01", "as-of date (documentation)")
	fs.Float64Var(&cfg.LatencyScale, "latency-scale", 1.0, "multiply documented latencies (0 = none)")
	noFail := fs.Bool("no-failures", false, "disable delphine's injected 500s/429")
	fs.StringVar(&cfg.APIKey, "api-key", envOr("EVENTIDE_API_KEY", "eventide-demo-key"), "eventide's expected X-Api-Key")
	_ = fs.Parse(args)
	cfg.Failures = !*noFail

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv := mock.NewServer(cfg)
	hs := &http.Server{Addr: *listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := shutdown.Context(context.Background())
	defer stop()
	go func() {
		log.Info("mocksources listening", "addr", *listen, "config", srv.String())
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return hs.Shutdown(c)
}

// cmdDump runs the real pipeline against an in-process server with a fixed
// clock and run id, so the fixture is byte-stable across machines.
func cmdDump(args []string) error {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	recipesDir := fs.String("recipes", "./recipes", "recipe directory")
	out := fs.String("out", "spark/tests/fixtures/raw", "fs sink root for the fixture")
	runID := fs.String("run-id", "fixture", "fixed run id")
	_ = fs.Parse(args)

	srv := mock.NewServer(mock.Config{Failures: true, LatencyScale: 0})
	ts := httptest.NewUnstartedServer(srv.Handler())
	ln, err := net.Listen("tcp", "127.0.0.1:18081") // fixed port keeps source_ref stable in the fixture
	if err != nil {
		return err
	}
	if err := ts.Listener.Close(); err != nil {
		return err
	}
	ts.Listener = ln
	ts.Start()
	defer ts.Close()
	if err := os.Setenv("MOCKSOURCES_URL", ts.URL); err != nil {
		return err
	}
	if err := os.Setenv("EVENTIDE_API_KEY", "eventide-demo-key"); err != nil {
		return err
	}

	set, err := recipe.Load(*recipesDir)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(*out); err != nil {
		return err
	}
	sk, err := sink.NewFS(*out)
	if err != nil {
		return err
	}
	fixed := fixedClock{t: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	runner := &pool.Runner{Sink: sk, Clock: fixed, Log: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	m, err := runner.Run(context.Background(), set, pool.Options{Workers: 8, AsOf: "2026-09-01", RunID: *runID, Version: "fixture"})
	if err != nil {
		return err
	}
	if m.Totals.SourcesFailed != 0 {
		return fmt.Errorf("fixture run had %d failed sources", m.Totals.SourcesFailed)
	}
	if m.Totals.Records != mock.ExpectedTotal() {
		return fmt.Errorf("fixture landed %d records, expected %d", m.Totals.Records, mock.ExpectedTotal())
	}
	return json.NewEncoder(os.Stdout).Encode(m)
}

// fixedClock makes fetched_at and durations deterministic in fixtures.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }
func (c fixedClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// cmdPorts writes the port table Spark uses for normalization.
func cmdPorts(args []string) error {
	fs := flag.NewFlagSet("ports", flag.ExitOnError)
	out := fs.String("out", "spark/config/ports.csv", "output csv")
	_ = fs.Parse(args)
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"locode", "city", "country", "aliases"})
	for _, p := range append(append([]mock.Port{}, mock.Origins...), mock.Destinations...) {
		_ = w.Write([]string{p.Locode, p.City, p.Country, strings.Join(p.Aliases, "|")})
	}
	w.Flush()
	return w.Error()
}
