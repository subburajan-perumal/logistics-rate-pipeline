// Command ingestd is the concurrent rate-feed ingestion service
// (docs/PLAN.md §8.7).
//
//	ingestd serve  --recipes DIR --sink fs|s3 [--out DIR | --bucket B] [--workers N] [--listen :8080] [--schedule 15m]
//	ingestd run    --recipes DIR --sink fs|s3 ... [--as-of DATE] [--run-id ID]
//	ingestd bench  --recipes DIR --out DIR --workers 1,2,4,8,16 --reps 5 --json FILE
//	ingestd gc     --sink fs|s3 ... --older-than 24h
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/metrics"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/pool"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/server"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/shutdown"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/sink"
)

// version is set with -ldflags "-X main.version=<git sha>".
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	case "bench":
		err = cmdBench(os.Args[2:])
	case "gc":
		err = cmdGC(os.Args[2:])
	case "version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ingestd serve|run|bench|gc|version [flags]")
}

// common flags shared by every subcommand; env vars are the defaults so the
// container needs no argument list.
type common struct {
	recipes  string
	sinkName string
	out      string
	bucket   string
	prefix   string
	region   string
	workers  int
	asOf     string
	logLevel string
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func (c *common) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.recipes, "recipes", env("INGESTD_RECIPES", "./recipes"), "directory of recipe JSON files")
	fs.StringVar(&c.sinkName, "sink", env("INGESTD_SINK", "fs"), "fs | s3")
	fs.StringVar(&c.out, "out", env("INGESTD_OUT", "./data/raw"), "fs sink root")
	fs.StringVar(&c.bucket, "bucket", env("INGESTD_BUCKET", ""), "s3 bucket")
	fs.StringVar(&c.prefix, "prefix", env("INGESTD_PREFIX", ""), "s3 key prefix")
	fs.StringVar(&c.region, "region", env("AWS_REGION", "us-east-1"), "aws region")
	fs.IntVar(&c.workers, "workers", envInt("INGESTD_WORKERS", 8), "max sources fetched concurrently")
	fs.StringVar(&c.asOf, "as-of", env("INGESTD_AS_OF", time.Now().UTC().Format("2006-01-02")), "as-of date stamped into the manifest")
	fs.StringVar(&c.logLevel, "log-level", env("INGESTD_LOG_LEVEL", "info"), "debug | info | warn | error")
}

func (c *common) logger() *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.logLevel))
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func (c *common) sink(ctx context.Context) (sink.Sink, error) {
	switch c.sinkName {
	case "fs":
		return sink.NewFS(c.out)
	case "s3":
		if c.bucket == "" {
			return nil, errors.New("--bucket / INGESTD_BUCKET is required for the s3 sink")
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.region))
		if err != nil {
			return nil, err
		}
		return sink.NewS3(s3.NewFromConfig(cfg), c.bucket, c.prefix), nil
	default:
		return nil, fmt.Errorf("unknown sink %q", c.sinkName)
	}
}

func httpClient(workers int) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = workers
	t.DisableCompression = true // recipes declare gzip explicitly
	return &http.Client{Transport: t}
}

// ---- serve ----

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	var c common
	c.bind(fs)
	listen := fs.String("listen", env("INGESTD_LISTEN", ":8080"), "listen address")
	schedule := fs.Duration("schedule", envDuration("INGESTD_SCHEDULE", 0), "start a run every interval (0 = API only)")
	grace := fs.Duration("shutdown-grace", mustDuration(env("INGESTD_SHUTDOWN_GRACE", "45s")), "time to wait for runs to abort on SIGTERM")
	_ = fs.Parse(args)
	log := c.logger()

	root, stop := shutdown.Context(context.Background())
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	set, rerr := recipe.Load(c.recipes)
	sk, serr := c.sink(root)
	runner := &pool.Runner{Sink: sk, Client: httpClient(c.workers), Log: log, Metrics: m}
	srv := server.New(root, runner, set, c.workers, c.asOf, version, log, reg)

	// Readiness: recipes valid and sink reachable. Failures keep the pod
	// out of the Service (visible in kubectl describe), the process stays up.
	switch {
	case rerr != nil:
		srv.SetReady(fmt.Errorf("recipes: %w", rerr))
		log.Error("recipes invalid; not ready", "error", rerr)
	case serr != nil:
		srv.SetReady(fmt.Errorf("sink: %w", serr))
		log.Error("sink misconfigured; not ready", "error", serr)
	default:
		go func() { // re-check the sink periodically so a fixed bucket flips ready
			for {
				err := sk.Ready(root)
				srv.SetReady(err)
				if err != nil {
					log.Warn("sink not ready", "error", err)
				}
				select {
				case <-root.Done():
					return
				case <-time.After(30 * time.Second):
				}
			}
		}()
	}

	hs := &http.Server{Addr: *listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("listening", "addr", *listen, "version", version, "workers", c.workers, "sink", c.sinkName, "go", runtime.Version())
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "error", err)
			stop()
		}
	}()
	if *schedule > 0 && rerr == nil && serr == nil {
		go func() {
			t := time.NewTicker(*schedule)
			defer t.Stop()
			for {
				select {
				case <-root.Done():
					return
				case <-t.C:
					if _, err := srv.StartRun(0, ""); err != nil {
						log.Warn("scheduled run not started", "error", err)
					}
				}
			}
		}()
	}

	clean := shutdown.Wait(root, log, *grace, func(g time.Duration) bool {
		log.Info("draining", "active_runs", srv.ActiveRuns())
		ok := srv.Drain(g)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(ctx)
		return ok
	})
	log.Info("exited", "clean", clean)
	return nil
}

func envDuration(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return d
	}
	return def
}

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 45 * time.Second
	}
	return d
}

// ---- run ----

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c common
	c.bind(fs)
	runID := fs.String("run-id", "", "fixed run id (default: generated)")
	_ = fs.Parse(args)
	log := c.logger()
	ctx, stop := shutdown.Context(context.Background())
	defer stop()
	set, err := recipe.Load(c.recipes)
	if err != nil {
		return err
	}
	sk, err := c.sink(ctx)
	if err != nil {
		return err
	}
	if err := sk.Ready(ctx); err != nil {
		return err
	}
	runner := &pool.Runner{Sink: sk, Client: httpClient(c.workers), Log: log}
	m, err := runner.Run(ctx, set, pool.Options{Workers: c.workers, AsOf: c.asOf, RunID: *runID, Version: version})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(m)
}

// ---- bench ----

type benchRun struct {
	Workers    int     `json:"workers"`
	Rep        int     `json:"rep"`
	WallMs     int64   `json:"wall_ms"`
	Records    int     `json:"records"`
	Failed     int     `json:"sources_failed"`
	PeakRSSMB  float64 `json:"peak_rss_mb"`
	HeapAllocM float64 `json:"heap_alloc_mb"`
}

type benchReport struct {
	GeneratedAt time.Time          `json:"generated_at"`
	GoVersion   string             `json:"go_version"`
	Version     string             `json:"ingestd_version"`
	Recipes     string             `json:"recipes_sha256"`
	Runs        []benchRun         `json:"runs"`
	MedianMs    map[string]int64   `json:"median_wall_ms"`
	Speedup     map[string]float64 `json:"speedup_vs_1"`
}

func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	var c common
	c.bind(fs)
	workersList := fs.String("workers-list", "1,2,4,8,16", "comma-separated worker counts")
	reps := fs.Int("reps", 5, "repetitions per worker count")
	jsonOut := fs.String("json", "", "write the report here")
	mdOut := fs.String("md", "", "write a markdown summary here")
	_ = fs.Parse(args)
	log := c.logger()
	ctx, stop := shutdown.Context(context.Background())
	defer stop()
	set, err := recipe.Load(c.recipes)
	if err != nil {
		return err
	}
	sk, err := c.sink(ctx)
	if err != nil {
		return err
	}
	var counts []int
	for _, s := range strings.Split(*workersList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return fmt.Errorf("bad workers-list: %w", err)
		}
		counts = append(counts, n)
	}
	rep := benchReport{GeneratedAt: time.Now().UTC(), GoVersion: runtime.Version(), Version: version, Recipes: set.SHA256,
		MedianMs: map[string]int64{}, Speedup: map[string]float64{}}
	quiet := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	for _, w := range counts {
		var walls []int64
		for i := 1; i <= *reps; i++ {
			runner := &pool.Runner{Sink: sk, Client: httpClient(w), Log: quiet}
			runtime.GC()
			start := time.Now()
			m, err := runner.Run(ctx, set, pool.Options{Workers: w, AsOf: c.asOf, Version: version})
			if err != nil {
				return err
			}
			wall := time.Since(start).Milliseconds()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			br := benchRun{Workers: w, Rep: i, WallMs: wall, Records: m.Totals.Records, Failed: m.Totals.SourcesFailed,
				PeakRSSMB: peakRSSMB(), HeapAllocM: float64(ms.HeapAlloc) / 1e6}
			rep.Runs = append(rep.Runs, br)
			walls = append(walls, wall)
			log.Info("bench run", "workers", w, "rep", i, "wall_ms", wall, "records", m.Totals.Records, "failed", m.Totals.SourcesFailed)
		}
		sort.Slice(walls, func(i, j int) bool { return walls[i] < walls[j] })
		rep.MedianMs[strconv.Itoa(w)] = walls[len(walls)/2]
	}
	if base, ok := rep.MedianMs["1"]; ok && base > 0 {
		for k, v := range rep.MedianMs {
			rep.Speedup[k] = float64(base) / float64(v)
		}
	}
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			return err
		}
	}
	md := renderBenchMD(rep, counts)
	if *mdOut != "" {
		if err := os.WriteFile(*mdOut, []byte(md), 0o644); err != nil {
			return err
		}
	}
	fmt.Print(md)
	return nil
}

func renderBenchMD(rep benchReport, counts []int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Worker-pool benchmark — %s\n\n", rep.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "Go %s · ingestd %s · recipes %s · %d reps per worker count · median wall time\n\n", rep.GoVersion, rep.Version, rep.Recipes[:12], len(rep.Runs)/len(counts))
	b.WriteString("| workers | median wall (s) | speedup vs 1 | records/run | peak RSS (MB) |\n|---|---|---|---|---|\n")
	for _, w := range counts {
		k := strconv.Itoa(w)
		var rss float64
		var recs int
		for _, r := range rep.Runs {
			if r.Workers == w {
				if r.PeakRSSMB > rss {
					rss = r.PeakRSSMB
				}
				recs = r.Records
			}
		}
		fmt.Fprintf(&b, "| %d | %.2f | %.2fx | %d | %.0f |\n", w, float64(rep.MedianMs[k])/1000, rep.Speedup[k], recs, rss)
	}
	return b.String()
}

// ---- gc ----

func cmdGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	var c common
	c.bind(fs)
	older := fs.Duration("older-than", 24*time.Hour, "delete temp parts older than this")
	_ = fs.Parse(args)
	ctx := context.Background()
	sk, err := c.sink(ctx)
	if err != nil {
		return err
	}
	n, err := sk.GC(ctx, *older)
	if err != nil {
		return err
	}
	fmt.Printf("removed %d temporary object(s)\n", n)
	return nil
}
