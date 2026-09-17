package pool

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/mock"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/sink"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// env starts an in-process mocksources with no latency and loads the
// committed recipes against it.
func env(t *testing.T, failures bool) (*recipe.Set, *httptest.Server) {
	t.Helper()
	srv := mock.NewServer(mock.Config{Failures: failures, LatencyScale: 0})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	t.Setenv("MOCKSOURCES_URL", ts.URL)
	t.Setenv("EVENTIDE_API_KEY", "eventide-demo-key")
	set, err := recipe.Load(filepath.Join("..", "..", "recipes"))
	if err != nil {
		t.Fatal(err)
	}
	return set, ts
}

func client(ts *httptest.Server) *http.Client {
	c := ts.Client()
	c.Transport.(*http.Transport).DisableCompression = true
	return c
}

func hashes(t *testing.T, root, runID string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(filepath.Join(root, "runs", "run_id="+runID), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".jsonl.gz") {
			return err
		}
		f, _ := os.Open(p)
		defer f.Close()
		gz, _ := gzip.NewReader(f)
		dec := json.NewDecoder(gz)
		for {
			var r record.RawRecord
			if err := dec.Decode(&r); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			out = append(out, r.RecordHash)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func TestRunAllSourcesWorkersEquivalence(t *testing.T) {
	set, ts := env(t, true)
	root := t.TempDir()
	sk, _ := sink.NewFS(root)
	var got [][]string
	for _, w := range []int{1, 8} {
		r := &Runner{Sink: sk, Client: client(ts), Log: quiet}
		m, err := r.Run(context.Background(), set, Options{Workers: w, AsOf: "2026-09-01", RunID: "w" + string(rune('0'+w))})
		if err != nil {
			t.Fatal(err)
		}
		if m.Totals.Records != mock.ExpectedTotal() || m.Totals.SourcesFailed != 0 {
			t.Fatalf("workers=%d totals=%+v", w, m.Totals)
		}
		for id, want := range mock.ExpectedRaw {
			if m.Sources[id].Records != want {
				t.Errorf("workers=%d %s: %d records, want %d", w, id, m.Sources[id].Records, want)
			}
		}
		if m.Sources["delphine"].Retries == 0 {
			t.Error("delphine must have retried under failure injection")
		}
		if len(m.Sources["fathom"].Parts) != 2 {
			t.Errorf("fathom parts=%v", m.Sources["fathom"].Parts)
		}
		got = append(got, hashes(t, root, m.RunID))
	}
	if len(got[0]) != len(got[1]) {
		t.Fatalf("record counts differ: %d vs %d", len(got[0]), len(got[1]))
	}
	for i := range got[0] {
		if got[0][i] != got[1][i] {
			t.Fatal("workers=1 and workers=8 must land identical record sets")
		}
	}
	has, _ := sk.HasManifest(context.Background(), "w1")
	if !has {
		t.Fatal("manifest missing")
	}
}

func TestFailingSourceIsDataNotError(t *testing.T) {
	set, ts := env(t, true)
	t.Setenv("EVENTIDE_API_KEY", "wrong") // eventide → 401 → non-retryable → failed
	set, err := recipe.Load(filepath.Join("..", "..", "recipes"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	sk, _ := sink.NewFS(root)
	r := &Runner{Sink: sk, Client: client(ts), Log: quiet}
	m, err := r.Run(context.Background(), set, Options{Workers: 4, RunID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if m.Totals.SourcesFailed != 1 || m.Sources["eventide"].Status != record.StatusFailed || m.Status() != "partial" {
		t.Fatalf("manifest=%+v", m.Totals)
	}
	if !strings.Contains(m.Sources["eventide"].Error, "401") {
		t.Fatalf("error=%q", m.Sources["eventide"].Error)
	}
	if _, err := os.Stat(filepath.Join(root, "runs", "run_id=f", "source=eventide", "part-000.jsonl.gz")); err == nil {
		t.Fatal("failed source must not promote parts")
	}
	if m.Totals.Records != mock.ExpectedTotal()-mock.ExpectedRaw["eventide"] {
		t.Fatalf("records=%d", m.Totals.Records)
	}
}

// slowSink blocks the first write until released, so a cancellation can be
// injected mid-run deterministically.
type slowSink struct {
	sink.Sink
	release chan struct{}
	once    sync.Once
	started chan struct{}
}

type slowWriter struct {
	sink.Writer
	s *slowSink
}

func (s *slowSink) Open(ctx context.Context, runID, sid string) (sink.Writer, error) {
	w, err := s.Sink.Open(ctx, runID, sid)
	return &slowWriter{w, s}, err
}

func (w *slowWriter) Write(r *record.RawRecord) error {
	w.s.once.Do(func() { close(w.s.started) })
	<-w.s.release
	return w.Writer.Write(r)
}

func TestCancelMidRunWritesNoManifestAndNoParts(t *testing.T) {
	set, ts := env(t, false)
	root := t.TempDir()
	fs, _ := sink.NewFS(root)
	ss := &slowSink{Sink: fs, release: make(chan struct{}), started: make(chan struct{})}
	r := &Runner{Sink: ss, Client: client(ts), Log: quiet}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, set, Options{Workers: 8, RunID: "c"})
		done <- err
	}()
	<-ss.started
	cancel()
	close(ss.release)
	err := <-done
	if !errors.Is(err, ErrCancelled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	has, _ := fs.HasManifest(context.Background(), "c")
	if has {
		t.Fatal("cancelled run must not write a manifest")
	}
	var promoted []string
	filepath.WalkDir(root, func(p string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() && strings.HasSuffix(p, ".jsonl.gz") {
			promoted = append(promoted, p)
		}
		return nil
	})
	if len(promoted) != 0 {
		t.Fatalf("cancelled run promoted parts: %v", promoted)
	}
}

func TestSourceTimeoutBecomesFailure(t *testing.T) {
	set, ts := env(t, false)
	// Make aurora impossibly slow: timeout 1ms.
	for i := range set.Recipes {
		if set.Recipes[i].SourceID == "aurora" {
			set.Recipes[i].Timeout = recipe.Duration{Duration: time.Millisecond}
		}
	}
	root := t.TempDir()
	sk, _ := sink.NewFS(root)
	r := &Runner{Sink: sk, Client: client(ts), Log: quiet}
	m, err := r.Run(context.Background(), set, Options{Workers: 8, RunID: "to"})
	if err != nil {
		t.Fatal(err)
	}
	if m.Sources["aurora"].Status != record.StatusFailed || !strings.Contains(m.Sources["aurora"].Error, "deadline") {
		t.Fatalf("aurora=%+v", m.Sources["aurora"])
	}
	if m.Totals.SourcesOK != 7 {
		t.Fatalf("totals=%+v", m.Totals)
	}
}

func TestSlowConsumerCompletesWithSmallBuffers(t *testing.T) {
	// Channels are bounded by construction (PageBuffer, BatchBuffer×BatchSize);
	// a slow writer must only slow the producers, never drop or duplicate.
	set, ts := env(t, false)
	only := &recipe.Set{SHA256: set.SHA256}
	for _, rc := range set.Recipes {
		if rc.SourceID == "fathom" {
			only.Recipes = append(only.Recipes, rc)
		}
	}
	root := t.TempDir()
	fs, _ := sink.NewFS(root)
	cs := &countingSink{Sink: fs}
	r := &Runner{Sink: cs, Client: client(ts), Log: quiet, BatchBuffer: 2, BatchSize: 100}
	if _, err := r.Run(context.Background(), only, Options{Workers: 1, RunID: "bp"}); err != nil {
		t.Fatal(err)
	}
	if cs.n != mock.FathomRows {
		t.Fatalf("wrote %d", cs.n)
	}
}

type countingSink struct {
	sink.Sink
	n int
}

type countingWriter struct {
	sink.Writer
	s *countingSink
}

func (c *countingSink) Open(ctx context.Context, runID, sid string) (sink.Writer, error) {
	w, err := c.Sink.Open(ctx, runID, sid)
	return &countingWriter{w, c}, err
}

func (w *countingWriter) Write(r *record.RawRecord) error {
	w.s.n++
	time.Sleep(10 * time.Microsecond) // slow consumer
	return w.Writer.Write(r)
}
