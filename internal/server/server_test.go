package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/metrics"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/mock"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/pool"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/sink"
)

func setup(t *testing.T) (*Server, string) {
	t.Helper()
	feeds := httptest.NewServer(mock.NewServer(mock.Config{LatencyScale: 0, Failures: true}).Handler())
	t.Cleanup(feeds.Close)
	t.Setenv("MOCKSOURCES_URL", feeds.URL)
	t.Setenv("EVENTIDE_API_KEY", "eventide-demo-key")
	set, err := recipe.Load(filepath.Join("..", "..", "recipes"))
	if err != nil {
		t.Fatal(err)
	}
	sk, _ := sink.NewFS(t.TempDir())
	reg := prometheus.NewRegistry()
	runner := &pool.Runner{Sink: sk, Client: feeds.Client(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics.New(reg)}
	runner.Client.Transport.(*http.Transport).DisableCompression = true
	s := New(context.Background(), runner, set, 8, "2026-09-01", "test", slog.New(slog.NewTextHandler(io.Discard, nil)), reg)
	api := httptest.NewServer(s.Handler())
	t.Cleanup(api.Close)
	return s, api.URL
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func waitRun(t *testing.T, api, id string) RunState {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, body := get(t, api+"/runs/"+id)
		var st RunState
		if err := json.Unmarshal([]byte(body), &st); err != nil {
			t.Fatal(err)
		}
		if st.Status != "running" {
			return st
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run did not finish")
	return RunState{}
}

func TestReadinessLifecycle(t *testing.T) {
	s, api := setup(t)
	if code, body := get(t, api+"/readyz"); code != 503 || !strings.Contains(body, "not ready") {
		t.Fatalf("before SetReady: %d %s", code, body)
	}
	s.SetReady(errors.New("bucket missing"))
	if code, body := get(t, api+"/readyz"); code != 503 || !strings.Contains(body, "bucket missing") {
		t.Fatalf("%d %s", code, body)
	}
	s.SetReady(nil)
	if code, _ := get(t, api+"/readyz"); code != 200 {
		t.Fatal(code)
	}
	if code, _ := get(t, api+"/healthz"); code != 200 {
		t.Fatal(code)
	}
	s.Drain(time.Second)
	if code, body := get(t, api+"/readyz"); code != 503 || !strings.Contains(body, "draining") {
		t.Fatalf("after drain: %d %s", code, body)
	}
	resp, _ := http.Post(api+"/runs", "application/json", nil)
	if resp.StatusCode != 503 {
		t.Fatalf("runs must be refused while draining, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestRunLifecycleAndMetrics(t *testing.T) {
	s, api := setup(t)
	s.SetReady(nil)
	resp, err := http.Post(api+"/runs", "application/json", strings.NewReader(`{"workers":4}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 202 || resp.Header.Get("Location") == "" {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var st RunState
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st.Status != "running" || st.Workers != 4 {
		t.Fatalf("st=%+v", st)
	}
	final := waitRun(t, api, st.RunID)
	if final.Status != "ok" || final.Records != mock.ExpectedTotal() || final.Manifest == nil {
		t.Fatalf("final=%+v", final)
	}
	_, list := get(t, api+"/runs")
	if !strings.Contains(list, st.RunID) {
		t.Fatal("list missing run")
	}
	_, m := get(t, api+"/metrics")
	for _, want := range []string{`ingest_runs_total{status="ok"} 1`, `ingest_records_total{source="fathom"} 20000`, `ingest_retries_total{reason="429",source="delphine"} 1`, "ingest_run_duration_seconds_bucket"} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if code, _ := get(t, api+"/debug/pprof/goroutine?debug=1"); code != 200 {
		t.Fatal("pprof not served")
	}
	if code, body := get(t, api+"/debug/pprof/goroutineleak?debug=1"); code != 200 || !strings.Contains(body, "goroutineleak") {
		t.Fatalf("goroutineleak profile: %d %s", code, body[:min(len(body), 200)])
	}
	if code, _ := get(t, api+"/runs/nope"); code != 404 {
		t.Fatal(code)
	}
}

func TestCancelViaAPI(t *testing.T) {
	s, api := setup(t)
	s.SetReady(nil)
	resp, _ := http.Post(api+"/runs", "application/json", nil)
	var st RunState
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	req, _ := http.NewRequest(http.MethodDelete, api+"/runs/"+st.RunID, nil)
	r2, err := http.DefaultClient.Do(req)
	if err != nil || r2.StatusCode != 202 {
		t.Fatalf("cancel: %v %d", err, r2.StatusCode)
	}
	r2.Body.Close()
	final := waitRun(t, api, st.RunID)
	if final.Status != "cancelled" && final.Status != "ok" { // ok only if it finished before the cancel landed
		t.Fatalf("final=%+v", final)
	}
}
