// Package metrics holds the Prometheus collectors (docs/PLAN.md §8.6).
package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// M groups every collector so tests can build an isolated registry.
type M struct {
	RunsTotal       *prometheus.CounterVec
	RecordsTotal    *prometheus.CounterVec
	SourceDuration  *prometheus.HistogramVec
	HTTPRequests    *prometheus.CounterVec
	RetriesTotal    *prometheus.CounterVec
	InflightSources prometheus.Gauge
	RunDuration     *prometheus.HistogramVec
}

// New registers the collectors on reg (nil = default registry).
func New(reg prometheus.Registerer) *M {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &M{
		RunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_runs_total", Help: "Runs by final status."}, []string{"status"}),
		RecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_records_total", Help: "Records landed per source."}, []string{"source"}),
		SourceDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_source_duration_seconds", Help: "Per-source wall time.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12)}, []string{"source", "status"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_http_requests_total", Help: "Upstream requests by status code."}, []string{"source", "code"}),
		RetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_retries_total", Help: "Retries by reason."}, []string{"source", "reason"}),
		InflightSources: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ingest_inflight_sources", Help: "Sources currently being fetched."}),
		RunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_run_duration_seconds", Help: "Run wall time by worker count.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 10)}, []string{"workers"}),
	}
	reg.MustRegister(m.RunsTotal, m.RecordsTotal, m.SourceDuration, m.HTTPRequests, m.RetriesTotal, m.InflightSources, m.RunDuration)
	return m
}

// Nop returns collectors registered on a throwaway registry (for the CLI
// one-shot modes and tests that do not scrape).
func Nop() *M { return New(prometheus.NewRegistry()) }

// Workers formats the label value.
func Workers(n int) string { return strconv.Itoa(n) }
