// Package record defines the raw record and manifest contract that the Go
// ingestion service lands and the PySpark job reads (docs/PLAN.md §7).
package record

import (
	"crypto/sha256"
	"strings"
	"sync"
	"time"
)

// SchemaVersion is bumped only with a Decision Log row.
const SchemaVersion = 1

// Surcharge is an additional charge attached to a rate row, as received.
type Surcharge struct {
	Name      string   `json:"name"`
	AmountRaw string   `json:"amount_raw"`
	Amount    *float64 `json:"amount"`
}

// RawRecord is one lane × container-type row exactly as a source served it,
// plus light typing. Semantic normalization is Spark's job (D-11).
type RawRecord struct {
	SchemaVersion    int         `json:"schema_version"`
	RunID            string      `json:"run_id"`
	SourceID         string      `json:"source_id"`
	Carrier          string      `json:"carrier"`
	FetchedAt        time.Time   `json:"fetched_at"`
	Page             int         `json:"page"`
	RowIndex         int         `json:"row_index"`
	OriginRaw        string      `json:"origin_raw"`
	DestinationRaw   string      `json:"destination_raw"`
	ContainerTypeRaw string      `json:"container_type_raw"`
	PriceRaw         string      `json:"price_raw"`
	Price            *float64    `json:"price"`
	Currency         string      `json:"currency"`
	Unit             string      `json:"unit"`
	ValidFromRaw     string      `json:"valid_from_raw"`
	ValidToRaw       string      `json:"valid_to_raw"`
	Surcharges       []Surcharge `json:"surcharges"`
	SourceRef        string      `json:"source_ref"`
	RecordHash       string      `json:"record_hash"`
}

// hashScratch pools the byte buffer used to assemble the canonical form, so
// hashing a row costs one allocation (the hex string) instead of several
// (found with pprof on BenchmarkParseCSV, see bench/results/*-parse.md).
var hashScratch = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}

const hexDigits = "0123456789abcdef"

// ComputeHash sets and returns RecordHash: sha256 over the canonical raw
// fields, excluding run/fetch metadata so the same row hashes the same in
// every run (the dedup tie-break in Spark, D-30).
func (r *RawRecord) ComputeHash() string {
	bp := hashScratch.Get().(*[]byte)
	b := (*bp)[:0]
	for _, f := range [...]string{
		r.SourceID, r.Carrier, r.OriginRaw, r.DestinationRaw, r.ContainerTypeRaw,
		r.PriceRaw, r.Currency, r.Unit, r.ValidFromRaw, r.ValidToRaw,
	} {
		b = append(b, strings.TrimSpace(f)...)
		b = append(b, 0x1f)
	}
	for _, s := range r.Surcharges {
		b = append(b, s.Name...)
		b = append(b, '=')
		b = append(b, strings.TrimSpace(s.AmountRaw)...)
		b = append(b, 0x1e)
	}
	sum := sha256.Sum256(b)
	*bp = b
	hashScratch.Put(bp)
	var hexBuf [64]byte
	for i, v := range sum {
		hexBuf[i*2] = hexDigits[v>>4]
		hexBuf[i*2+1] = hexDigits[v&0x0f]
	}
	r.RecordHash = string(hexBuf[:])
	return r.RecordHash
}

// SourceStatus values used in the manifest.
const (
	StatusOK     = "ok"
	StatusFailed = "failed"
)

// SourceReport is the fan-in unit: one per source per run.
type SourceReport struct {
	SourceID   string   `json:"-"`
	Status     string   `json:"status"`
	Records    int      `json:"records"`
	Pages      int      `json:"pages"`
	Attempts   int      `json:"attempts"`
	Retries    int      `json:"retries"`
	DurationMs int64    `json:"duration_ms"`
	Parts      []string `json:"parts"`
	Error      string   `json:"error,omitempty"`
}

// Totals summarises a run.
type Totals struct {
	Records       int `json:"records"`
	SourcesOK     int `json:"sources_ok"`
	SourcesFailed int `json:"sources_failed"`
}

// Manifest is written last; a run without one is incomplete (D-14).
type Manifest struct {
	RunID          string                  `json:"run_id"`
	StartedAt      time.Time               `json:"started_at"`
	FinishedAt     time.Time               `json:"finished_at"`
	Workers        int                     `json:"workers"`
	AsOf           string                  `json:"as_of"`
	Sources        map[string]SourceReport `json:"sources"`
	Totals         Totals                  `json:"totals"`
	IngestdVersion string                  `json:"ingestd_version"`
	RecipesSHA256  string                  `json:"recipes_sha256"`
}

// Status of the whole run as reported by the run metric.
func (m *Manifest) Status() string {
	if m.Totals.SourcesFailed == 0 {
		return "ok"
	}
	return "partial"
}
