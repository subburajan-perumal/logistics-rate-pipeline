// Package sink lands raw records atomically: parts are written under a
// temporary name/prefix and promoted only on Close; the manifest is written
// last (docs/PLAN.md §8.5, D-14).
package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

// DefaultMaxRecordsPerPart rotates parts so a large source lands as several
// files (the fathom feed yields two).
const DefaultMaxRecordsPerPart = 10000

// Sink is a destination for runs.
type Sink interface {
	// Name is "fs" or "s3".
	Name() string
	// Ready reports whether the destination is reachable/writable.
	Ready(ctx context.Context) error
	// Open starts writing one source of one run.
	Open(ctx context.Context, runID, sourceID string) (Writer, error)
	// WriteManifest finalises a run. Must be called last.
	WriteManifest(ctx context.Context, m *record.Manifest) error
	// HasManifest reports whether a run is complete.
	HasManifest(ctx context.Context, runID string) (bool, error)
	// GC removes temporary objects older than the given age.
	GC(ctx context.Context, olderThan time.Duration) (int, error)
}

// Writer receives the records of one source.
type Writer interface {
	Write(r *record.RawRecord) error
	// Close promotes every part and returns their names.
	Close(ctx context.Context) ([]string, error)
	// Abort discards everything written so far.
	Abort(ctx context.Context) error
}

// Keys shared by implementations.
func runPrefix(runID string) string         { return path.Join("runs", "run_id="+runID) }
func sourcePrefix(runID, sid string) string { return path.Join(runPrefix(runID), "source="+sid) }
func partName(n int) string                 { return fmt.Sprintf("part-%03d.jsonl.gz", n) }
func manifestKey(runID string) string       { return path.Join(runPrefix(runID), "_MANIFEST.json") }
func partKey(runID, sid string, n int) string {
	return path.Join(sourcePrefix(runID, sid), partName(n))
}

// partBuffer encodes records as gzipped JSONL in memory until rotation.
// Parts are ≤ 10k records (~1 MB gz) so buffering is bounded by design.
type partBuffer struct {
	buf  bytes.Buffer
	gz   *gzip.Writer
	enc  *json.Encoder
	n    int
	part int
}

func newPartBuffer(part int) *partBuffer {
	p := &partBuffer{part: part}
	p.gz = gzip.NewWriter(&p.buf)
	p.enc = json.NewEncoder(p.gz)
	return p
}

func (p *partBuffer) write(r *record.RawRecord) error {
	if err := p.enc.Encode(r); err != nil {
		return err
	}
	p.n++
	return nil
}

func (p *partBuffer) bytes() ([]byte, error) {
	if err := p.gz.Close(); err != nil {
		return nil, err
	}
	return p.buf.Bytes(), nil
}

func marshalManifest(m *record.Manifest) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}
