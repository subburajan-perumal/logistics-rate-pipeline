package sink

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

// FS lands runs in a local directory (a PVC on kind).
type FS struct {
	Root              string
	MaxRecordsPerPart int
}

// NewFS creates the root if needed.
func NewFS(root string) (*FS, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &FS{Root: root, MaxRecordsPerPart: DefaultMaxRecordsPerPart}, nil
}

func (s *FS) Name() string { return "fs" }

func (s *FS) Ready(ctx context.Context) error {
	f, err := os.CreateTemp(s.Root, ".ready-*")
	if err != nil {
		return fmt.Errorf("fs sink not writable: %w", err)
	}
	f.Close()
	return os.Remove(f.Name())
}

func (s *FS) abs(key string) string { return filepath.Join(s.Root, filepath.FromSlash(key)) }

type fsWriter struct {
	s      *FS
	runID  string
	sid    string
	cur    *partBuffer
	tmps   []string // temp paths written so far, in part order
	closed bool
}

func (s *FS) Open(ctx context.Context, runID, sourceID string) (Writer, error) {
	if err := os.MkdirAll(s.abs(sourcePrefix(runID, sourceID)), 0o755); err != nil {
		return nil, err
	}
	return &fsWriter{s: s, runID: runID, sid: sourceID, cur: newPartBuffer(0)}, nil
}

func (w *fsWriter) Write(r *record.RawRecord) error {
	if w.closed {
		return errors.New("write after close")
	}
	if err := w.cur.write(r); err != nil {
		return err
	}
	if w.cur.n >= w.s.MaxRecordsPerPart {
		return w.flush()
	}
	return nil
}

// flush writes the current part to its temp file and starts the next.
func (w *fsWriter) flush() error {
	if w.cur.n == 0 {
		return nil
	}
	b, err := w.cur.bytes()
	if err != nil {
		return err
	}
	tmp := w.s.abs(partKey(w.runID, w.sid, w.cur.part)) + ".tmp"
	if err := writeFileSync(tmp, b); err != nil {
		return err
	}
	w.tmps = append(w.tmps, tmp)
	w.cur = newPartBuffer(w.cur.part + 1)
	return nil
}

func (w *fsWriter) Close(ctx context.Context) ([]string, error) {
	if w.closed {
		return nil, errors.New("double close")
	}
	w.closed = true
	if err := w.flush(); err != nil {
		_ = w.Abort(ctx)
		return nil, err
	}
	var parts []string
	for _, tmp := range w.tmps {
		final := strings.TrimSuffix(tmp, ".tmp")
		if err := os.Rename(tmp, final); err != nil {
			return nil, fmt.Errorf("promote %s: %w", tmp, err)
		}
		parts = append(parts, filepath.Base(final))
	}
	return parts, nil
}

func (w *fsWriter) Abort(ctx context.Context) error {
	w.closed = true
	var errs []error
	for _, tmp := range w.tmps {
		if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	w.tmps = nil
	return errors.Join(errs...)
}

func (s *FS) WriteManifest(ctx context.Context, m *record.Manifest) error {
	b, err := marshalManifest(m)
	if err != nil {
		return err
	}
	final := s.abs(manifestKey(m.RunID))
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	if err := writeFileSync(final+".tmp", b); err != nil {
		return err
	}
	return os.Rename(final+".tmp", final)
}

func (s *FS) HasManifest(ctx context.Context, runID string) (bool, error) {
	_, err := os.Stat(s.abs(manifestKey(runID)))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// GC deletes *.tmp files older than the given age under runs/.
func (s *FS) GC(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	n := 0
	root := filepath.Join(s.Root, "runs")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".tmp") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(p); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

func writeFileSync(p string, b []byte) error {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
