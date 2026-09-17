package sink

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

func rec(i int) *record.RawRecord {
	r := &record.RawRecord{SchemaVersion: 1, RunID: "r1", SourceID: "s", Carrier: "S", FetchedAt: time.Unix(0, 0).UTC(),
		RowIndex: i, OriginRaw: "A", DestinationRaw: "B", ContainerTypeRaw: "20DRY", PriceRaw: "1", Currency: "USD", Unit: "per_container",
		Surcharges: []record.Surcharge{}}
	r.ComputeHash()
	return r
}

func mustWrite(t *testing.T, w Writer, recs ...*record.RawRecord) {
	t.Helper()
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
}

func manifest() *record.Manifest {
	return &record.Manifest{RunID: "r1", StartedAt: time.Unix(0, 0).UTC(), FinishedAt: time.Unix(1, 0).UTC(), Workers: 1,
		Sources: map[string]record.SourceReport{"s": {Status: "ok", Records: 3, Parts: []string{"part-000.jsonl.gz"}}},
		Totals:  record.Totals{Records: 3, SourcesOK: 1}}
}

// ---- fs ----

func TestFSWriteCloseRotatesAndPromotes(t *testing.T) {
	root := t.TempDir()
	s, err := NewFS(root)
	if err != nil {
		t.Fatal(err)
	}
	s.MaxRecordsPerPart = 2
	if err := s.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	w, _ := s.Open(context.Background(), "r1", "s")
	for i := 0; i < 5; i++ {
		if err := w.Write(rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	// before Close: only .tmp files exist
	if names := ls(t, filepath.Join(root, "runs", "run_id=r1", "source=s")); strings.Join(names, ",") != "part-000.jsonl.gz.tmp,part-001.jsonl.gz.tmp" {
		t.Fatalf("pre-close files: %v", names)
	}
	parts, err := w.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(parts, ",") != "part-000.jsonl.gz,part-001.jsonl.gz,part-002.jsonl.gz" {
		t.Fatalf("parts=%v", parts)
	}
	if names := ls(t, filepath.Join(root, "runs", "run_id=r1", "source=s")); strings.Join(names, ",") != "part-000.jsonl.gz,part-001.jsonl.gz,part-002.jsonl.gz" {
		t.Fatalf("post-close files: %v", names)
	}
	// content round-trips
	f, _ := os.Open(filepath.Join(root, "runs", "run_id=r1", "source=s", "part-002.jsonl.gz"))
	gz, _ := gzip.NewReader(f)
	b, _ := io.ReadAll(gz)
	f.Close()
	var got record.RawRecord
	if err := json.Unmarshal(b, &got); err != nil || got.RowIndex != 4 {
		t.Fatalf("round trip: %v %+v", err, got)
	}
	// manifest last
	has, _ := s.HasManifest(context.Background(), "r1")
	if has {
		t.Fatal("no manifest yet")
	}
	if err := s.WriteManifest(context.Background(), manifest()); err != nil {
		t.Fatal(err)
	}
	has, _ = s.HasManifest(context.Background(), "r1")
	if !has {
		t.Fatal("manifest expected")
	}
	if _, err := w.Close(context.Background()); err == nil {
		t.Fatal("double close must fail")
	}
}

func TestFSAbortLeavesNothingAndGC(t *testing.T) {
	root := t.TempDir()
	s, _ := NewFS(root)
	s.MaxRecordsPerPart = 1
	w, _ := s.Open(context.Background(), "r1", "s")
	mustWrite(t, w, rec(0), rec(1))
	if err := w.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if names := ls(t, filepath.Join(root, "runs", "run_id=r1", "source=s")); len(names) != 0 {
		t.Fatalf("abort left %v", names)
	}
	// GC removes stale temp files only
	w2, _ := s.Open(context.Background(), "r2", "s")
	mustWrite(t, w2, rec(0), rec(1))
	stale := filepath.Join(root, "runs", "run_id=r2", "source=s", "part-000.jsonl.gz.tmp")
	if err := os.Chtimes(stale, time.Now().Add(-48*time.Hour), time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	n, err := s.GC(context.Background(), 24*time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("gc n=%d err=%v", n, err)
	}
}

func ls(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// ---- s3 (gofakes3, in process) ----

func fakeS3(t *testing.T) (*s3.Client, func(prefix string) []string) {
	t.Helper()
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	t.Cleanup(ts.Close)
	if err := backend.CreateBucket("rates"); err != nil {
		t.Fatal(err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("KEY", "SECRET", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	list := func(prefix string) []string {
		out, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("rates"), Prefix: aws.String(prefix)})
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, o := range out.Contents {
			keys = append(keys, *o.Key)
		}
		return keys
	}
	return client, list
}

func TestS3PromoteOnClose(t *testing.T) {
	client, list := fakeS3(t)
	s := NewS3(client, "rates", "pfx")
	s.MaxRecordsPerPart = 2
	if err := s.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	w, _ := s.Open(context.Background(), "r1", "s")
	mustWrite(t, w, rec(0), rec(1), rec(2))
	if keys := list("pfx/_tmp/"); len(keys) != 1 {
		t.Fatalf("expected one temp part before close, got %v", keys)
	}
	if keys := list("pfx/runs/"); len(keys) != 0 {
		t.Fatalf("nothing promoted before close, got %v", keys)
	}
	parts, err := w.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(parts, ",") != "part-000.jsonl.gz,part-001.jsonl.gz" {
		t.Fatal(parts)
	}
	if keys := list("pfx/_tmp/"); len(keys) != 0 {
		t.Fatalf("temp must be cleaned, got %v", keys)
	}
	keys := list("pfx/runs/run_id=r1/source=s/")
	if strings.Join(keys, ",") != "pfx/runs/run_id=r1/source=s/part-000.jsonl.gz,pfx/runs/run_id=r1/source=s/part-001.jsonl.gz" {
		t.Fatalf("keys=%v", keys)
	}
	has, _ := s.HasManifest(context.Background(), "r1")
	if has {
		t.Fatal("no manifest yet")
	}
	if err := s.WriteManifest(context.Background(), manifest()); err != nil {
		t.Fatal(err)
	}
	has, err = s.HasManifest(context.Background(), "r1")
	if err != nil || !has {
		t.Fatalf("manifest: %v %v", has, err)
	}
}

func TestS3AbortAndGC(t *testing.T) {
	client, list := fakeS3(t)
	s := NewS3(client, "rates", "")
	s.MaxRecordsPerPart = 1
	w, _ := s.Open(context.Background(), "r1", "s")
	mustWrite(t, w, rec(0), rec(1))
	if err := w.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if keys := list(""); len(keys) != 0 {
		t.Fatalf("abort left %v", keys)
	}
	w2, _ := s.Open(context.Background(), "r2", "s")
	mustWrite(t, w2, rec(0), rec(1))
	n, err := s.GC(context.Background(), 0)
	if err != nil || n != 2 { // two writes at one record per part = two temp objects
		t.Fatalf("gc n=%d err=%v keys=%v", n, err, list(""))
	}
}
