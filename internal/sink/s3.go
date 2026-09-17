package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

// tmpPrefix holds unpromoted parts; a lifecycle rule expires it after a day.
const tmpPrefix = "_tmp/"

// S3API is the subset of the S3 client used, so tests can use gofakes3 and
// production the real client.
type S3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3 lands runs in a bucket under an optional key prefix.
type S3 struct {
	Client            S3API
	Bucket            string
	Prefix            string
	MaxRecordsPerPart int
}

// NewS3 wires a sink; the client's credential chain decides auth (env on
// kind, Pod Identity on EKS — no keys in the chart).
func NewS3(client S3API, bucket, prefix string) *S3 {
	return &S3{Client: client, Bucket: bucket, Prefix: strings.Trim(prefix, "/"), MaxRecordsPerPart: DefaultMaxRecordsPerPart}
}

func (s *S3) Name() string { return "s3" }

func (s *S3) key(k string) string {
	if s.Prefix == "" {
		return k
	}
	return path.Join(s.Prefix, k)
}

func (s *S3) tmpKey(k string) string { return s.key(tmpPrefix + k) }

func (s *S3) Ready(ctx context.Context) error {
	_, err := s.Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.Bucket)})
	if err != nil {
		return fmt.Errorf("s3 bucket %s not reachable: %w", s.Bucket, err)
	}
	return nil
}

type s3Writer struct {
	s      *S3
	ctx    context.Context // from Open; cancellation aborts uploads
	runID  string
	sid    string
	cur    *partBuffer
	tmps   []string // temp keys uploaded so far, in part order
	closed bool
}

func (s *S3) Open(ctx context.Context, runID, sourceID string) (Writer, error) {
	return &s3Writer{s: s, ctx: ctx, runID: runID, sid: sourceID, cur: newPartBuffer(0)}, nil
}

func (w *s3Writer) Write(r *record.RawRecord) error {
	if w.closed {
		return errors.New("write after close")
	}
	if err := w.cur.write(r); err != nil {
		return err
	}
	if w.cur.n >= w.s.MaxRecordsPerPart {
		return w.flush(w.ctx)
	}
	return nil
}

func (w *s3Writer) flush(ctx context.Context) error {
	if w.cur.n == 0 {
		return nil
	}
	b, err := w.cur.bytes()
	if err != nil {
		return err
	}
	key := w.s.tmpKey(partKey(w.runID, w.sid, w.cur.part))
	_, err = w.s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(w.s.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(b),
		ContentType: aws.String("application/gzip"),
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	w.tmps = append(w.tmps, key)
	w.cur = newPartBuffer(w.cur.part + 1)
	return nil
}

// Close promotes each temp object with CopyObject + DeleteObject (S3 has no
// rename); a failure mid-way leaves already-promoted parts, which the
// missing manifest makes harmless.
func (w *s3Writer) Close(ctx context.Context) ([]string, error) {
	if w.closed {
		return nil, errors.New("double close")
	}
	w.closed = true
	if err := w.flush(ctx); err != nil {
		_ = w.Abort(ctx)
		return nil, err
	}
	var parts []string
	for _, tmp := range w.tmps {
		final := strings.Replace(tmp, tmpPrefix, "", 1)
		_, err := w.s.Client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(w.s.Bucket),
			Key:        aws.String(final),
			CopySource: aws.String(path.Join(w.s.Bucket, tmp)),
		})
		if err != nil {
			return nil, fmt.Errorf("promote %s: %w", tmp, err)
		}
		_, _ = w.s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(w.s.Bucket), Key: aws.String(tmp)})
		parts = append(parts, path.Base(final))
	}
	return parts, nil
}

func (w *s3Writer) Abort(ctx context.Context) error {
	w.closed = true
	var errs []error
	for _, tmp := range w.tmps {
		if _, err := w.s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(w.s.Bucket), Key: aws.String(tmp)}); err != nil {
			errs = append(errs, err)
		}
	}
	w.tmps = nil
	return errors.Join(errs...)
}

func (s *S3) WriteManifest(ctx context.Context, m *record.Manifest) error {
	b, err := marshalManifest(m)
	if err != nil {
		return err
	}
	_, err = s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(s.key(manifestKey(m.RunID))),
		Body:        bytes.NewReader(b),
		ContentType: aws.String("application/json"),
	})
	return err
}

func (s *S3) HasManifest(ctx context.Context, runID string) (bool, error) {
	_, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.key(manifestKey(runID)))})
	if err == nil {
		return true, nil
	}
	var nf *types.NotFound
	if errors.As(err, &nf) || strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
		return false, nil
	}
	return false, err
}

// GC deletes objects under _tmp/ older than the given age.
func (s *S3) GC(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	n := 0
	var token *string
	for {
		out, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket), Prefix: aws.String(s.tmpKey("")), ContinuationToken: token,
		})
		if err != nil {
			return n, err
		}
		for _, o := range out.Contents {
			if o.LastModified != nil && o.LastModified.Before(cutoff) {
				if _, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket), Key: o.Key}); err != nil {
					return n, err
				}
				n++
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return n, nil
		}
		token = out.NextContinuationToken
	}
}
