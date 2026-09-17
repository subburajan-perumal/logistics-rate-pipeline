package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
)

// fakeClock records sleeps instead of sleeping.
type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return nil
}

// script is a RoundTripper replaying canned responses in order.
type script struct {
	responses []*http.Response
	errs      []error
	seen      []*http.Request
}

func (s *script) RoundTrip(r *http.Request) (*http.Response, error) {
	i := len(s.seen)
	s.seen = append(s.seen, r)
	if i < len(s.errs) && s.errs[i] != nil {
		return nil, s.errs[i]
	}
	if i >= len(s.responses) {
		return nil, errors.New("script exhausted")
	}
	return s.responses[i], nil
}

func resp(code int, hdr map[string]string) *http.Response {
	r := &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("x"))}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	return r
}

func rc(t *testing.T) recipe.Recipe {
	t.Helper()
	r := recipe.Recipe{SourceID: "s", Carrier: "S", URL: "http://h/feed", Format: "json", RecordsPath: "items",
		Columns: recipe.Columns{Origin: "o", Destination: "d", ContainerType: "t", Price: "p", Currency: "c", Unit: "per_container"},
		Retry:   recipe.Retry{MaxAttempts: 4, BaseBackoff: recipe.Duration{Duration: 100 * time.Millisecond}, MaxBackoff: recipe.Duration{Duration: 250 * time.Millisecond}}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func newFetcher(t *testing.T, sc *script, r recipe.Recipe) (*Fetcher, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	f := New(&http.Client{Transport: sc}, clock, r)
	f.Jitter = func() float64 { return 1 } // deterministic full backoff
	return f, clock
}

func TestRetrySchedule5xxThenSuccess(t *testing.T) {
	sc := &script{responses: []*http.Response{resp(500, nil), resp(503, nil), resp(502, nil), resp(200, nil)}}
	f, clock := newFetcher(t, sc, rc(t))
	var st Stats
	r, err := f.Get(context.Background(), "http://h/feed", &st)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if st.Attempts != 4 || st.Retries != 3 {
		t.Fatalf("stats=%+v", st)
	}
	// 100ms, 200ms, capped 250ms
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond}
	if len(clock.sleeps) != 3 {
		t.Fatalf("sleeps=%v", clock.sleeps)
	}
	for i := range want {
		if clock.sleeps[i] != want[i] {
			t.Fatalf("sleeps=%v want %v", clock.sleeps, want)
		}
	}
}

func TestRetryExhausted(t *testing.T) {
	sc := &script{responses: []*http.Response{resp(500, nil), resp(500, nil), resp(500, nil), resp(500, nil)}}
	f, _ := newFetcher(t, sc, rc(t))
	var st Stats
	r, err := f.Get(context.Background(), "http://h/feed", &st)
	if r != nil {
		defer r.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "after 4 attempts") {
		t.Fatalf("err=%v", err)
	}
	if st.Attempts != 4 || st.Retries != 3 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestRetryAfterSecondsAndDate(t *testing.T) {
	retryAfter := func(v string) map[string]string { return map[string]string{"Retry-After": v} }
	sc := &script{responses: []*http.Response{resp(429, retryAfter("2")), resp(429, retryAfter("Tue, 01 Sep 2026 00:00:07 GMT")), resp(200, nil)}}
	f, clock := newFetcher(t, sc, rc(t))
	var st Stats
	r, err := f.Get(context.Background(), "http://h/feed", &st)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	// first sleep 2s (header), second: date is 7s after start, clock is now +2s → 5s
	if len(clock.sleeps) != 2 || clock.sleeps[0] != 2*time.Second || clock.sleeps[1] != 5*time.Second {
		t.Fatalf("sleeps=%v", clock.sleeps)
	}
}

func TestNoRetryOn4xx(t *testing.T) {
	sc := &script{responses: []*http.Response{resp(404, nil)}}
	f, clock := newFetcher(t, sc, rc(t))
	var st Stats
	_, err := f.Get(context.Background(), "http://h/feed", &st)
	if !IsNonRetryable(err) || st.Attempts != 1 || len(clock.sleeps) != 0 {
		t.Fatalf("err=%v stats=%+v sleeps=%v", err, st, clock.sleeps)
	}
}

func TestNetworkErrorRetried(t *testing.T) {
	sc := &script{errs: []error{errors.New("conn reset"), nil}, responses: []*http.Response{nil, resp(200, nil)}}
	f, _ := newFetcher(t, sc, rc(t))
	var st Stats
	r, err := f.Get(context.Background(), "http://h/feed", &st)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if st.Retries != 1 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestCancelDuringBackoffReturnsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sc := &script{responses: []*http.Response{resp(500, nil), resp(200, nil)}}
	f, _ := newFetcher(t, sc, rc(t))
	blocking := &blockingClock{fakeClock{now: time.Now()}, cancel}
	f.Clock = blocking
	var st Stats
	_, err := f.Get(ctx, "http://h/feed", &st)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if len(sc.seen) != 1 {
		t.Fatalf("must not retry after cancel; requests=%d", len(sc.seen))
	}
}

// blockingClock cancels the context when asked to sleep, simulating a
// SIGTERM arriving mid-backoff.
type blockingClock struct {
	fakeClock
	cancel context.CancelFunc
}

func (c *blockingClock) Sleep(ctx context.Context, d time.Duration) error {
	c.cancel()
	return ctx.Err()
}

func TestAuthHeaderOnlyWhenConfigured(t *testing.T) {
	r := rc(t)
	sc := &script{responses: []*http.Response{resp(200, nil), resp(200, nil)}}
	f, _ := newFetcher(t, sc, r)
	var st Stats
	x, _ := f.Get(context.Background(), r.URL, &st)
	x.Body.Close()
	if sc.seen[0].Header.Get("X-Api-Key") != "" {
		t.Fatal("no auth expected")
	}
	r.Auth = recipe.Auth{Type: "api_key", Header: "X-Api-Key", SecretEnv: "TEST_KEY"}
	t.Setenv("TEST_KEY", "secret")
	f2, _ := newFetcher(t, sc, r)
	y, err := f2.Get(context.Background(), r.URL, &st)
	if err != nil {
		t.Fatal(err)
	}
	y.Body.Close()
	if sc.seen[1].Header.Get("X-Api-Key") != "secret" {
		t.Fatal("auth header missing")
	}
	t.Setenv("TEST_KEY", "")
	if _, err := f2.Get(context.Background(), r.URL, &st); err == nil {
		t.Fatal("empty secret must fail loudly")
	}
}

func TestRateLimitSpacing(t *testing.T) {
	r := rc(t)
	r.RateLimitRPS = 4 // 250ms apart
	sc := &script{responses: []*http.Response{resp(200, nil), resp(200, nil), resp(200, nil)}}
	f, clock := newFetcher(t, sc, r)
	var st Stats
	for i := 0; i < 3; i++ {
		x, err := f.Get(context.Background(), r.URL, &st)
		if err != nil {
			t.Fatal(err)
		}
		x.Body.Close()
	}
	if len(clock.sleeps) != 2 || clock.sleeps[0] != 250*time.Millisecond || clock.sleeps[1] != 250*time.Millisecond {
		t.Fatalf("sleeps=%v", clock.sleeps)
	}
}

func TestPageURL(t *testing.T) {
	r := rc(t)
	r.Pagination = recipe.Pagination{Type: "page", Param: "page", Start: 3, Path: "next"}
	f, _ := newFetcher(t, &script{}, r)
	if f.FirstPage() != "3" {
		t.Fatal(f.FirstPage())
	}
	u, _ := f.PageURL("4")
	if u != "http://h/feed?page=4" {
		t.Fatal(u)
	}
	r.Pagination = recipe.Pagination{Type: "cursor", Param: "cursor", Path: "next"}
	f, _ = newFetcher(t, &script{}, r)
	if f.FirstPage() != "" {
		t.Fatal("cursor feeds start without a param")
	}
	u, _ = f.PageURL("")
	if u != "http://h/feed" {
		t.Fatal(u)
	}
	u, _ = f.PageURL("a b")
	if u != "http://h/feed?cursor=a+b" {
		t.Fatal(u)
	}
}
