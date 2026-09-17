// Package source performs the HTTP side of ingestion: auth, pagination
// URLs, rate limiting and retry with backoff (docs/PLAN.md §8.3, D-13).
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
)

// Clock abstracts time so backoff and rate limiting are testable with
// testing/synctest and a deterministic jitter source.
type Clock interface {
	Now() time.Time
	// Sleep returns early with ctx.Err() when the context is cancelled.
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the production clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Fetcher issues requests for one source. Create one per source per run.
type Fetcher struct {
	Client *http.Client
	Clock  Clock
	// Jitter returns a value in [0,1). Defaults to math/rand/v2; tests inject
	// a seeded generator.
	Jitter func() float64
	Recipe recipe.Recipe
	// Optional observers for metrics.
	OnResponse func(status int)
	OnRetry    func(reason string)

	limiter *limiter
}

// Stats accumulates attempt counts across the source's pages.
type Stats struct {
	Attempts int
	Retries  int
}

// New builds a fetcher for a recipe.
func New(client *http.Client, clock Clock, rc recipe.Recipe) *Fetcher {
	f := &Fetcher{Client: client, Clock: clock, Jitter: rand.Float64, Recipe: rc}
	if rc.RateLimitRPS > 0 {
		f.limiter = newLimiter(rc.RateLimitRPS, clock)
	}
	return f
}

// NonRetryable marks a terminal HTTP failure (4xx other than 429).
type NonRetryable struct {
	Status int
	URL    string
}

func (e *NonRetryable) Error() string {
	return fmt.Sprintf("GET %s: HTTP %d (not retried)", e.URL, e.Status)
}

// PageURL builds the URL for a page given the previous page's "next" value.
// For page-type pagination next is the page number; for cursor it is opaque.
func (f *Fetcher) PageURL(next string) (string, error) {
	rc := f.Recipe
	if rc.Pagination.Type == "none" || next == "" {
		return rc.URL, nil
	}
	u, err := url.Parse(rc.URL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set(rc.Pagination.Param, next)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// FirstPage returns the "next" value that fetches the first page.
func (f *Fetcher) FirstPage() string {
	if f.Recipe.Pagination.Type == "page" {
		return strconv.Itoa(f.Recipe.Pagination.Start)
	}
	return ""
}

// Get performs a GET with the recipe's auth, rate limit and retry policy.
// The caller must close the returned body. Failures are returned after the
// last attempt; stats are updated in place.
func (f *Fetcher) Get(ctx context.Context, rawURL string, st *Stats) (*http.Response, error) {
	rc := f.Recipe
	var lastErr error
	for attempt := 1; attempt <= rc.Retry.MaxAttempts; attempt++ {
		if f.limiter != nil {
			if err := f.limiter.Wait(ctx); err != nil {
				return nil, err
			}
		}
		st.Attempts++
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "ingestd/1")
		req.Header.Set("Accept-Encoding", "identity") // we handle gzip ourselves when the recipe says so
		if rc.Auth.Type == "api_key" {
			v := os.Getenv(rc.Auth.SecretEnv)
			if v == "" {
				return nil, fmt.Errorf("auth: env %s is empty", rc.Auth.SecretEnv)
			}
			req.Header.Set(rc.Auth.Header, v)
		}

		resp, err := f.Client.Do(req)
		var wait time.Duration
		reason := ""
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			f.observe(0)
			reason = "network"
			lastErr = fmt.Errorf("GET %s: %w", rawURL, err)
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			f.observe(resp.StatusCode)
			return resp, nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			f.observe(resp.StatusCode)
			reason = "5xx"
			if resp.StatusCode == http.StatusTooManyRequests {
				reason = "429"
			}
			wait = retryAfter(resp.Header.Get("Retry-After"), f.Clock.Now())
			drain(resp)
			lastErr = fmt.Errorf("GET %s: HTTP %d", rawURL, resp.StatusCode)
		default:
			f.observe(resp.StatusCode)
			drain(resp)
			return nil, &NonRetryable{Status: resp.StatusCode, URL: rawURL}
		}

		if attempt == rc.Retry.MaxAttempts {
			break
		}
		st.Retries++
		if f.OnRetry != nil {
			f.OnRetry(reason)
		}
		if wait == 0 {
			wait = f.backoff(attempt)
		}
		if err := f.Clock.Sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("after %d attempts: %w", rc.Retry.MaxAttempts, lastErr)
}

func (f *Fetcher) observe(status int) {
	if f.OnResponse != nil {
		f.OnResponse(status)
	}
}

// backoff is exponential with full jitter, capped (D-13).
func (f *Fetcher) backoff(attempt int) time.Duration {
	base := float64(f.Recipe.Retry.BaseBackoff.Duration) * math.Pow(2, float64(attempt-1))
	if max := float64(f.Recipe.Retry.MaxBackoff.Duration); base > max {
		base = max
	}
	return time.Duration(base * f.Jitter())
}

// retryAfter parses a Retry-After header as seconds or an HTTP date.
func retryAfter(h string, now time.Time) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if s, err := strconv.Atoi(h); err == nil && s >= 0 {
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}

// IsNonRetryable reports whether err is a terminal HTTP failure.
func IsNonRetryable(err error) bool {
	var nr *NonRetryable
	return errors.As(err, &nr)
}

// limiter enforces a minimum interval between request starts.
type limiter struct {
	interval time.Duration
	clock    Clock
	next     time.Time
}

func newLimiter(rps float64, clock Clock) *limiter {
	return &limiter{interval: time.Duration(float64(time.Second) / rps), clock: clock}
}

func (l *limiter) Wait(ctx context.Context) error {
	now := l.clock.Now()
	if now.Before(l.next) {
		if err := l.clock.Sleep(ctx, l.next.Sub(now)); err != nil {
			return err
		}
		now = l.next
	}
	l.next = now.Add(l.interval)
	return nil
}
