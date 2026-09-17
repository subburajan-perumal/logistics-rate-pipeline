// Package shutdown turns SIGTERM/SIGINT into an orderly stop: stop
// accepting, cancel work, wait up to a grace period, exit 0 (D-16).
package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Context returns a context cancelled on SIGTERM/SIGINT plus the signal
// that caused it (via the returned channel, for logging).
func Context(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
}

// Wait blocks until ctx is done, then runs drain with the grace period and
// returns whether it completed in time. A second signal exits immediately.
func Wait(ctx context.Context, log *slog.Logger, grace time.Duration, drain func(time.Duration) bool) bool {
	<-ctx.Done()
	log.Info("shutdown signal received", "grace", grace)
	second := make(chan os.Signal, 1)
	signal.Notify(second, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(second)
	done := make(chan bool, 1)
	go func() { done <- drain(grace) }()
	select {
	case ok := <-done:
		return ok
	case <-second:
		log.Warn("second signal; exiting immediately")
		return false
	}
}
