// Package app owns the process lifecycle: it pings the Herdr socket,
// bootstraps from a session.snapshot, subscribes to scoped lifecycle events,
// and keeps the subscription alive under the tracker's drift detection.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/client"
	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
	"github.com/nabutabu/herdr-scribe/internal/tracker"
)

const maxInitialAttempts = 5

// App owns the subscription lifecycle. A single Tracker is shared between the
// event loop (ApplyEvent) and the reconciliation watchdog (Run's diffing).
type App struct {
	sub *events.Subscriber
	tr  *tracker.Tracker
}

func New() *App {
	return &App{}
}

// Run drives the process until ctx is cancelled or the subscription stream
// ends. Any startup failure is returned as an error.
func (a *App) Run(ctx context.Context) error {
	if err := a.ping(ctx); err != nil {
		return fmt.Errorf("ping herdr: %w", err)
	}

	sub, resp, ok := subscribeWithBackoff(ctx, maxInitialAttempts)
	if !ok {
		return fmt.Errorf("initial session snapshot failed; aborting")
	}
	a.sub = sub
	defer func() { a.sub.Close() }() // reconnect() swaps a.sub; close the last one

	a.tr = tracker.NewTracker()
	a.tr.ApplySnapshot(resp.Snapshot)

	// Drift detected by the tracker's reconciliation loop is the only path that
	// tears down and re-creates the subscription. The loop goroutine never
	// touches `sub`; it only signals here so ownership stays with Run.
	resubscribe := make(chan struct{}, 1)
	onDrift := func() {
		select {
		case resubscribe <- struct{}{}:
		default:
		}
	}
	go a.tr.Run(ctx, time.Minute, onDrift)

	slog.Info("subscribed; open/close a pane or drive an agent to see events (Ctrl-C to stop)")

	for {
		select {
		case ev, ok := <-a.sub.Events():
			if !ok {
				slog.Info("subscription stream ended")
				return nil
			}

			a.tr.ApplyEvent(ev)
			slog.Info("Parsed Event", "event", ev)

		case err := <-a.sub.Err():
			if err != nil {
				slog.Error("subscription error", "error", err)
			}
			a.reconnect(ctx)

		case <-resubscribe:
			// Silent-stream guard (0.4): the socket is healthy but the event
			// stream drifted from reality. Re-bootstrap and resubscribe.
			a.reconnect(ctx)

		case <-ctx.Done():
			slog.Info("shutting down")
			return nil
		}
	}
}

func (a *App) ping(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	raw, err := client.Call(cctx, "ping", map[string]any{})
	cancel()
	if err != nil {
		return err
	}
	slog.Info("ping ok", "response", string(raw))
	return nil
}

// reconnect closes the current subscription and establishes a fresh one with a
// fresh session.snapshot (state may have drifted while we were disconnect),
// re-baselining the tracker so the next reconcile tick doesn't immediately
// re-report the gap.
func (a *App) reconnect(ctx context.Context) {
	a.sub.Close()
	next, resp, ok := subscribeWithBackoff(ctx, 0)
	if ok {
		a.sub = next
		a.tr.ApplySnapshot(resp.Snapshot)
	}
}

// subscribeWithBackoff fetches a fresh session.snapshot and establishes a
// subscription scoped to the panes it finds, retrying with capped exponential
// backoff. maxAttempts == 0 means retry forever; otherwise it returns
// success=false after maxAttempts failed attempts.
func subscribeWithBackoff(ctx context.Context, maxAttempts int) (*events.Subscriber, snapshot.Response, bool) {
	for attempt := 1; maxAttempts == 0 || attempt <= maxAttempts; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s, resp, ok := events.SubscribeFromSnapshot(cctx)
		cancel()
		if ok {
			return s, resp, true
		}
		backoff := time.Duration(1<<(attempt-1)) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		slog.Info("snapshot subscribe failed; retrying", "attempt", attempt, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, snapshot.Response{}, false
		}
	}
	return nil, snapshot.Response{}, false
}
