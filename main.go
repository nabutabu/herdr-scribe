package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/client"
	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
	"github.com/nabutabu/herdr-scribe/internal/tracker"
)

const maxInitialAttempts = 5

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	raw, err := client.Call(ctx, "ping", map[string]any{})
	cancel()
	if err != nil {
		slog.Error("ping failed", "error", err)
		os.Exit(1)
	}
	slog.Info("ping ok", "response", string(raw))

	sub, resp, ok := subscribeWithBackoff(maxInitialAttempts)
	if !ok {
		slog.Error("initial session snapshot failed; aborting")
		os.Exit(1)
	}
	defer func() { sub.Close() }()

	tr := tracker.NewTracker()
	tr.ApplySnapshot(resp.Snapshot)

	// Drift detected by the tracker's reconciliation loop is the only path that
	// tears down and re-creates the subscription. The loop goroutine never
	// touches `sub`; it only signals here so ownership stays with main.
	resubscribe := make(chan struct{}, 1)
	onDrift := func() {
		select {
		case resubscribe <- struct{}{}:
		default:
		}
	}
	go tr.Run(context.Background(), time.Minute, onDrift)

	slog.Info("subscribed; open/close a pane or drive an agent to see events (Ctrl-C to stop)")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				slog.Info("subscription stream ended")
				return
			}

			tr.ApplyEvent(ev)
			slog.Info("Parsed Event", "event", ev)

		case err := <-sub.Err():
			if err != nil {
				slog.Error("subscription error", "error", err)
			}
			return
		case <-resubscribe:
			// Silent-stream guard (0.4): the socket is healthy but the event
			// stream drifted from reality. Re-bootstrap and resubscribe.
			reconnect(tr, sub, &sub)
		case <-sig:
			slog.Info("shutting down")
			return
		}
	}
}

// reconnect closes the current subscription and establishes a fresh one with a
// fresh session.snapshot (state may have drifted while we were disconnect),
// re-baselining the tracker so the next reconcile tick doesn't immediately
// re-report the gap.
func reconnect(tr *tracker.Tracker, old *events.Subscriber, sub **events.Subscriber) {
	old.Close()
	next, resp, ok := subscribeWithBackoff(0)
	if ok {
		*sub = next
		tr.ApplySnapshot(resp.Snapshot)
	}
}

// subscribeWithBackoff fetches a fresh session.snapshot and establishes a
// subscription scoped to the panes it finds, retrying with capped exponential
// backoff. maxAttempts == 0 means retry forever; otherwise it returns
// success=false after maxAttempts failed attempts.
func subscribeWithBackoff(maxAttempts int) (*events.Subscriber, snapshot.Response, bool) {
	for attempt := 1; maxAttempts == 0 || attempt <= maxAttempts; attempt++ {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		time.Sleep(backoff)
	}
	return nil, snapshot.Response{}, false
}