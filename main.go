package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/client"
	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	raw, err := client.Call(ctx, "ping", map[string]any{})
	cancel()
	if err != nil {
		slog.Error("ping failed", "error", err)
		os.Exit(1)
	}
	slog.Info("ping ok", "response", string(raw))

	sub, err := events.NewSubscriber(events.BuildParams(nil))
	if err != nil {
		slog.Error("subscribe failed", "error", err)
		os.Exit(1)
	}
	defer func() { sub.Close() }()

	slog.Info("subscribed; open/close a pane or drive an agent to see events (Ctrl-C to stop)")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	sigError := make(chan os.Signal, 1)
	signal.Notify(sigError, syscall.SIGABRT, syscall.SIGPIPE)

	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				slog.Info("subscription stream ended")
				return
			}

			slog.Info("Parsed Event", "event", ev)

		case err := <-sub.Err():
			if err != nil {
				slog.Error("subscription error", "error", err)
			}
			return
		case <-sig:
			slog.Info("shutting down")
			return
		case <-sigError:
			// try reconnect (with session.snapshot) with exp backoff using sleep
			slog.Debug("Error signal. Trying reconnect...")
			sub.Close()
			for attempt := 1; ; attempt++ {
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				s, ok := TryConnectWithSessionSnapshot(cctx)
				cancel()
				if ok {
					sub = s
					break
				}
				backoff := time.Duration(1<<(attempt-1)) * time.Second
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				slog.Info("reconnect failed; retrying", "attempt", attempt, "backoff", backoff)
				time.Sleep(backoff)
			}
		}
	}
}

func TryConnectWithSessionSnapshot(ctx context.Context) (*events.Subscriber, bool) {
	raw, err := client.Call(ctx, "session.snapshot", map[string]any{})
	if err != nil {
		slog.Error("session reconnect failed", "error", err)
		return nil, false
	}

	var resp snapshot.Response
	if err := json.Unmarshal(raw, &resp); err != nil {
		slog.Error("parsing session snapshot", "error", err, "raw", string(raw))
		return nil, false
	}

	paneIDs := make([]string, 0, len(resp.Snapshot.Panes))
	for _, pane := range resp.Snapshot.Panes {
		paneIDs = append(paneIDs, pane.PaneID)
	}
	slog.Info("resubscribing from snapshot", "pane_count", len(paneIDs), "pane_ids", paneIDs)

	sub, err := events.NewSubscriber(events.BuildParams(paneIDs))
	if err != nil {
		slog.Error("resubscribe failed", "error", err)
		return nil, false
	}
	return sub, true
}
