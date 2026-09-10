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
	defer sub.Close()

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

			slog.Info("Parsed Event", "event", ev)

		case err := <-sub.Err():
			if err != nil {
				slog.Error("subscription error", "error", err)
			}
			return
		case <-sig:
			slog.Info("shutting down")
			return
		}
	}
}
