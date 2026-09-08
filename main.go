package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := client.Call(ctx, "ping", map[string]any{})
	if err != nil {
		slog.Error("client call failed", "error", err)
		os.Exit(1)
	}

	slog.Info("client call succeeded", "response", string(raw))
}
