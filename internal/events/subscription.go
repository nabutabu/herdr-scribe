package events

import (
	"context"
	"log/slog"

	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

// SubscriptionType is the event name Herdr accepts in an events.subscribe
// request (dotted form, e.g. "pane.created"), distinct from the underscore
// wire name pushed back on the event stream.
type SubscriptionType string

const (
	SubscribeWorkspaceCreated       SubscriptionType = "workspace.created"
	SubscribeWorkspaceClosed        SubscriptionType = "workspace.closed"
	SubscribePaneCreated            SubscriptionType = "pane.created"
	SubscribePaneClosed             SubscriptionType = "pane.closed"
	SubscribePaneAgentDetected      SubscriptionType = "pane.agent_detected"
	SubscribePaneAgentStatusChanged SubscriptionType = "pane.agent_status_changed"
)

type Subscription struct {
	Type   SubscriptionType `json:"type"`
	PaneID string           `json:"pane_id,omitempty"`
}

func BuildParams(paneIDs []string) map[string]any {
	subscriptions := []Subscription{
		{Type: SubscribeWorkspaceCreated},
		{Type: SubscribeWorkspaceClosed},
		{Type: SubscribePaneCreated},
		{Type: SubscribePaneClosed},
		{Type: SubscribePaneAgentDetected},
	}
	for _, paneID := range paneIDs {
		subscriptions = append(subscriptions, Subscription{Type: SubscribePaneAgentStatusChanged, PaneID: paneID})
	}

	return map[string]any{"subscriptions": subscriptions}
}

// SubscribeFromSnapshot fetches a fresh session.snapshot (returned to the
// caller for tracking the bootstrap baseline) and establishes a subscription
// scoped to the panes it finds. Returns the new Subscriber and true on
// success; nil and false on any failure (caller owns Close()).
func SubscribeFromSnapshot(ctx context.Context) (*Subscriber, snapshot.Response, bool) {
	var resp snapshot.Response
	resp, err := resp.Fetch(ctx)
	if err != nil {
		slog.Error("session snapshot failed", "error", err)
		return nil, snapshot.Response{}, false
	}

	paneIDs := make([]string, 0, len(resp.Snapshot.Panes))
	for _, pane := range resp.Snapshot.Panes {
		paneIDs = append(paneIDs, pane.PaneID)
	}
	slog.Info("subscribing from session snapshot", "pane_count", len(paneIDs), "pane_ids", paneIDs)

	sub, err := NewSubscriber(BuildParams(paneIDs))
	if err != nil {
		slog.Error("subscribe failed", "error", err)
		return nil, snapshot.Response{}, false
	}
	return sub, resp, true
}
