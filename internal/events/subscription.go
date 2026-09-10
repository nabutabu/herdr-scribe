package events

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
