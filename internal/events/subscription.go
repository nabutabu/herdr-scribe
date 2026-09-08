package events

type Subscription struct {
	Type   string `json:"type"`
	PaneID string `json:"pane_id,omitempty"`
}

const (
	EventWorkspaceCreated       = "workspace.created"
	EventWorkspaceClosed        = "workspace.closed"
	EventPaneCreated            = "pane.created"
	EventPaneClosed             = "pane.closed"
	EventPaneAgentStatusChanged = "pane.agent_status_changed"
	EventPaneAgentDetected      = "pane.agent_detected"
)

func BuildParams(paneIDs []string) map[string]any {
	subscriptions := []Subscription{
		{Type: EventWorkspaceCreated},
		{Type: EventWorkspaceClosed},
		{Type: EventPaneCreated},
		{Type: EventPaneClosed},
		{Type: EventPaneAgentDetected},
	}
	for _, paneID := range paneIDs {
		subscriptions = append(subscriptions, Subscription{Type: EventPaneAgentStatusChanged, PaneID: paneID})
	}

	return map[string]any{"subscriptions": subscriptions}
}
