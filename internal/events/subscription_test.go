package events

import (
	"encoding/json"
	"testing"
)

func TestBuildParams(t *testing.T) {
	params := BuildParams([]string{"w1:p1", "w1:p2"})

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"subscriptions":[{"type":"workspace.created"},{"type":"workspace.closed"},{"type":"pane.created"},{"type":"pane.closed"},{"type":"pane.agent_detected"},{"type":"pane.agent_status_changed","pane_id":"w1:p1"},{"type":"pane.agent_status_changed","pane_id":"w1:p2"}]}`
	if string(raw) != want {
		t.Errorf("params JSON = %s, want %s", string(raw), want)
	}
}

func TestBuildParamsGlobalEvents(t *testing.T) {
	params := BuildParams(nil)

	subs := params["subscriptions"].([]Subscription)
	wantGlobal := []string{
		EventWorkspaceCreated,
		EventWorkspaceClosed,
		EventPaneCreated,
		EventPaneClosed,
		EventPaneAgentDetected,
	}

	seen := make(map[string]int)
	for _, s := range subs {
		seen[s.Type]++
	}

	if len(subs) != len(wantGlobal) {
		t.Fatalf("subscriptions = %d, want %d", len(subs), len(wantGlobal))
	}
	for _, typ := range wantGlobal {
		if seen[typ] != 1 {
			t.Errorf("subscription %q count = %d, want 1", typ, seen[typ])
		}
	}
}

func TestBuildParamsPaneScoped(t *testing.T) {
	params := BuildParams([]string{"w1:p1", "w1:p2", "w1:p3"})

	subs := params["subscriptions"].([]Subscription)
	var scoped []Subscription
	for _, s := range subs {
		if s.Type == EventPaneAgentStatusChanged {
			scoped = append(scoped, s)
		}
	}

	if len(scoped) != 3 {
		t.Fatalf("agent_status_changed subscriptions = %d, want 3", len(scoped))
	}

	wantPanes := []string{"w1:p1", "w1:p2", "w1:p3"}
	for i, s := range scoped {
		if s.PaneID != wantPanes[i] {
			t.Errorf("subscription %d pane_id = %q, want %q", i, s.PaneID, wantPanes[i])
		}
	}
}

func TestBuildParamsNoPanesSkipsScoped(t *testing.T) {
	params := BuildParams(nil)

	subs := params["subscriptions"].([]Subscription)
	for _, s := range subs {
		if s.Type == EventPaneAgentStatusChanged {
			t.Errorf("unexpected agent_status_changed subscription: %+v", s)
		}
		if s.PaneID != "" {
			t.Errorf("subscription %q has unexpected pane_id: %+v", s.Type, s)
		}
	}
}

func TestBuildParamsExcludesNoise(t *testing.T) {
	for _, paneIDs := range [][]string{nil, {"w1:p1"}} {
		params := BuildParams(paneIDs)
		subs := params["subscriptions"].([]Subscription)
		for _, s := range subs {
			switch s.Type {
			case "pane.scroll_changed", "pane.output_matched", "pane.updated":
				t.Errorf("noise event subscribed: %q", s.Type)
			}
		}
	}
}
