package snapshot

import (
	"encoding/json"
	"testing"
)

const liveSnapshotJSON = `{"type":"session_snapshot","snapshot":{"version":"0.9.0","protocol":22,"focused_workspace_id":"w3","focused_tab_id":"w3:t1","focused_pane_id":"w3:p1","workspaces":[{"workspace_id":"w3","number":1,"label":"~","focused":true,"pane_count":1,"tab_count":1,"active_tab_id":"w3:t1","agent_status":"unknown"}],"panes":[{"pane_id":"w3:p1","terminal_id":"term_65aef03e0a82e1","workspace_id":"w3","tab_id":"w3:t1","focused":true,"cwd":"/home/nabutabu","foreground_cwd":"/home/nabutabu","terminal_title":"nabutabu@NavyaAlienware:~","terminal_title_stripped":"nabutabu@NavyaAlienware:~","agent_status":"unknown","scroll":{"offset_from_bottom":0,"max_offset_from_bottom":0,"viewport_rows":24},"revision":1}],"agents":[]}}`

func TestUnmarshalLiveSnapshot(t *testing.T) {
	var resp Response
	if err := json.Unmarshal([]byte(liveSnapshotJSON), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Type != "session_snapshot" {
		t.Errorf("type = %q, want session_snapshot", resp.Type)
	}

	snap := resp.Snapshot
	if snap.Version != "0.9.0" || snap.Protocol != 22 {
		t.Errorf("version/protocol = %q/%d", snap.Version, snap.Protocol)
	}
	if snap.FocusedWorkspaceID == nil || *snap.FocusedWorkspaceID != "w3" {
		t.Errorf("focused workspace = %v, want w3", snap.FocusedWorkspaceID)
	}

	if len(snap.Workspaces) != 1 {
		t.Fatalf("workspaces = %d, want 1", len(snap.Workspaces))
	}
	ws := snap.Workspaces[0]
	if ws.WorkspaceID != "w3" || ws.ActiveTabID != "w3:t1" || ws.AgentStatus != AgentStatusUnknown {
		t.Errorf("workspace = %+v", ws)
	}
	if !ws.Focused || ws.PaneCount != 1 || ws.TabCount != 1 || ws.Label != "~" {
		t.Errorf("workspace = %+v", ws)
	}

	if len(snap.Panes) != 1 {
		t.Fatalf("panes = %d, want 1", len(snap.Panes))
	}
	pane := snap.Panes[0]
	if pane.PaneID != "w3:p1" || pane.TerminalID != "term_65aef03e0a82e1" {
		t.Errorf("pane = %+v", pane)
	}
	if pane.Revision != 1 || pane.AgentStatus != AgentStatusUnknown {
		t.Errorf("pane = %+v", pane)
	}
	if pane.Cwd == nil || *pane.Cwd != "/home/nabutabu" {
		t.Errorf("pane cwd = %v", pane.Cwd)
	}
	if pane.Scroll == nil || pane.Scroll.ViewportRows != 24 {
		t.Errorf("pane scroll = %+v", pane.Scroll)
	}

	if len(snap.Agents) != 0 {
		t.Fatalf("agents = %d, want 0", len(snap.Agents))
	}
}

func TestAgentFieldsFromSchema(t *testing.T) {
	var agent Agent
	if err := json.Unmarshal([]byte(`{
		"terminal_id":"term_1","agent_status":"working","workspace_id":"w1",
		"tab_id":"w1:t1","pane_id":"w1:p1","focused":false,"revision":5,
		"name":"codex","agent":"codex","display_agent":"Codex",
		"interactive_ready":true,"launch_pending":false,
		"screen_detection_skipped":false,"state_change_seq":3,
		"state_labels":{"state":"Implementing"},
		"agent_session":{"source":"panel","agent":"codex","kind":"id","value":"a1"}
	}`), &agent); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if agent.AgentStatus != AgentStatusWorking || agent.StateChangeSeq != 3 {
		t.Errorf("agent = %+v", agent)
	}
	if agent.Name == nil || *agent.Name != "codex" {
		t.Errorf("agent name = %v", agent.Name)
	}
	if !agent.InteractiveReady || agent.LaunchPending {
		t.Errorf("agent = %+v", agent)
	}
	if agent.AgentSession == nil || agent.AgentSession.Kind != AgentSessionRefKindID {
		t.Errorf("agent session = %+v", agent.AgentSession)
	}
}
