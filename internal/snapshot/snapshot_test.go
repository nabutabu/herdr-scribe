package snapshot

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
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

func TestUnmarshalTabShape(t *testing.T) {
	var resp Response
	if err := json.Unmarshal([]byte(`{"type":"session_snapshot","snapshot":{"tabs":[{"tab_id":"w3:t1","workspace_id":"w3","label":"~","number":1,"focused":true,"pane_count":1,"agent_status":"working"}]}}`), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(resp.Snapshot.Tabs) != 1 {
		t.Fatalf("tabs = %d, want 1", len(resp.Snapshot.Tabs))
	}
	tab := resp.Snapshot.Tabs[0]
	if tab.TabID != "w3:t1" || tab.WorkspaceID != "w3" {
		t.Errorf("tab = %+v", tab)
	}
	if !tab.Focused || tab.PaneCount != 1 || tab.Number != 1 || tab.Label != "~" {
		t.Errorf("tab = %+v", tab)
	}
	if tab.AgentStatus != AgentStatusWorking {
		t.Errorf("tab agent_status = %q, want working", tab.AgentStatus)
	}
}

func startStubServer(t *testing.T, handler func(*bufio.Reader, net.Conn) error) string {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "herdr-snapshot-test.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if err := handler(bufio.NewReader(conn), conn); err != nil {
			t.Errorf("stub server: %v", err)
		}
	}()

	return sockPath
}

func TestFetch(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		if err := json.Unmarshal(line, &struct {
			ID     string `json:"id"`
			Method string `json:"method"`
		}{}); err != nil {
			return fmt.Errorf("parsing request: %w", err)
		}
		_, err = conn.Write([]byte(`{"id":"herdr-scribe","result":{"type":"session_snapshot","snapshot":{"version":"0.9.0","protocol":22,"tabs":[{"tab_id":"w1:t1","workspace_id":"w1"}]}}}` + "\n"))
		return err
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if resp.Type != "session_snapshot" {
		t.Errorf("type = %q, want session_snapshot", resp.Type)
	}
	if len(resp.Snapshot.Tabs) != 1 || resp.Snapshot.Tabs[0].TabID != "w1:t1" {
		t.Errorf("snapshot = %+v", resp.Snapshot)
	}
}

func TestFetchUnmarshalError(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := r.ReadBytes('\n'); err != nil {
			return err
		}
		_, err := conn.Write([]byte("this is not json\n"))
		return err
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	if _, err := Fetch(context.Background()); err == nil {
		t.Fatal("expected error for malformed response")
	}
}
