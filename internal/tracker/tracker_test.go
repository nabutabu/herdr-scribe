package tracker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

func workingPanes(agent string) []snapshot.Pane {
	return []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking}}
}

func TestApplyEventTracksState(t *testing.T) {
	tr := NewTracker()
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindWorkspaceCreated, WorkspaceID: "w1"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindAgentDetected, PaneID: "w1:p1", Agent: "codex"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", Agent: "codex", NewState: snapshot.AgentStatusWorking})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if _, ok := tr.workspaces["w1"]; !ok {
		t.Error("workspace w1 not tracked")
	}
	pane, ok := tr.panes["w1:p1"]
	if !ok {
		t.Fatal("pane w1:p1 not tracked")
	}
	if pane.Agent != "codex" || pane.Status != snapshot.AgentStatusWorking || pane.TabID != "w1:t1" {
		t.Errorf("pane = %+v", pane)
	}
	if _, ok := tr.tabs["w1:t1"]; !ok {
		t.Error("tab w1:t1 not seeded from pane.created")
	}
}

func TestApplyEventStatusChangeUpsertsUnknownPane(t *testing.T) {
	tr := NewTracker()
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", WorkspaceID: "w1", Agent: "codex", NewState: snapshot.AgentStatusBlocked})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	pane, ok := tr.panes["w1:p1"]
	if !ok {
		t.Fatal("pane w1:p1 not upserted")
	}
	if pane.Status != snapshot.AgentStatusBlocked {
		t.Errorf("pane status = %q, want blocked", pane.Status)
	}
}

func TestApplyEventWorkspaceClosedCascades(t *testing.T) {
	tr := NewTracker()
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindWorkspaceCreated, WorkspaceID: "w1"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindWorkspaceClosed, WorkspaceID: "w1"})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if _, ok := tr.workspaces["w1"]; ok {
		t.Error("workspace w1 still tracked after close")
	}
	if _, ok := tr.panes["w1:p1"]; ok {
		t.Error("pane w1:p1 still tracked after workspace close")
	}
	if _, ok := tr.tabs["w1:t1"]; ok {
		t.Error("tab w1:t1 still tracked after workspace close")
	}
}

func TestApplySnapshotSeedsBaseline(t *testing.T) {
	tr := NewTracker()
	agent := "codex"
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1", Label: "herdr"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1", Label: "~"}},
		Panes:      []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusWorking, Agent: &agent}},
	})

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	ws := tr.workspaces["w1"]
	if ws.Label != "herdr" {
		t.Errorf("workspace label = %q, want herdr", ws.Label)
	}
	pane := tr.panes["w1:p1"]
	if pane.Agent != "codex" || pane.Status != snapshot.AgentStatusWorking {
		t.Errorf("pane = %+v", pane)
	}
	if tab := tr.tabs["w1:t1"]; tab.Label != "~" {
		t.Errorf("tab label = %q, want ~", tab.Label)
	}
}

func TestDiffNoDrift(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindWorkspaceCreated, WorkspaceID: "w1"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindPaneCreated, PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1"})
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", WorkspaceID: "w1", Agent: "", NewState: snapshot.AgentStatusWorking})

	if report := tr.Diff(snap); report.Drifted() {
		t.Fatalf("unexpected drift: %+v", report.Drifts)
	}
}

func TestDiffDetectsStatusChange(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplySnapshot(snap)
	// Event stream says blocked; snapshot says working -> drift.
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindAgentStatusChanged, PaneID: "w1:p1", WorkspaceID: "w1", NewState: snapshot.AgentStatusBlocked})

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift after status change not mirrored in snapshot")
	}
	if len(report.Drifts) != 1 || report.Drifts[0].Kind != "pane" || report.Drifts[0].ID != "w1:p1" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsAddedPane(t *testing.T) {
	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes:      []snapshot.Pane{{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusIdle}},
	})

	// Snapshot now contains a pane the tracker has never seen.
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes: []snapshot.Pane{
			{PaneID: "w1:p1", WorkspaceID: "w1", TabID: "w1:t1", AgentStatus: snapshot.AgentStatusIdle},
			{PaneID: "w1:p2", WorkspaceID: "w1", TabID: "w1:t2", AgentStatus: snapshot.AgentStatusWorking},
		},
	}

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift for added pane")
	}
	if report.Drifts[0].Kind != "pane" || report.Drifts[0].ID != "w1:p2" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsRemovedPane(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplySnapshot(snap)
	// Pane closed in the stream, but the snapshot still lists it -> drift.
	tr.ApplyEvent(events.NormalizedEvent{Kind: events.KindPaneClosed, PaneID: "w1:p1"})

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift for removed pane")
	}
	if report.Drifts[0].Kind != "pane" || report.Drifts[0].ID != "w1:p1" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffDetectsAddedWorkspace(t *testing.T) {
	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}})

	snap := snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}, {WorkspaceID: "w2"}}}

	report := tr.Diff(snap)
	if !report.Drifted() {
		t.Fatal("expected drift for added workspace")
	}
	if report.Drifts[0].Kind != "workspace" || report.Drifts[0].ID != "w2" {
		t.Errorf("drifts = %+v", report.Drifts)
	}
}

func TestDiffIgnoresTabMembership(t *testing.T) {
	snap := snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}, {TabID: "w1:t2", WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	}

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{
		Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}},
		Tabs:       []snapshot.Tab{{TabID: "w1:t1", WorkspaceID: "w1"}},
		Panes:      workingPanes(""),
	})

	if report := tr.Diff(snap); report.Drifted() {
		t.Fatalf("tab-only changes must not drift: %+v", report.Drifts)
	}
}

// startSnapshotStub runs a unix socket server that accepts connections, reads
// one session.snapshot request, and answers with the JSON produced by respond.
// Multiple connections are supported (each RPC opens its own).
func startSnapshotStub(t *testing.T, respond func() string) string {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "herdr-tracker-test.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadBytes('\n')
				if err != nil {
					return
				}
				var req struct {
					Method string `json:"method"`
				}
				if err := json.Unmarshal(line, &req); err != nil {
					return
				}
				if req.Method != "session.snapshot" {
					fmt.Fprintf(conn, `{"id":"herdr-scribe","error":{"code":"MethodNotFound","message":"no such method: %s"}}`+"\n", req.Method)
					return
				}
				body := respond()
				if body == "" {
					conn.Write([]byte("this is not json\n"))
					return
				}
				fmt.Fprintf(conn, `{"id":"herdr-scribe","result":%s}`+"\n", body)
			}(conn)
		}
	}()

	return sockPath
}

func TestRunCallsOnDrift(t *testing.T) {
	// Snapshot says the pane is blocked; event-fed tracker says working.
	driftBody := `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"blocked"}]}}`
	sock := startSnapshotStub(t, func() string { return driftBody })
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fired := make(chan struct{}, 1)
	go tr.Run(ctx, 20*time.Millisecond, func() { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("onDrift not called despite drift")
	}
}

func TestRunSilentWhenInSync(t *testing.T) {
	syncBody := `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"working"}]}}`
	sock := startSnapshotStub(t, func() string { return syncBody })
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := make(chan struct{}, 1)
	go tr.Run(ctx, 20*time.Millisecond, func() { called <- struct{}{} })

	select {
	case <-called:
		t.Fatal("onDrift called despite matching state")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRunSurvivesFetchError(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	body := `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"blocked"}]}}`
	sock := startSnapshotStub(t, func() string {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 2 {
			return "" // malformed response -> fetch error
		}
		return body
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fired := make(chan struct{}, 1)
	go tr.Run(ctx, 20*time.Millisecond, func() { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not survive fetch errors and fire on a later good tick")
	}
}

func TestRunsStopsOnCancel(t *testing.T) {
	sock := startSnapshotStub(t, func() string {
		return `{"type":"session_snapshot","snapshot":{"workspaces":[{"workspace_id":"w1"}],"panes":[{"pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1","agent_status":"working"}]}}`
	})
	t.Setenv("HERDR_SOCKET_PATH", sock)

	tr := NewTracker()
	tr.ApplySnapshot(snapshot.Snapshot{Workspaces: []snapshot.Workspace{{WorkspaceID: "w1"}}, Panes: workingPanes("")})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		tr.Run(ctx, 20*time.Millisecond, func() {})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}