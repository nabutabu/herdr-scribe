package tracker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/events"
	"github.com/nabutabu/herdr-scribe/internal/snapshot"
)

type WorkspaceState struct {
	WorkspaceID string
	Label       string
	UpdatedAt   time.Time
}

type PaneState struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	Agent       string
	Status      snapshot.AgentStatus
	UpdatedAt   time.Time
}

type TabState struct {
	TabID       string
	WorkspaceID string
	Label       string // only ever known via snapshot — no event carries it
	UpdatedAt   time.Time
}

type AgentState struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	AgentType   string // e.g. "codex" — from the event's Agent field

	Status         snapshot.AgentStatus
	StateEnteredAt time.Time

	// DurationByState holds only *closed* intervals — time already spent in
	// a state before the most recent transition out of it. The open
	// interval for the current Status is deliberately not kept here;
	// callers wanting an as-of-now total add time.Since(StateEnteredAt).
	DurationByState map[snapshot.AgentStatus]time.Duration

	// AttentionStartedAt is set when Status transitions into
	// snapshot.AgentStatusDone, zero otherwise. Kept separate from
	// DurationByState because attention latency is its own MVP metric
	// (2.4, 3.5), not folded into generic "time in done".
	AttentionStartedAt time.Time
}

// Tracker holds the live picture of the session as currently known. Steady
// state is event-driven — the app's dispatcher calls the per-kind Apply*
// methods; the bootstrap snapshot is folded in once via ApplySnapshot. It is
// never adopted from a snapshot after that — Run's reconcile step only diffs
// and signals, it never mutates, so that diffing has an independent truth to
// compare against (0.4: events carry no sequence number and the stream can
// silently stall).
type Tracker struct {
	mu         sync.RWMutex
	workspaces map[string]WorkspaceState
	tabs       map[string]TabState
	panes      map[string]PaneState
	agents     map[string]AgentState

	// now is the clock used by the Apply* methods for duration math. Defaults
	// to time.Now; tests substitute a fake to make duration assertions
	// deterministic.
	now func() time.Time
}

func NewTracker() *Tracker {
	return &Tracker{
		workspaces: map[string]WorkspaceState{},
		tabs:       map[string]TabState{},
		panes:      map[string]PaneState{},
		agents:     map[string]AgentState{},
		now:        time.Now,
	}
}

// ApplyWorkspaceCreated folds a workspace.created event into the tracked state.
func (t *Tracker) ApplyWorkspaceCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workspaces[ev.WorkspaceID] = WorkspaceState{WorkspaceID: ev.WorkspaceID, UpdatedAt: t.now()}
}

// ApplyWorkspaceClosed removes a workspace and cascades its tabs and panes —
// their own close events may not arrive.
func (t *Tracker) ApplyWorkspaceClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.workspaces, ev.WorkspaceID)
	for id, tab := range t.tabs {
		if tab.WorkspaceID == ev.WorkspaceID {
			delete(t.tabs, id)
		}
	}
	for id, pane := range t.panes {
		if pane.WorkspaceID == ev.WorkspaceID {
			delete(t.panes, id)
		}
	}
}

// ApplyPaneCreated records a pane and seeds tab membership. No tab events
// exist (0.2), so pane.created is the only event that can seed a tab.
func (t *Tracker) ApplyPaneCreated(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.panes[ev.PaneID] = PaneState{PaneID: ev.PaneID, WorkspaceID: ev.WorkspaceID, TabID: ev.TabID, UpdatedAt: now}
	if ev.TabID != "" {
		tab := t.tabs[ev.TabID]
		tab.TabID = ev.TabID
		tab.WorkspaceID = ev.WorkspaceID
		tab.UpdatedAt = now
		t.tabs[ev.TabID] = tab
	}
}

// ApplyPaneClosed removes a pane.
func (t *Tracker) ApplyPaneClosed(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.panes, ev.PaneID)
}

// ApplyAgentDetected records which agent holds a pane.
func (t *Tracker) ApplyAgentDetected(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pane := t.panes[ev.PaneID]
	pane.Agent = ev.Agent
	pane.UpdatedAt = t.now()
	t.panes[ev.PaneID] = pane
}

// ApplyAgentStatusChanged updates the pane and, for the pane's agent, closes
// out the duration of the previous state and opens the new one (2.3). Same
// state re-applied is a no-op (idempotency seam, 2.5). Attention-latency
// tracking for `done` (2.4) is deliberately not wired here.
func (t *Tracker) ApplyAgentStatusChanged(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()

	// Upsert: a status change may arrive for a pane whose created event was
	// missed or which predates the subscription.
	pane := t.panes[ev.PaneID]
	pane.PaneID = ev.PaneID
	pane.WorkspaceID = ev.WorkspaceID
	pane.Agent = ev.Agent
	pane.Status = ev.NewState
	pane.UpdatedAt = now
	t.panes[ev.PaneID] = pane

	ag, ok := t.agents[ev.PaneID]
	if !ok {
		tabID := ev.TabID
		if tabID == "" {
			tabID = pane.TabID
		}
		t.agents[ev.PaneID] = AgentState{
			PaneID:          ev.PaneID,
			WorkspaceID:     ev.WorkspaceID,
			TabID:           tabID,
			AgentType:       ev.Agent,
			Status:          ev.NewState,
			StateEnteredAt:  now,
			DurationByState: map[snapshot.AgentStatus]time.Duration{},
		}
		return
	}

	if ag.Status == ev.NewState {
		return
	}

	ag.DurationByState[ag.Status] += now.Sub(ag.StateEnteredAt)
	ag.Status = ev.NewState
	ag.StateEnteredAt = now
	ag.WorkspaceID = ev.WorkspaceID
	if ev.Agent != "" {
		ag.AgentType = ev.Agent
	}
	t.agents[ev.PaneID] = ag
}

// ApplySnapshot folds a full session.snapshot into the tracked state. Used as
// the one-shot baseline on startup and again after a reconnect re-bootstrap,
// when the event stream may have a gap. After this, the stream owns the state
// until the next re-bootstrap.
func (t *Tracker) ApplySnapshot(snap snapshot.Snapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()

	clear(t.workspaces)
	clear(t.tabs)
	clear(t.panes)
	clear(t.agents)

	for _, ws := range snap.Workspaces {
		t.workspaces[ws.WorkspaceID] = WorkspaceState{WorkspaceID: ws.WorkspaceID, Label: ws.Label, UpdatedAt: now}
	}
	for _, tab := range snap.Tabs {
		t.tabs[tab.TabID] = TabState{TabID: tab.TabID, WorkspaceID: tab.WorkspaceID, Label: tab.Label, UpdatedAt: now}
	}
	for _, pane := range snap.Panes {
		state := PaneState{PaneID: pane.PaneID, WorkspaceID: pane.WorkspaceID, TabID: pane.TabID, Status: pane.AgentStatus, UpdatedAt: now}
		if pane.Agent != nil {
			state.Agent = *pane.Agent
		}
		t.panes[pane.PaneID] = state

		// Seed an agent record so post-bootstrap transitions have a prior
		// state to close out a duration against.
		if pane.Agent != nil {
			t.agents[pane.PaneID] = AgentState{
				PaneID:          pane.PaneID,
				WorkspaceID:     pane.WorkspaceID,
				TabID:           pane.TabID,
				AgentType:       *pane.Agent,
				Status:          pane.AgentStatus,
				StateEnteredAt:  now,
				DurationByState: map[snapshot.AgentStatus]time.Duration{},
			}
		}
	}
}

// Drift is one observed difference between tracked state and a snapshot.
type Drift struct {
	Kind    string // "workspace" | "pane"
	ID      string
	Tracked string // "" when absent from the tracker
	Actual  string // "" when absent from the snapshot
}

type DiffReport struct {
	Drifts []Drift
}

func (r DiffReport) Drifted() bool { return len(r.Drifts) > 0 }

// Diff compares tracked state against a fresh snapshot without mutating.
// Workspaces and panes participate; tabs deliberately do not — events can
// never remove a tab (0.2), so tab membership is not a reliable drift probe
// and would only manufacture false resubscribes.
func (t *Tracker) Diff(snap snapshot.Snapshot) DiffReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var drifts []Drift

	for id := range t.workspaces {
		if !hasWorkspace(snap.Workspaces, id) {
			drifts = append(drifts, Drift{Kind: "workspace", ID: id, Tracked: "present"})
		}
	}
	for _, ws := range snap.Workspaces {
		if _, ok := t.workspaces[ws.WorkspaceID]; !ok {
			drifts = append(drifts, Drift{Kind: "workspace", ID: ws.WorkspaceID, Actual: "present"})
		}
	}

	snapPanes := make(map[string]snapshot.Pane, len(snap.Panes))
	for _, pane := range snap.Panes {
		snapPanes[pane.PaneID] = pane
	}

	for id, pane := range t.panes {
		sp, ok := snapPanes[id]
		if !ok {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Tracked: describePaneTracked(pane)})
			continue
		}
		tracked := describePaneTracked(pane)
		actual := describePaneSnapshot(sp)
		if tracked != actual {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Tracked: tracked, Actual: actual})
		}
	}
	for id, sp := range snapPanes {
		if _, ok := t.panes[id]; !ok {
			drifts = append(drifts, Drift{Kind: "pane", ID: id, Actual: describePaneSnapshot(sp)})
		}
	}

	return DiffReport{Drifts: drifts}
}

// Run owns the periodic liveness check (1.8). It opens a fresh short-lived
// session.snapshot connection each interval, diffs it against the tracked
// state, and calls onDrift when they disagree — the sole signal for forcing a
// subscription re-create. Run never mutates tracker state.
func (t *Tracker) Run(ctx context.Context, interval time.Duration, onDrift func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reconcile(ctx, onDrift)
		}
	}
}

// reconcile performs one liveness probe. A fetch failure is logged and skipped
// — that is the loud-failure path owned by the subscriber's reconnect logic,
// not this check (0.4: this loop exists for the silent failure mode).
func (t *Tracker) reconcile(ctx context.Context, onDrift func()) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := snapshot.Fetch(tctx)
	cancel()
	if err != nil {
		slog.Warn("reconcile: session.snapshot failed", "error", err)
		return
	}

	report := t.Diff(resp.Snapshot)
	if !report.Drifted() {
		return
	}
	for _, d := range report.Drifts {
		slog.Warn("reconcile: state drift", "kind", d.Kind, "id", d.ID, "tracked", d.Tracked, "actual", d.Actual)
	}
	onDrift()
}

func hasWorkspace(ws []snapshot.Workspace, id string) bool {
	for _, w := range ws {
		if w.WorkspaceID == id {
			return true
		}
	}
	return false
}

func describePaneTracked(p PaneState) string {
	return fmt.Sprintf("workspace=%s tab=%s agent=%s status=%s", p.WorkspaceID, p.TabID, p.Agent, p.Status)
}

func describePaneSnapshot(p snapshot.Pane) string {
	agent := ""
	if p.Agent != nil {
		agent = *p.Agent
	}
	return fmt.Sprintf("workspace=%s tab=%s agent=%s status=%s", p.WorkspaceID, p.TabID, agent, p.AgentStatus)
}
