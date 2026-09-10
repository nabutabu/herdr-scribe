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

// Tracker holds the live picture of the session as currently known. Steady
// state is event-driven (ApplyEvent); the bootstrap snapshot is folded in once
// via ApplySnapshot. It is never adopted from a snapshot after that — Run's
// reconcile step only diffs and signals, it never mutates, so that diffing has
// an independent truth to compare against (0.4: events carry no sequence
// number and the stream can silently stall).
type Tracker struct {
	mu         sync.RWMutex
	workspaces map[string]WorkspaceState
	tabs       map[string]TabState
	panes      map[string]PaneState
}

func NewTracker() *Tracker {
	return &Tracker{workspaces: map[string]WorkspaceState{}, tabs: map[string]TabState{}, panes: map[string]PaneState{}}
}

// ApplyEvent folds a normalized event into the tracked state. This is the only
// writer in steady state.
func (t *Tracker) ApplyEvent(ev events.NormalizedEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()

	switch ev.Kind {
	case events.KindWorkspaceCreated:
		t.workspaces[ev.WorkspaceID] = WorkspaceState{WorkspaceID: ev.WorkspaceID, UpdatedAt: now}

	case events.KindWorkspaceClosed:
		delete(t.workspaces, ev.WorkspaceID)
		// panes/tabs under this workspace are gone with it — cascade so the
		// tracker doesn't keep dead children (their own close events may not
		// arrive).
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

	case events.KindPaneCreated:
		t.panes[ev.PaneID] = PaneState{PaneID: ev.PaneID, WorkspaceID: ev.WorkspaceID, TabID: ev.TabID, UpdatedAt: now}
		// No tab events exist (0.2), so pane.created is the only event that
		// can seed tab membership.
		if ev.TabID != "" {
			tab := t.tabs[ev.TabID]
			tab.TabID = ev.TabID
			tab.WorkspaceID = ev.WorkspaceID
			tab.UpdatedAt = now
			t.tabs[ev.TabID] = tab
		}

	case events.KindPaneClosed:
		delete(t.panes, ev.PaneID)

	case events.KindAgentDetected:
		pane := t.panes[ev.PaneID]
		pane.Agent = ev.Agent
		pane.UpdatedAt = now
		t.panes[ev.PaneID] = pane

	case events.KindAgentStatusChanged:
		// Upsert: a status change may arrive for a pane whose created event was
		// missed or which predates the subscription.
		pane := t.panes[ev.PaneID]
		pane.PaneID = ev.PaneID
		pane.WorkspaceID = ev.WorkspaceID
		pane.Agent = ev.Agent
		pane.Status = ev.NewState
		pane.UpdatedAt = now
		t.panes[ev.PaneID] = pane
	}
}

// ApplySnapshot folds a full session.snapshot into the tracked state. Used as
// the one-shot baseline on startup and again after a reconnect re-bootstrap,
// when the event stream may have a gap. After this, the stream owns the state
// until the next re-bootstrap.
func (t *Tracker) ApplySnapshot(snap snapshot.Snapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()

	clear(t.workspaces)
	clear(t.tabs)
	clear(t.panes)

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
	resp, err := (snapshot.Response{}).Fetch(tctx)
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