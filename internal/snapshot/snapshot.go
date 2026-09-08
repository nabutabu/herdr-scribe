package snapshot

type Response struct {
	Type     string   `json:"type"`
	Snapshot Snapshot `json:"snapshot"`
}

type Snapshot struct {
	Version            string      `json:"version"`
	Protocol           uint32      `json:"protocol"`
	FocusedWorkspaceID *string     `json:"focused_workspace_id"`
	FocusedTabID       *string     `json:"focused_tab_id"`
	FocusedPaneID      *string     `json:"focused_pane_id"`
	Workspaces         []Workspace `json:"workspaces"`
	Panes              []Pane      `json:"panes"`
	Agents             []Agent     `json:"agents"`
}

type AgentStatus string

const (
	AgentStatusIdle    AgentStatus = "idle"
	AgentStatusWorking AgentStatus = "working"
	AgentStatusBlocked AgentStatus = "blocked"
	AgentStatusDone    AgentStatus = "done"
	AgentStatusUnknown AgentStatus = "unknown"
)

type Workspace struct {
	WorkspaceID string            `json:"workspace_id"`
	Number      uint              `json:"number"`
	Label       string            `json:"label"`
	Focused     bool              `json:"focused"`
	PaneCount   uint              `json:"pane_count"`
	TabCount    uint              `json:"tab_count"`
	ActiveTabID string            `json:"active_tab_id"`
	AgentStatus AgentStatus       `json:"agent_status"`
	Tokens      map[string]string `json:"tokens,omitempty"`
}

type Pane struct {
	PaneID                string            `json:"pane_id"`
	TerminalID            string            `json:"terminal_id"`
	WorkspaceID           string            `json:"workspace_id"`
	TabID                 string            `json:"tab_id"`
	Focused               bool              `json:"focused"`
	AgentStatus           AgentStatus       `json:"agent_status"`
	Revision              uint64            `json:"revision"`
	Cwd                   *string           `json:"cwd"`
	ForegroundCwd         *string           `json:"foreground_cwd"`
	Label                 *string           `json:"label"`
	Title                 *string           `json:"title"`
	TerminalTitle         *string           `json:"terminal_title"`
	TerminalTitleStripped *string           `json:"terminal_title_stripped"`
	Agent                 *string           `json:"agent"`
	DisplayAgent          *string           `json:"display_agent"`
	Scroll                *PaneScrollInfo   `json:"scroll"`
	AgentSession          *AgentSessionInfo `json:"agent_session"`
	StateLabels           map[string]string `json:"state_labels"`
	Tokens                map[string]string `json:"tokens"`
}

type Agent struct {
	PaneID                 string            `json:"pane_id"`
	TerminalID             string            `json:"terminal_id"`
	WorkspaceID            string            `json:"workspace_id"`
	TabID                  string            `json:"tab_id"`
	Focused                bool              `json:"focused"`
	AgentStatus            AgentStatus       `json:"agent_status"`
	Revision               uint64            `json:"revision"`
	Name                   *string           `json:"name"`
	Cwd                    *string           `json:"cwd"`
	ForegroundCwd          *string           `json:"foreground_cwd"`
	Label                  *string           `json:"label"`
	Title                  *string           `json:"title"`
	TerminalTitle          *string           `json:"terminal_title"`
	TerminalTitleStripped  *string           `json:"terminal_title_stripped"`
	Agent                  *string           `json:"agent"`
	DisplayAgent           *string           `json:"display_agent"`
	InteractiveReady       bool              `json:"interactive_ready"`
	LaunchPending          bool              `json:"launch_pending"`
	ScreenDetectionSkipped bool              `json:"screen_detection_skipped"`
	StateChangeSeq         uint64            `json:"state_change_seq"`
	Scroll                 *PaneScrollInfo   `json:"scroll"`
	AgentSession           *AgentSessionInfo `json:"agent_session"`
	StateLabels            map[string]string `json:"state_labels"`
	Tokens                 map[string]string `json:"tokens"`
}

type AgentSessionInfo struct {
	Source string              `json:"source"`
	Agent  string              `json:"agent"`
	Kind   AgentSessionRefKind `json:"kind"`
	Value  string              `json:"value"`
}

type AgentSessionRefKind string

const (
	AgentSessionRefKindID   AgentSessionRefKind = "id"
	AgentSessionRefKindPath AgentSessionRefKind = "path"
)

type PaneScrollInfo struct {
	OffsetFromBottom    uint64 `json:"offset_from_bottom"`
	MaxOffsetFromBottom uint64 `json:"max_offset_from_bottom"`
	ViewportRows        uint64 `json:"viewport_rows"`
}
