package daemon

import (
	"sync"
	"time"

	"botmux-go/internal/protocol"
)

type SessionStatus string

const (
	StatusCreated    SessionStatus = "CREATED"
	StatusSpawning   SessionStatus = "SPAWNING"
	StatusReady      SessionStatus = "READY"
	StatusRecovering SessionStatus = "RECOVERING"
	StatusClosed     SessionStatus = "CLOSED"
)

func NewSessionMeta(sid, botID string) *SessionMeta {
	return &SessionMeta{
		SessionID:        sid,
		BotID:            botID,
		CreatedAt:        time.Now(),
		Status:           StatusCreated,
		outputNotifyCh:   make(chan struct{}),
		terminalNotifyCh: make(chan struct{}),
	}
}

type SessionMeta struct {
	SessionID    string
	BotID        string
	CliType      string
	CliPath      string
	Model        string
	CodexProfile string
	CliSessionID string
	WorkingDir   string
	LastOutput   []string
	CreatedAt    time.Time
	Closed       bool
	Status       SessionStatus

	mu               sync.Mutex
	lastActive       time.Time
	onClose          []func()
	outputSeq        uint64
	outputNotifyCh   chan struct{}
	turnActive       bool
	turnCancelling   bool
	turnToken        uint64
	terminalSeq      uint64
	terminals        []protocol.TurnTerminal
	terminalNotifyCh chan struct{}
}

func (m *SessionMeta) touchActive() {
	m.mu.Lock()
	m.lastActive = time.Now()
	m.mu.Unlock()
}

func (m *SessionMeta) LastActive() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastActive
}

const maxMemoryOutputLines = 2000

func (m *SessionMeta) AddOutput(line string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureOutputStateLocked()
	m.LastOutput = append(m.LastOutput, line)
	m.outputSeq++
	if len(m.LastOutput) > maxMemoryOutputLines {
		m.LastOutput = m.LastOutput[len(m.LastOutput)-maxMemoryOutputLines:]
	}
	notifyCh := m.outputNotifyCh
	m.outputNotifyCh = make(chan struct{})
	close(notifyCh)
}

func (m *SessionMeta) SnapshotOutput() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.LastOutput))
	copy(out, m.LastOutput)
	return out
}

func (m *SessionMeta) OutputCursor() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureOutputStateLocked()
	return m.outputSeq
}

func (m *SessionMeta) OutputSubscription() (uint64, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureOutputStateLocked()
	return m.outputSeq, m.outputNotifyCh
}

func (m *SessionMeta) SnapshotOutputSince(cursor uint64) ([]string, uint64, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureOutputStateLocked()

	next := m.outputSeq
	base := next - uint64(len(m.LastOutput))
	if cursor < base {
		cursor = base
	}
	if cursor > next {
		cursor = next
	}
	start := int(cursor - base)
	out := make([]string, len(m.LastOutput)-start)
	copy(out, m.LastOutput[start:])
	return out, next, m.outputNotifyCh
}

func (m *SessionMeta) ensureOutputStateLocked() {
	if m.outputNotifyCh == nil {
		m.outputNotifyCh = make(chan struct{})
	}
	if minimum := uint64(len(m.LastOutput)); m.outputSeq < minimum {
		m.outputSeq = minimum
	}
}

const maxMemoryTerminals = 16

type TurnSnapshot struct {
	Active     bool
	Cancelling bool
	Token      uint64
	Latest     *protocol.TurnTerminal
}

func (m *SessionMeta) BeginTurn() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.turnActive {
		return false
	}
	m.turnToken++
	m.turnActive = true
	m.turnCancelling = false
	return true
}

func (m *SessionMeta) BeginTurnCancel() (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.turnActive || m.turnCancelling {
		return 0, false
	}
	m.turnCancelling = true
	return m.turnToken, true
}

func (m *SessionMeta) CompleteTurn(terminal protocol.TurnTerminal) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.turnActive {
		return false
	}
	m.finishTurnLocked(terminal)
	return true
}

func (m *SessionMeta) FailCancellingTurn(token uint64, terminal protocol.TurnTerminal) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.turnActive || !m.turnCancelling || m.turnToken != token {
		return false
	}
	m.finishTurnLocked(terminal)
	return true
}

func (m *SessionMeta) TurnSnapshot() TurnSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := TurnSnapshot{
		Active:     m.turnActive,
		Cancelling: m.turnCancelling,
		Token:      m.turnToken,
	}
	if len(m.terminals) > 0 {
		latest := m.terminals[len(m.terminals)-1]
		snapshot.Latest = &latest
	}
	return snapshot
}

func (m *SessionMeta) FinishTurn() {
	m.mu.Lock()
	m.turnActive = false
	m.turnCancelling = false
	m.mu.Unlock()
}

func (m *SessionMeta) PublishTerminal(terminal protocol.TurnTerminal) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishTerminalLocked(terminal)
}

func (m *SessionMeta) finishTurnLocked(terminal protocol.TurnTerminal) {
	m.turnActive = false
	m.turnCancelling = false
	m.publishTerminalLocked(terminal)
}

func (m *SessionMeta) publishTerminalLocked(terminal protocol.TurnTerminal) {
	m.ensureTerminalStateLocked()
	m.terminals = append(m.terminals, terminal)
	m.terminalSeq++
	if len(m.terminals) > maxMemoryTerminals {
		m.terminals = m.terminals[len(m.terminals)-maxMemoryTerminals:]
	}
	notifyCh := m.terminalNotifyCh
	m.terminalNotifyCh = make(chan struct{})
	close(notifyCh)
}

func (m *SessionMeta) TerminalSubscription() (uint64, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureTerminalStateLocked()
	return m.terminalSeq, m.terminalNotifyCh
}

func (m *SessionMeta) SnapshotTerminalsSince(cursor uint64) ([]protocol.TurnTerminal, uint64, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureTerminalStateLocked()
	next := m.terminalSeq
	base := next - uint64(len(m.terminals))
	if cursor < base {
		cursor = base
	}
	if cursor > next {
		cursor = next
	}
	start := int(cursor - base)
	terminals := make([]protocol.TurnTerminal, len(m.terminals)-start)
	copy(terminals, m.terminals[start:])
	return terminals, next, m.terminalNotifyCh
}

func (m *SessionMeta) SetCliSessionID(id string) {
	m.mu.Lock()
	m.CliSessionID = id
	m.mu.Unlock()
}

func (m *SessionMeta) GetCliSessionID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CliSessionID
}

func (m *SessionMeta) ensureTerminalStateLocked() {
	if m.terminalNotifyCh == nil {
		m.terminalNotifyCh = make(chan struct{})
	}
	if minimum := uint64(len(m.terminals)); m.terminalSeq < minimum {
		m.terminalSeq = minimum
	}
}

func (m *SessionMeta) AddOnClose(fn func()) {
	m.mu.Lock()
	m.onClose = append(m.onClose, fn)
	m.mu.Unlock()
}

func (m *SessionMeta) DrainOnClose() []func() {
	m.mu.Lock()
	fns := m.onClose
	m.onClose = nil
	m.mu.Unlock()
	return fns
}

func (m *SessionMeta) ToPersisted() *PersistedSession {
	m.mu.Lock()
	lastActive := m.lastActive
	lastOutput := make([]string, len(m.LastOutput))
	copy(lastOutput, m.LastOutput)
	m.mu.Unlock()
	return &PersistedSession{
		SessionID:    m.SessionID,
		BotID:        m.BotID,
		CliType:      m.CliType,
		CliPath:      m.CliPath,
		Model:        m.Model,
		CodexProfile: m.CodexProfile,
		CliSessionID: m.CliSessionID,
		WorkingDir:   m.WorkingDir,
		LastOutput:   lastOutput,
		LastActive:   lastActive,
		CreatedAt:    m.CreatedAt,
		Closed:       m.Closed,
	}
}

func SessionMetaFromPersisted(ps *PersistedSession) *SessionMeta {
	status := StatusCreated
	if ps.Closed {
		status = StatusClosed
	}
	lastOutput := make([]string, len(ps.LastOutput))
	copy(lastOutput, ps.LastOutput)
	lastActive := ps.LastActive
	if lastActive.IsZero() {
		lastActive = time.Now()
	}
	createdAt := ps.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	return &SessionMeta{
		SessionID:        ps.SessionID,
		BotID:            ps.BotID,
		CliType:          ps.CliType,
		CliPath:          ps.CliPath,
		Model:            ps.Model,
		CodexProfile:     ps.CodexProfile,
		CliSessionID:     ps.CliSessionID,
		WorkingDir:       ps.WorkingDir,
		LastOutput:       lastOutput,
		CreatedAt:        createdAt,
		Closed:           ps.Closed,
		Status:           status,
		lastActive:       lastActive,
		outputSeq:        uint64(len(lastOutput)),
		outputNotifyCh:   make(chan struct{}),
		terminalNotifyCh: make(chan struct{}),
	}
}
