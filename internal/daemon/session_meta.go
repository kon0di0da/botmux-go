package daemon

import (
	"sync"
	"time"
)

type SessionStatus string

const (
	StatusCreated    SessionStatus = "CREATED"
	StatusSpawning   SessionStatus = "SPAWNING"
	StatusReady      SessionStatus = "READY"
	StatusRecovering SessionStatus = "RECOVERING"
	StatusClosed     SessionStatus = "CLOSED"
)

type SessionMeta struct {
	SessionID  string
	BotID      string
	CliType    string
	CliPath    string
	WorkingDir string
	LastOutput []string
	CreatedAt  time.Time
	Closed     bool
	Status     SessionStatus

	mu         sync.Mutex
	lastActive time.Time
	onClose    []func()
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
	m.LastOutput = append(m.LastOutput, line)
	if len(m.LastOutput) > maxMemoryOutputLines {
		m.LastOutput = m.LastOutput[len(m.LastOutput)-maxMemoryOutputLines:]
	}
}

func (m *SessionMeta) SnapshotOutput() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.LastOutput))
	copy(out, m.LastOutput)
	return out
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
		SessionID:  m.SessionID,
		BotID:      m.BotID,
		CliType:    m.CliType,
		CliPath:    m.CliPath,
		WorkingDir: m.WorkingDir,
		LastOutput: lastOutput,
		LastActive: lastActive,
		CreatedAt:  m.CreatedAt,
		Closed:     m.Closed,
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
		SessionID:  ps.SessionID,
		BotID:      ps.BotID,
		CliType:    ps.CliType,
		CliPath:    ps.CliPath,
		WorkingDir: ps.WorkingDir,
		LastOutput: lastOutput,
		CreatedAt:  createdAt,
		Closed:     ps.Closed,
		Status:     status,
		lastActive: lastActive,
	}
}
