package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type PersistedSession struct {
	SessionID    string    `json:"session_id"`
	BotID        string    `json:"bot_id"`
	CliType      string    `json:"cli_type"`
	CliPath      string    `json:"cli_path,omitempty"`
	Model        string    `json:"model,omitempty"`
	CodexProfile string    `json:"codex_profile,omitempty"`
	CliSessionID string    `json:"cli_session_id,omitempty"`
	WorkingDir   string    `json:"working_dir"`
	WorkerPID    int       `json:"worker_pid"`
	LastOutput   []string  `json:"last_output"`
	LastActive   time.Time `json:"last_active"`
	CreatedAt    time.Time `json:"created_at"`
	Closed       bool      `json:"closed"`
}

type SessionStore struct {
	mu  sync.Mutex
	dir string
	// beforeSave is a test hook. Production stores leave it nil.
	beforeSave func(*PersistedSession)
}

func NewSessionStore(dir string) *SessionStore {
	return &SessionStore{dir: dir}
}

func (s *SessionStore) save(ps *PersistedSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked(ps)
}

func (s *SessionStore) saveLocked(ps *PersistedSession) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.dir, ps.SessionID+".json")
	data, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return err
	}
	if s.beforeSave != nil {
		s.beforeSave(ps)
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func (s *SessionStore) load(sessionID string) (*PersistedSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(sessionID)
}

func (s *SessionStore) loadLocked(sessionID string) (*PersistedSession, error) {
	path := filepath.Join(s.dir, sessionID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ps PersistedSession
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, err
	}
	return &ps, nil
}

func (s *SessionStore) list() ([]*PersistedSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []*PersistedSession
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		ps, err := s.loadLocked(e.Name()[:len(e.Name())-len(".json")])
		if err != nil {
			continue
		}
		if !ps.Closed {
			result = append(result, ps)
		}
	}
	return result, nil
}

func (s *SessionStore) remove(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removeLocked(sessionID)
}

func (s *SessionStore) removeLocked(sessionID string) error {
	path := filepath.Join(s.dir, sessionID+".json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *SessionStore) MarkClosed(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ps, err := s.loadLocked(sessionID)
	if err != nil {
		return err
	}
	ps.Closed = true
	ps.LastActive = time.Now()
	return s.saveLocked(ps)
}

const maxPersistedOutputLines = 5000

func (s *SessionStore) UpdateOutput(sessionID string, output string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ps, err := s.loadLocked(sessionID)
	if err != nil {
		return err
	}
	if ps.Closed {
		return nil
	}
	ps.LastOutput = append(ps.LastOutput, output)
	if len(ps.LastOutput) > maxPersistedOutputLines {
		ps.LastOutput = ps.LastOutput[len(ps.LastOutput)-maxPersistedOutputLines:]
	}
	ps.LastActive = time.Now()
	return s.saveLocked(ps)
}

func (s *SessionStore) UpdateLastActive(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ps, err := s.loadLocked(sessionID)
	if err != nil {
		return err
	}
	if ps.Closed {
		return nil
	}
	ps.LastActive = time.Now()
	return s.saveLocked(ps)
}

func (s *SessionStore) UpdateWorkerPID(sessionID string, pid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ps, err := s.loadLocked(sessionID)
	if err != nil {
		return err
	}
	if ps.Closed {
		return nil
	}
	ps.WorkerPID = pid
	return s.saveLocked(ps)
}

func (s *SessionStore) UpdateCliSessionID(sessionID, cliSessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ps, err := s.loadLocked(sessionID)
	if err != nil {
		return err
	}
	if ps.Closed {
		return nil
	}
	ps.CliSessionID = cliSessionID
	ps.LastActive = time.Now()
	return s.saveLocked(ps)
}

func (s *SessionStore) List() ([]*PersistedSession, error) {
	return s.list()
}

func (s *SessionStore) String() string {
	return fmt.Sprintf("SessionStore(dir=%s)", s.dir)
}
