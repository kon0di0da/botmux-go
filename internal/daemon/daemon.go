package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"botmux-go/internal/config"
	"botmux-go/internal/protocol"
)

const (
	defaultCancelTurnTimeout  = 10 * time.Second
	defaultRestartWorkerGrace = 2 * time.Second
	maxCurrentWorkerSendTries = 3
	workerReadyAckPayload     = "worker_ready"
)

type cancelSendFailureResult uint8

const (
	cancelSendFailureCommitted cancelSendFailureResult = iota
	cancelSendFailureRetry
	cancelSendFailureStaleSession
	cancelSendFailureNoLongerCancelling
)

type sessionFileLockEntry struct {
	mu   sync.Mutex
	refs int
}

type Daemon struct {
	cfg          *config.DaemonConfig
	listener     net.Listener
	httpListener net.Listener
	selfExe      string
	store        *SessionStore

	ctx       context.Context
	cancel    context.CancelFunc
	startedAt time.Time

	sessionsMu sync.RWMutex
	sessions   map[string]*SessionMeta
	// sessionFileLocks serializes lifecycle filesystem operations by session ID.
	sessionFileLocksMu sync.Mutex
	sessionFileLocks   map[string]*sessionFileLockEntry
	// pendingRestart identifies the worker generation that must not reopen a
	// recovering session with a late READY.
	pendingRestart map[*SessionMeta]*WorkerHandle

	workersMu sync.RWMutex
	workers   map[string]*WorkerHandle
	// workerOwners binds each worker generation to the exact SessionMeta
	// instance that created it. Session IDs can be purged and reused.
	workerOwners map[*WorkerHandle]*SessionMeta

	connMapMu  sync.Mutex
	connToSess map[net.Conn]string

	closed  bool
	closeMu sync.Mutex

	wg sync.WaitGroup

	monitorMu     sync.Mutex
	monitorStop   chan struct{}
	spawnFailures map[string]*spawnFailure
	spawnSem      chan struct{}

	turnCancelTimeout  time.Duration
	restartWorkerGrace time.Duration
}

func New(cfg *config.DaemonConfig) (*Daemon, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get self exe: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Daemon{
		cfg:              cfg,
		selfExe:          exe,
		store:            NewSessionStore(cfg.SessionsDir),
		ctx:              ctx,
		cancel:           cancel,
		sessions:         make(map[string]*SessionMeta),
		sessionFileLocks: make(map[string]*sessionFileLockEntry),
		workers:          make(map[string]*WorkerHandle),
		workerOwners:     make(map[*WorkerHandle]*SessionMeta),
		connToSess:       make(map[net.Conn]string),
		pendingRestart:   make(map[*SessionMeta]*WorkerHandle),
	}, nil
}

// lockSessionFile must be acquired before workersMu or sessionsMu for a
// session lifecycle operation that changes the session's persisted file.
func (d *Daemon) lockSessionFile(sessionID string) func() {
	d.sessionFileLocksMu.Lock()
	if d.sessionFileLocks == nil {
		d.sessionFileLocks = make(map[string]*sessionFileLockEntry)
	}
	entry := d.sessionFileLocks[sessionID]
	if entry == nil {
		entry = &sessionFileLockEntry{}
		d.sessionFileLocks[sessionID] = entry
	}
	entry.refs++
	d.sessionFileLocksMu.Unlock()

	entry.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.Unlock()

			d.sessionFileLocksMu.Lock()
			entry.refs--
			if entry.refs == 0 && d.sessionFileLocks[sessionID] == entry {
				delete(d.sessionFileLocks, sessionID)
			}
			d.sessionFileLocksMu.Unlock()
		})
	}
}

func (d *Daemon) Start() error {
	if err := d.cfg.EnsureDirs(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", d.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", d.cfg.ListenAddr, err)
	}
	d.listener = ln
	d.startedAt = time.Now()
	log.Printf("[daemon] listening on %s (self=%s, sessions_dir=%s)", d.cfg.ListenAddr, d.selfExe, d.cfg.SessionsDir)

	d.restoreSessions()
	d.startSessionMonitor()

	d.wg.Add(3)
	go d.acceptLoop()
	go d.periodicGC()
	go d.startHTTPServer()
	return nil
}

func (d *Daemon) Wait() {
	d.wg.Wait()
}

func (d *Daemon) Stop() error {
	d.closeMu.Lock()
	if d.closed {
		d.closeMu.Unlock()
		return nil
	}
	d.closed = true
	d.closeMu.Unlock()
	d.stopSessionMonitor()
	if d.httpListener != nil {
		_ = d.httpListener.Close()
	}
	if d.listener != nil {
		_ = d.listener.Close()
	}
	d.sessionsMu.RLock()
	ids := make([]string, 0, len(d.sessions))
	for id := range d.sessions {
		ids = append(ids, id)
	}
	d.sessionsMu.RUnlock()
	for _, id := range ids {
		d.CloseSession(id, "daemon shutdown")
	}
	d.waitForWorkersExit()
	d.cancel()
	return nil
}

type NewSessionOpts struct {
	SessionID    string
	BotID        string
	CliType      string
	CliPath      string
	WorkingDir   string
	CodexProfile string
	OnReady      func(meta *SessionMeta)
}

type newSessionPayload struct {
	BotID        string `json:"bot_id,omitempty"`
	CodexProfile string `json:"codex_profile,omitempty"`
}

func (d *Daemon) NewSession(opts NewSessionOpts) (*SessionMeta, error) {
	if d.isClosed() {
		return nil, errors.New("daemon closed")
	}
	if opts.SessionID == "" {
		return nil, errors.New("session_id required")
	}
	bot, ok := d.cfg.FindBot(opts.BotID)
	if !ok {
		if len(d.cfg.Bots) == 0 {
			return nil, fmt.Errorf("no bots configured")
		}
		bot = &d.cfg.Bots[0]
	}
	cliType := opts.CliType
	if cliType == "" {
		cliType = string(bot.CliType)
	}
	cliPath := opts.CliPath
	if cliPath == "" {
		cliPath = bot.CliPath
	}
	workingDir := opts.WorkingDir
	if workingDir == "" {
		workingDir = bot.WorkingDir
	}
	profile := opts.CodexProfile
	if profile == "" && cliType == string(config.CliCodex) {
		profile = bot.CodexProfile
	}
	if cliType == string(config.CliCodex) {
		var err error
		profile, err = config.ValidateCodexProfile(profile)
		if err != nil {
			return nil, fmt.Errorf("invalid Codex profile: %w", err)
		}
	}

	now := time.Now()
	meta := &SessionMeta{
		SessionID:      opts.SessionID,
		BotID:          bot.BotID,
		CliType:        cliType,
		CliPath:        cliPath,
		Model:          bot.Model,
		CodexProfile:   profile,
		WorkingDir:     workingDir,
		LastOutput:     []string{},
		CreatedAt:      now,
		Status:         StatusCreated,
		lastActive:     now,
		outputNotifyCh: make(chan struct{}),
	}

	unlockFile := d.lockSessionFile(opts.SessionID)
	d.sessionsMu.Lock()
	if _, exists := d.sessions[opts.SessionID]; exists {
		d.sessionsMu.Unlock()
		unlockFile()
		return nil, fmt.Errorf("session %s already exists", opts.SessionID)
	}
	d.sessions[opts.SessionID] = meta
	d.sessionsMu.Unlock()

	if err := d.store.save(meta.ToPersisted()); err != nil {
		log.Printf("[daemon] warn: persist session %s failed: %v", safeShort(opts.SessionID), err)
	}
	unlockFile()

	if err := d.spawnWorkerForSession(meta); err != nil {
		return nil, err
	}

	if opts.OnReady != nil {
		d.workersMu.RLock()
		h := d.workers[meta.SessionID]
		d.workersMu.RUnlock()
		if h != nil {
			go func() {
				select {
				case <-h.Ready:
					opts.OnReady(meta)
				case <-time.After(10 * time.Second):
					log.Printf("[daemon] session %s: worker ready timeout", safeShort(opts.SessionID))
				case <-d.ctx.Done():
				}
			}()
		}
	}
	return meta, nil
}

func (d *Daemon) spawnWorkerForSession(meta *SessionMeta) error {
	handle := NewWorkerHandle(meta.SessionID)
	instanceID, err := newWorkerInstanceID()
	if err != nil {
		return fmt.Errorf("generate worker instance ID: %w", err)
	}
	if instanceID == "" {
		return errors.New("generate worker instance ID: empty value")
	}
	handle.InstanceID = instanceID

	// Worker lifetime is controlled by its IPC close handshake, not the daemon
	// service context. Stop waits for that handshake before canceling d.ctx.
	cmd := exec.Command(d.selfExe)
	cmd.Env = append(os.Environ(),
		"BOTMUX_ROLE=worker",
		"BOTMUX_SESSION_ID="+meta.SessionID,
		"BOTMUX_DAEMON_ADDR="+d.cfg.ListenAddr,
		"BOTMUX_CLI_TYPE="+meta.CliType,
		"BOTMUX_CLI_PATH="+meta.CliPath,
		"BOTMUX_MODEL="+meta.Model,
		"BOTMUX_CODEX_PROFILE="+meta.CodexProfile,
		"BOTMUX_RESUME_SESSION_ID="+meta.CliSessionID,
		"BOTMUX_WORKING_DIR="+meta.WorkingDir,
		"BOTMUX_STORE_DIR="+d.cfg.SessionsDir,
		"BOTMUX_WORKER_INSTANCE_ID="+handle.InstanceID,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	handle.Cmd = cmd

	// Keep the identity check and worker replacement in one workersMu ->
	// sessionsMu critical section. A session ID may have been purged and
	// recreated while an earlier spawn was waiting to run.
	d.workersMu.Lock()
	d.sessionsMu.Lock()
	if !d.isCurrentSessionLocked(meta) {
		d.sessionsMu.Unlock()
		d.workersMu.Unlock()
		return fmt.Errorf("session %s is no longer current", meta.SessionID)
	}
	meta.Status = StatusSpawning
	delete(d.pendingRestart, meta)
	existing := d.workers[meta.SessionID]
	d.installWorkerLocked(meta, handle)
	if err := cmd.Start(); err != nil {
		d.deleteWorkerIfCurrentLocked(meta.SessionID, handle)
		meta.Status = StatusRecovering
		d.sessionsMu.Unlock()
		d.workersMu.Unlock()
		d.stopWorkerHandle(existing)
		return fmt.Errorf("spawn worker: %w", err)
	}
	handle.Pid = cmd.Process.Pid
	d.sessionsMu.Unlock()
	d.workersMu.Unlock()

	d.stopWorkerHandle(existing)

	if err := d.persistCurrentSession(meta, func() error {
		return d.store.UpdateWorkerPID(meta.SessionID, handle.Pid)
	}); err != nil {
		log.Printf("[daemon] session %s persist worker PID: %v", safeShort(meta.SessionID), err)
	}

	go d.waitWorkerExit(handle)
	go d.monitorReady(handle, meta)

	log.Printf("[daemon] session %s spawned worker (cli=%s, pid=%d)", safeShort(meta.SessionID), meta.CliType, handle.Pid)
	return nil
}

func (d *Daemon) monitorReady(handle *WorkerHandle, meta *SessionMeta) {
	select {
	case <-handle.Ready:
		return
	case <-time.After(10 * time.Second):
		if !d.isCurrentWorker(handle) {
			return
		}
		d.sessionsMu.Lock()
		failed := false
		if !meta.Closed && meta.Status != StatusReady {
			meta.Status = StatusRecovering
			failed = true
		}
		d.sessionsMu.Unlock()
		if failed {
			failCount := 0
			if handle.markStartupFailure() {
				failCount = d.recordSpawnFailure(meta.SessionID)
			}
			log.Printf("[daemon] session %s: worker ready timeout, status=RECOVERING, fail_count=%d",
				safeShort(meta.SessionID), failCount)
		}
	case <-d.ctx.Done():
	}
}

func (d *Daemon) restoreSessions() {
	stored, err := d.store.list()
	if err != nil || len(stored) == 0 {
		if err != nil {
			log.Printf("[daemon] restore sessions: %v", err)
		}
		return
	}
	log.Printf("[daemon] restoring %d persisted session(s)", len(stored))
	for _, ps := range stored {
		meta := SessionMetaFromPersisted(ps)
		d.sessionsMu.Lock()
		d.sessions[ps.SessionID] = meta
		d.sessionsMu.Unlock()
		log.Printf("[daemon] restored session %s (bot=%s, status=%s, last_active=%s)",
			safeShort(ps.SessionID), ps.BotID, meta.Status, meta.LastActive().Format("15:04:05"))
	}
}

func (d *Daemon) waitWorkerExit(handle *WorkerHandle) {
	defer close(handle.ExitDone)
	err := handle.Cmd.Wait()
	exitMsg := fmt.Sprintf("worker exit (pid=%d):", handle.Pid)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("[daemon] session %s %s err=%v", safeShort(handle.SessionID), exitMsg, err)
		}
	}

	// Resolve the worker's original owner while deciding whether this exit can
	// affect session state. A reused ID can have a different SessionMeta while
	// the old worker remains in the current worker slot.
	d.workersMu.Lock()
	d.sessionsMu.Lock()
	owner := d.workerOwnerLocked(handle)
	h := d.workers[handle.SessionID]
	isOwnedCurrent := h == handle && owner != nil && d.isCurrentSessionLocked(owner)
	if isOwnedCurrent {
		d.deleteWorkerIfCurrentLocked(handle.SessionID, handle)
	} else if d.workerOwners != nil && d.workerOwners[handle] == owner {
		delete(d.workerOwners, handle)
	}
	shouldRecover := isOwnedCurrent && !owner.Closed
	if shouldRecover {
		owner.Status = StatusRecovering
	}
	d.sessionsMu.Unlock()
	d.workersMu.Unlock()

	if shouldRecover && handle.markStartupFailure() {
		failCount := d.recordSpawnFailure(handle.SessionID)
		log.Printf("[daemon] session %s: worker exited before READY (fail_count=%d)",
			safeShort(handle.SessionID), failCount)
	}

	handle.CloseConn()

	if isOwnedCurrent {
		for _, fn := range owner.DrainOnClose() {
			func() {
				defer func() { _ = recover() }()
				fn()
			}()
		}
	}
}

func (d *Daemon) waitForWorkersExit() {
	d.workersMu.RLock()
	handles := make([]*WorkerHandle, 0, len(d.workers))
	for _, handle := range d.workers {
		handles = append(handles, handle)
	}
	d.workersMu.RUnlock()

	const shutdownGracePeriod = 3 * time.Second
	deadline := time.NewTimer(shutdownGracePeriod)
	defer deadline.Stop()
	for _, handle := range handles {
		select {
		case <-handle.ExitDone:
		case <-deadline.C:
			if handle.Cmd != nil && handle.Cmd.Process != nil {
				_ = handle.Cmd.Process.Kill()
			}
			<-handle.ExitDone
		}
	}
}

func (d *Daemon) removeSession(id string) {
	d.sessionsMu.RLock()
	meta := d.sessions[id]
	d.sessionsMu.RUnlock()
	d.removeSessionMeta(meta)
}

func (d *Daemon) removeSessionMeta(meta *SessionMeta) {
	h, closed := d.markSessionMetaClosed(meta, true)
	if !closed {
		return
	}

	if h != nil {
		h.CloseConn()
	}
	for _, fn := range meta.DrainOnClose() {
		func() {
			defer func() { _ = recover() }()
			fn()
		}()
	}
}

func (d *Daemon) markSessionMetaClosed(meta *SessionMeta, removeWorker bool) (*WorkerHandle, bool) {
	if meta == nil {
		return nil, false
	}

	unlockFile := d.lockSessionFile(meta.SessionID)
	d.workersMu.Lock()
	d.sessionsMu.Lock()
	if !d.isCurrentSessionLocked(meta) {
		d.sessionsMu.Unlock()
		d.workersMu.Unlock()
		unlockFile()
		return nil, false
	}

	meta.Closed = true
	meta.Status = StatusClosed
	h := d.workers[meta.SessionID]
	if h == nil || !d.workerOwnedByMetaLocked(h, meta) {
		h = nil
	} else if removeWorker {
		d.deleteWorkerIfCurrentLocked(meta.SessionID, h)
	}
	d.sessionsMu.Unlock()
	d.workersMu.Unlock()

	if err := d.store.MarkClosed(meta.SessionID); err != nil {
		log.Printf("[daemon] session %s persist closed state: %v", safeShort(meta.SessionID), err)
	}
	unlockFile()

	return h, true
}

func (d *Daemon) PurgeSession(id string) {
	unlockFile := d.lockSessionFile(id)
	d.workersMu.Lock()
	d.sessionsMu.Lock()
	h, hasHandle := d.workers[id]
	if hasHandle {
		d.deleteWorkerIfCurrentLocked(id, h)
	}
	delete(d.sessions, id)
	d.sessionsMu.Unlock()
	d.workersMu.Unlock()

	_ = d.store.remove(id)
	unlockFile()

	if hasHandle && h != nil {
		h.CloseConn()
		if h.Cmd != nil && h.Cmd.Process != nil {
			_ = h.Cmd.Process.Kill()
			done := make(chan struct{})
			go func() {
				_, _ = h.Cmd.Process.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (d *Daemon) CloseSession(id, reason string) {
	d.sessionsMu.RLock()
	meta, ok := d.sessions[id]
	d.sessionsMu.RUnlock()
	if !ok {
		return
	}
	d.closeSessionMeta(meta, reason)
}

func (d *Daemon) closeSessionMeta(meta *SessionMeta, reason string) {
	h, closed := d.markSessionMetaClosed(meta, false)
	if !closed {
		return
	}

	if h != nil {
		msg := protocol.NewMessage(protocol.MsgClose, meta.SessionID, reason)
		_ = h.Send(msg)
		time.AfterFunc(2*time.Second, func() {
			if h.Cmd != nil && h.Cmd.Process != nil {
				_ = h.Cmd.Process.Kill()
			}
		})
	}
	log.Printf("[daemon] session %s closed (reason=%s)", safeShort(meta.SessionID), reason)
}

func (d *Daemon) SendInput(id, input string) error {
	d.sessionsMu.RLock()
	meta, ok := d.sessions[id]
	d.sessionsMu.RUnlock()
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	if meta.Closed {
		return fmt.Errorf("session %s is closed", id)
	}

	d.workersMu.RLock()
	h := d.workers[id]
	d.workersMu.RUnlock()
	if h == nil {
		return fmt.Errorf("session %s has no worker (status=%s)", id, meta.Status)
	}

	select {
	case <-h.Ready:
	case <-time.After(15 * time.Second):
		return fmt.Errorf("session %s worker not connected (status=%s)", id, meta.Status)
	}

	if meta.CliType == string(config.CliCodex) {
		if err := d.beginCodexTurnIfReady(meta); err != nil {
			return err
		}
	}
	msg := protocol.NewMessage(protocol.MsgUserInput, id, input)
	meta.touchActive()
	userLine := "[user] " + input
	meta.AddOutput(userLine)
	if err := d.persistCurrentSession(meta, func() error {
		return d.store.UpdateOutput(meta.SessionID, userLine)
	}); err != nil {
		log.Printf("[daemon] session %s persist input: %v", safeShort(meta.SessionID), err)
	}
	fmt.Printf("[session=%s] %s\n", safeShort(id), userLine)
	if err := h.Send(msg); err != nil {
		meta.FinishTurn()
		return err
	}
	return nil
}

func (d *Daemon) CancelTurn(id string) error {
	d.sessionsMu.RLock()
	meta, ok := d.sessions[id]
	if !ok {
		d.sessionsMu.RUnlock()
		return fmt.Errorf("session %s not found", id)
	}
	if meta.Closed {
		d.sessionsMu.RUnlock()
		return fmt.Errorf("session %s is closed", id)
	}
	if meta.CliType != string(config.CliCodex) {
		d.sessionsMu.RUnlock()
		return fmt.Errorf("session %s does not use Codex", id)
	}
	d.sessionsMu.RUnlock()
	if !d.hasReadyCurrentWorkerForSession(meta) {
		return fmt.Errorf("session %s has no ready worker", id)
	}

	d.sessionsMu.RLock()
	token, ok := d.beginTurnCancelIfCurrentLocked(meta)
	d.sessionsMu.RUnlock()
	if !ok {
		return fmt.Errorf("session %s has no cancellable active turn", id)
	}
	go d.watchCancelledTurn(meta, token)
	go func() {
		if err := d.sendCancelToCurrentWorker(meta, token); err != nil {
			d.finishCancelWithFailure(meta, token, "codex_cancel_failed", err.Error())
			log.Printf("[daemon] session %s cancel delivery: %v", safeShort(meta.SessionID), err)
		}
	}()
	return nil
}

func (d *Daemon) watchCancelledTurn(meta *SessionMeta, token uint64) {
	timer := time.NewTimer(d.cancelTurnTimeout())
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-d.ctx.Done():
		return
	}

	// Resolve and fence the worker generation while committing the terminal
	// recovery state, before any restart IPC can block.
	d.workersMu.RLock()
	handle := d.workers[meta.SessionID]
	d.sessionsMu.Lock()
	restart := false
	if d.isCurrentSessionLocked(meta) && !meta.Closed {
		restart = meta.FailCancellingTurn(token, protocol.TurnTerminal{
			Status:      protocol.TurnFailed,
			ErrorCode:   "codex_cancel_timeout",
			ErrorDetail: "Codex did not report turn_aborted after cancellation",
		})
	}
	if restart {
		meta.Status = StatusRecovering
		if handle != nil {
			if d.pendingRestart == nil {
				d.pendingRestart = make(map[*SessionMeta]*WorkerHandle)
			}
			d.pendingRestart[meta] = handle
		}
	}
	d.sessionsMu.Unlock()
	d.workersMu.RUnlock()
	if restart {
		d.restartWorkerForSession(meta)
	}
}

func (d *Daemon) finishCancelWithFailure(meta *SessionMeta, token uint64, errorCode, errorDetail string) bool {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	if !d.isCurrentSessionLocked(meta) {
		return false
	}
	return meta.FailCancellingTurn(token, protocol.TurnTerminal{
		Status:      protocol.TurnFailed,
		ErrorCode:   errorCode,
		ErrorDetail: errorDetail,
	})
}

func (d *Daemon) sendCancelToCurrentWorker(meta *SessionMeta, token uint64) error {
	var lastErr error
	for attempt := 0; attempt < maxCurrentWorkerSendTries; attempt++ {
		snapshot := meta.TurnSnapshot()
		if !snapshot.Active || !snapshot.Cancelling || snapshot.Token != token {
			return fmt.Errorf("session %s cancellation is no longer active", meta.SessionID)
		}

		handle, current := d.currentWorkerForSession(meta)
		if !current {
			return fmt.Errorf("session %s is no longer current", meta.SessionID)
		}
		var err error
		if handle == nil || !handle.IsReady() {
			err = fmt.Errorf("session %s has no ready worker", meta.SessionID)
		} else {
			err = handle.Send(protocol.NewMessage(protocol.MsgCancelTurn, meta.SessionID, ""))
		}
		if err != nil {
			switch d.commitCancelSendFailureIfCurrent(meta, token, handle, err) {
			case cancelSendFailureCommitted:
				return err
			case cancelSendFailureRetry:
				lastErr = err
				continue
			case cancelSendFailureStaleSession:
				return fmt.Errorf("session %s is no longer current", meta.SessionID)
			case cancelSendFailureNoLongerCancelling:
				return fmt.Errorf("session %s cancellation is no longer active", meta.SessionID)
			}
		}

		if d.isCurrentWorkerForSession(meta, handle) {
			return nil
		}
		if !d.isCurrentSession(meta) {
			return fmt.Errorf("session %s is no longer current", meta.SessionID)
		}
		lastErr = fmt.Errorf("worker generation changed while sending %s", protocol.MsgCancelTurn)
	}
	return fmt.Errorf("session %s worker changed while sending %s after %d attempts: %w",
		meta.SessionID, protocol.MsgCancelTurn, maxCurrentWorkerSendTries, lastErr)
}

func (d *Daemon) commitCancelSendFailureIfCurrent(
	meta *SessionMeta,
	token uint64,
	handle *WorkerHandle,
	err error,
) cancelSendFailureResult {
	d.workersMu.Lock()
	defer d.workersMu.Unlock()
	if d.workers[meta.SessionID] != handle {
		return cancelSendFailureRetry
	}

	d.sessionsMu.RLock()
	if !d.isCurrentSessionLocked(meta) {
		d.sessionsMu.RUnlock()
		return cancelSendFailureStaleSession
	}
	committed := meta.FailCancellingTurn(token, protocol.TurnTerminal{
		Status:      protocol.TurnFailed,
		ErrorCode:   "codex_cancel_failed",
		ErrorDetail: err.Error(),
	})
	d.sessionsMu.RUnlock()
	if !committed {
		return cancelSendFailureNoLongerCancelling
	}
	return cancelSendFailureCommitted
}

func (d *Daemon) isCurrentSession(meta *SessionMeta) bool {
	if meta == nil {
		return false
	}
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	return d.isCurrentSessionLocked(meta)
}

// persistCurrentSession keeps the SessionMeta identity stable until its
// ID-keyed store operation completes. Stale or terminal metas are no-ops.
func (d *Daemon) persistCurrentSession(meta *SessionMeta, fn func() error) error {
	if meta == nil || fn == nil {
		return nil
	}
	unlockFile := d.lockSessionFile(meta.SessionID)
	defer unlockFile()

	d.sessionsMu.RLock()
	// A zero-value Daemon has no session registry and is only used by direct
	// store tests. Running daemons initialize sessions in New.
	current := d.sessions == nil || (d.sessions[meta.SessionID] == meta && !meta.Closed)
	d.sessionsMu.RUnlock()
	if !current {
		return nil
	}
	return fn()
}

func (d *Daemon) isCurrentSessionLocked(meta *SessionMeta) bool {
	return meta != nil && d.sessions[meta.SessionID] == meta
}

// workerOwnedByMetaLocked treats a nil workerOwners map as a legacy test
// fixture. Production daemons initialize it in New and record every install.
func (d *Daemon) workerOwnedByMetaLocked(handle *WorkerHandle, meta *SessionMeta) bool {
	if handle == nil || meta == nil {
		return false
	}
	if d.workerOwners == nil {
		return true
	}
	return d.workerOwners[handle] == meta
}

func (d *Daemon) workerOwnerLocked(handle *WorkerHandle) *SessionMeta {
	if handle == nil {
		return nil
	}
	if d.workerOwners != nil {
		return d.workerOwners[handle]
	}
	// Older literal Daemon fixtures do not initialize workerOwners. Their
	// worker map remains safe to use only while the handle is still current.
	if d.workers[handle.SessionID] != handle {
		return nil
	}
	return d.sessions[handle.SessionID]
}

func (d *Daemon) installWorkerLocked(meta *SessionMeta, handle *WorkerHandle) {
	if d.workers == nil {
		d.workers = make(map[string]*WorkerHandle)
	}
	if d.workerOwners == nil {
		d.workerOwners = make(map[*WorkerHandle]*SessionMeta)
	}
	d.workers[meta.SessionID] = handle
	d.workerOwners[handle] = meta
}

func (d *Daemon) deleteWorkerIfCurrentLocked(sessionID string, handle *WorkerHandle) bool {
	if handle == nil || d.workers[sessionID] != handle {
		return false
	}
	delete(d.workers, sessionID)
	if d.workerOwners != nil {
		delete(d.workerOwners, handle)
	}
	return true
}

func (d *Daemon) stopWorkerHandle(handle *WorkerHandle) {
	if handle == nil {
		return
	}
	if handle.Cmd != nil && handle.Cmd.Process != nil {
		_ = handle.Cmd.Process.Kill()
	}
	handle.CloseConn()
}

func (d *Daemon) isCurrentSessionWorker(meta *SessionMeta, handle *WorkerHandle) bool {
	if meta == nil || handle == nil {
		return false
	}
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	if d.workers[meta.SessionID] != handle || !d.workerOwnedByMetaLocked(handle, meta) {
		return false
	}
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	return d.isCurrentSessionLocked(meta)
}

func (d *Daemon) hasReadyCurrentWorkerForSession(meta *SessionMeta) bool {
	if meta == nil {
		return false
	}
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	handle := d.workers[meta.SessionID]
	if handle == nil || !handle.IsReady() {
		return false
	}
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	return d.isCurrentSessionLocked(meta)
}

func (d *Daemon) beginCodexTurnIfReady(meta *SessionMeta) error {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	if !d.isCurrentSessionLocked(meta) {
		return fmt.Errorf("session %s not found", meta.SessionID)
	}
	if meta.Closed {
		return fmt.Errorf("session %s is closed", meta.SessionID)
	}
	if meta.Status != StatusReady {
		return fmt.Errorf("session %s is not ready (status=%s)", meta.SessionID, meta.Status)
	}
	if !meta.BeginTurn() {
		return fmt.Errorf("session %s already has an active turn", meta.SessionID)
	}
	return nil
}

func (d *Daemon) beginTurnCancelIfCurrent(meta *SessionMeta) (uint64, bool) {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	return d.beginTurnCancelIfCurrentLocked(meta)
}

func (d *Daemon) beginTurnCancelIfCurrentLocked(meta *SessionMeta) (uint64, bool) {
	if !d.isCurrentSessionLocked(meta) || meta.Closed || meta.CliType != string(config.CliCodex) {
		return 0, false
	}
	return meta.BeginTurnCancel()
}

// Worker map checks precede session map checks everywhere both locks are held.
// No code acquires workersMu while holding sessionsMu, so this order cannot
// deadlock with the session lifecycle paths.
func (d *Daemon) currentWorkerForSession(meta *SessionMeta) (*WorkerHandle, bool) {
	if meta == nil {
		return nil, false
	}
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	if !d.isCurrentSessionLocked(meta) {
		return nil, false
	}
	return d.workers[meta.SessionID], true
}

func (d *Daemon) isCurrentWorkerForSession(meta *SessionMeta, handle *WorkerHandle) bool {
	if meta == nil {
		return false
	}
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	if d.workers[meta.SessionID] != handle {
		return false
	}
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	return d.isCurrentSessionLocked(meta)
}

func (d *Daemon) sendToCurrentWorker(
	meta *SessionMeta,
	msgType protocol.MessageType,
	payload string,
	stillRelevant func() bool,
) (*WorkerHandle, error) {
	var lastErr error
	for attempt := 0; attempt < maxCurrentWorkerSendTries; attempt++ {
		if !d.isCurrentSession(meta) {
			return nil, fmt.Errorf("session %s is no longer current", meta.SessionID)
		}
		if stillRelevant != nil && !stillRelevant() {
			return nil, fmt.Errorf("session %s operation is no longer active", meta.SessionID)
		}

		handle, current := d.currentWorkerForSession(meta)
		if !current {
			return nil, fmt.Errorf("session %s is no longer current", meta.SessionID)
		}
		if handle == nil || !handle.IsReady() {
			return nil, fmt.Errorf("session %s has no ready worker", meta.SessionID)
		}

		err := handle.Send(protocol.NewMessage(msgType, meta.SessionID, payload))
		if d.isCurrentWorkerForSession(meta, handle) {
			if err != nil {
				return nil, err
			}
			return handle, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("worker generation changed while sending %s", msgType)
		}
	}
	return nil, fmt.Errorf("session %s worker changed while sending %s after %d attempts: %w",
		meta.SessionID, msgType, maxCurrentWorkerSendTries, lastErr)
}

func (d *Daemon) restartWorkerForSession(meta *SessionMeta) {
	var lastErr error
	for attempt := 0; attempt < maxCurrentWorkerSendTries; attempt++ {
		handle, current := d.markCurrentWorkerRestartPending(meta)
		if !current {
			return
		}
		if handle == nil || !handle.IsReady() {
			log.Printf("[daemon] session %s restart worker: no ready worker", safeShort(meta.SessionID))
			return
		}

		err := handle.Send(protocol.NewMessage(protocol.MsgRestartWorker, meta.SessionID, ""))
		if d.isCurrentWorkerForSession(meta, handle) {
			if err != nil {
				log.Printf("[daemon] session %s restart worker: %v", safeShort(meta.SessionID), err)
				return
			}
			d.restartWorkerAfterGrace(meta, handle)
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("worker generation changed while sending %s", protocol.MsgRestartWorker)
		}
	}
	log.Printf("[daemon] session %s restart worker: generation changed after %d attempts: %v",
		safeShort(meta.SessionID), maxCurrentWorkerSendTries, lastErr)
}

// markCurrentWorkerRestartPending binds the restart fence to the worker
// generation selected for the next restart IPC. The worker lock is released
// before the caller writes to its socket.
func (d *Daemon) markCurrentWorkerRestartPending(meta *SessionMeta) (*WorkerHandle, bool) {
	if meta == nil {
		return nil, false
	}
	d.workersMu.RLock()
	handle := d.workers[meta.SessionID]
	d.sessionsMu.Lock()
	if !d.isCurrentSessionLocked(meta) || meta.Closed {
		d.sessionsMu.Unlock()
		d.workersMu.RUnlock()
		return nil, false
	}
	meta.Status = StatusRecovering
	if handle != nil {
		if d.pendingRestart == nil {
			d.pendingRestart = make(map[*SessionMeta]*WorkerHandle)
		}
		d.pendingRestart[meta] = handle
	}
	d.sessionsMu.Unlock()
	d.workersMu.RUnlock()
	return handle, true
}

func (d *Daemon) restartWorkerAfterGrace(meta *SessionMeta, handle *WorkerHandle) {
	go func() {
		timer := time.NewTimer(d.workerRestartGrace())
		defer timer.Stop()
		select {
		case <-handle.ExitDone:
			return
		case <-d.ctx.Done():
			return
		case <-timer.C:
			if !d.isCurrentWorkerForSession(meta, handle) {
				return
			}
			if handle.Cmd != nil && handle.Cmd.Process != nil {
				if err := handle.Cmd.Process.Kill(); err != nil {
					if !errors.Is(err, os.ErrProcessDone) {
						log.Printf("[daemon] session %s kill restarting worker: %v", safeShort(handle.SessionID), err)
					}
				}
			}
		}
	}()
}

func (d *Daemon) cancelTurnTimeout() time.Duration {
	if d.turnCancelTimeout != 0 {
		return d.turnCancelTimeout
	}
	return defaultCancelTurnTimeout
}

func (d *Daemon) workerRestartGrace() time.Duration {
	if d.restartWorkerGrace != 0 {
		return d.restartWorkerGrace
	}
	return defaultRestartWorkerGrace
}

func (d *Daemon) Sessions() map[string]*SessionMeta {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	out := make(map[string]*SessionMeta, len(d.sessions))
	for k, v := range d.sessions {
		out[k] = v
	}
	return out
}

func (d *Daemon) GetSessionMeta(id string) (*SessionMeta, bool) {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	m, ok := d.sessions[id]
	return m, ok
}

func (d *Daemon) GetWorkerHandle(id string) (*WorkerHandle, bool) {
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	h, ok := d.workers[id]
	return h, ok
}

func (d *Daemon) isClosed() bool {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	return d.closed
}

func (d *Daemon) acceptLoop() {
	defer d.wg.Done()
	defer d.Stop()
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			if d.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("[daemon] accept: %v", err)
			continue
		}
		d.wg.Add(1)
		go d.handleConn(conn)
	}
}

func (d *Daemon) handleConn(conn net.Conn) {
	defer d.wg.Done()
	defer conn.Close()

	reader := protocol.NewMessageReader(conn)
	first, err := reader.Read()
	if err != nil {
		log.Printf("[daemon] conn read first: %v", err)
		return
	}

	if first.Type != protocol.MsgReady && isClientFirstMessage(first) {
		d.handleClientConn(conn, reader, first)
		return
	}

	sessionID := first.SessionID
	if first.Type != protocol.MsgReady || first.WorkerInstanceID == "" {
		log.Printf("[daemon] session %s rejected worker without ready instance identity", safeShort(sessionID))
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "worker must send ready with instance ID").WriteTo(conn)
		return
	}

	d.workersMu.Lock()
	d.sessionsMu.RLock()
	meta, sessionExists := d.sessions[sessionID]
	h := d.workers[sessionID]
	isExpected := sessionExists &&
		!meta.Closed &&
		d.isCurrentSessionLocked(meta) &&
		h != nil &&
		d.workerOwners != nil &&
		d.workerOwners[h] == meta
	if !isExpected {
		d.sessionsMu.RUnlock()
		d.workersMu.Unlock()
		log.Printf("[daemon] session %s rejected unexpected worker", safeShort(sessionID))
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "worker not expected").WriteTo(conn)
		return
	}
	if first.WorkerInstanceID != h.InstanceID {
		d.sessionsMu.RUnlock()
		d.workersMu.Unlock()
		log.Printf("[daemon] session %s rejected worker with mismatched instance ID", safeShort(sessionID))
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "worker instance ID mismatch").WriteTo(conn)
		return
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, sessionID, workerReadyAckPayload).WriteTo(conn); err != nil {
		d.sessionsMu.RUnlock()
		d.workersMu.Unlock()
		log.Printf("[daemon] session %s ready acknowledgment: %v", safeShort(sessionID), err)
		return
	}
	oldConn := h.SetConn(conn)
	d.sessionsMu.RUnlock()
	d.workersMu.Unlock()
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}

	d.connMapMu.Lock()
	d.connToSess[conn] = sessionID
	d.connMapMu.Unlock()
	defer func() {
		d.connMapMu.Lock()
		delete(d.connToSess, conn)
		d.connMapMu.Unlock()
	}()

	d.routeMessage(first, meta, h)
	for {
		msg, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("[daemon] worker session %s read: %v", safeShort(sessionID), err)
			}
			return
		}
		if msg.SessionID == "" {
			msg.SessionID = sessionID
		}
		d.routeMessage(msg, meta, h)
	}
}

func newWorkerInstanceID() (string, error) {
	buf := make([]byte, 16)
	n, err := rand.Read(buf)
	if err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	if n != len(buf) {
		return "", fmt.Errorf("read random bytes: got %d, want %d", n, len(buf))
	}
	return hex.EncodeToString(buf), nil
}

func (d *Daemon) handleClientConn(conn net.Conn, reader *protocol.MessageReader, first *protocol.Message) {
	cliID := fmt.Sprintf("cli-%p", conn)
	defer func() {
		log.Printf("[daemon] client %s disconnected", cliID)
	}()
	switch first.Type {
	case protocol.MsgListSessions:
		d.handleListSessions(conn)
		return
	case protocol.MsgHistory:
		d.handleHistory(conn, first.SessionID)
		return
	case protocol.MsgCloseSession:
		d.handleCloseSession(conn, first.SessionID, first.Payload)
		return
	case protocol.MsgNewSession:
	}
	if first.Type == protocol.MsgNewSession {
		sid := first.SessionID
		if sid == "" {
			sid = fmt.Sprintf("s-%d", time.Now().UnixNano())
		}
		payload := newSessionPayload{}
		if err := json.Unmarshal([]byte(first.Payload), &payload); err != nil {
			payload.BotID = first.Payload
		}
		botID := payload.BotID
		log.Printf("[daemon] %s creating session %s (bot=%s)", cliID, safeShort(sid), botID)
		var readyCh <-chan struct{} = nil
		_, err := d.NewSession(NewSessionOpts{
			SessionID:    sid,
			BotID:        botID,
			CodexProfile: payload.CodexProfile,
		})
		if err == nil {
			d.workersMu.RLock()
			h := d.workers[sid]
			d.workersMu.RUnlock()
			if h != nil {
				readyCh = h.Ready
			}
		}
		ackPayload := "ok"
		if err != nil {
			ackPayload = "err:" + err.Error()
		}
		_, _ = protocol.NewMessage(protocol.MsgAck, sid, ackPayload).WriteTo(conn)
		if err != nil {
			log.Printf("[daemon] %s new session failed: %v", cliID, err)
			return
		}
		select {
		case <-readyCh:
			_, _ = protocol.NewMessage(protocol.MsgReady, sid, "").WriteTo(conn)
		case <-time.After(15 * time.Second):
			log.Printf("[daemon] %s session %s ready timeout", cliID, safeShort(sid))
			return
		case <-d.ctx.Done():
			return
		}
	} else if first != nil {
		sid := first.SessionID
		if sid != "" {
			d.forwardClientMessage(conn, first)
		}
	}
	for {
		msg, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("[daemon] client %s read: %v", cliID, err)
			}
			return
		}
		d.forwardClientMessage(conn, msg)
	}
}

type sessionListEntry struct {
	SessionID  string   `json:"session_id"`
	BotID      string   `json:"bot_id"`
	CliType    string   `json:"cli_type"`
	Status     string   `json:"status"`
	Pid        int      `json:"pid"`
	LastActive string   `json:"last_active"`
	Outputs    []string `json:"outputs"`
}

func (d *Daemon) handleListSessions(conn net.Conn) {
	d.sessionsMu.RLock()
	metas := make([]*SessionMeta, 0, len(d.sessions))
	for _, m := range d.sessions {
		metas = append(metas, m)
	}
	d.sessionsMu.RUnlock()
	entries := make([]sessionListEntry, 0, len(metas))
	for _, m := range metas {
		pid := 0
		d.workersMu.RLock()
		if h, ok := d.workers[m.SessionID]; ok {
			pid = h.Pid
		}
		d.workersMu.RUnlock()
		entries = append(entries, sessionListEntry{
			SessionID:  m.SessionID,
			BotID:      m.BotID,
			CliType:    m.CliType,
			Status:     string(m.Status),
			Pid:        pid,
			LastActive: m.LastActive().Format("2006-01-02 15:04:05"),
			Outputs:    m.SnapshotOutput(),
		})
	}
	data, err := json.Marshal(entries)
	if err != nil {
		_, _ = protocol.NewMessage(protocol.MsgError, "", "marshal sessions: "+err.Error()).WriteTo(conn)
		return
	}
	_, _ = protocol.NewMessage(protocol.MsgListSessionsRsp, "", string(data)).WriteTo(conn)
}

func (d *Daemon) handleHistory(conn net.Conn, sessionID string) {
	if sessionID == "" {
		_, _ = protocol.NewMessage(protocol.MsgError, "", "missing session_id").WriteTo(conn)
		return
	}
	d.sessionsMu.RLock()
	m, ok := d.sessions[sessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "session not found").WriteTo(conn)
		return
	}
	outs := m.SnapshotOutput()
	data, err := json.Marshal(outs)
	if err != nil {
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "marshal: "+err.Error()).WriteTo(conn)
		return
	}
	_, _ = protocol.NewMessage(protocol.MsgHistoryRsp, sessionID, string(data)).WriteTo(conn)
}

func (d *Daemon) handleCloseSession(conn net.Conn, sessionID, reason string) {
	if sessionID == "" {
		_, _ = protocol.NewMessage(protocol.MsgError, "", "missing session_id").WriteTo(conn)
		return
	}
	if reason == "" {
		reason = "client request"
	}
	d.sessionsMu.RLock()
	_, ok := d.sessions[sessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "session not found").WriteTo(conn)
		return
	}
	_, _ = protocol.NewMessage(protocol.MsgCloseSessionAck, sessionID, "ok").WriteTo(conn)
	go d.CloseSession(sessionID, reason)
}

func (d *Daemon) forwardClientMessage(clientConn net.Conn, msg *protocol.Message) {
	switch msg.Type {
	case protocol.MsgClose:
		d.CloseSession(msg.SessionID, "client close")
		return
	case protocol.MsgUserInput, protocol.MsgNewSession:
	default:
	}
	if msg.SessionID == "" {
		_, _ = protocol.NewMessage(protocol.MsgError, "", "missing session_id").WriteTo(clientConn)
		return
	}

	d.sessionsMu.RLock()
	meta, ok := d.sessions[msg.SessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "session not found").WriteTo(clientConn)
		return
	}
	if meta.Closed {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "session is closed").WriteTo(clientConn)
		return
	}

	d.workersMu.RLock()
	h := d.workers[msg.SessionID]
	d.workersMu.RUnlock()
	if h == nil {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "worker not available").WriteTo(clientConn)
		return
	}

	select {
	case <-h.Ready:
	case <-time.After(15 * time.Second):
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "worker not connected").WriteTo(clientConn)
		return
	}

	cursor, outputCh := meta.OutputSubscription()
	terminalCursor, terminalCh := meta.TerminalSubscription()
	if msg.Type == protocol.MsgUserInput {
		if meta.CliType == string(config.CliCodex) {
			if err := d.beginCodexTurnIfReady(meta); err != nil {
				_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, err.Error()).WriteTo(clientConn)
				return
			}
		}
		inCopy := *msg
		if err := h.Send(&inCopy); err != nil {
			meta.FinishTurn()
			_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "forward: "+err.Error()).WriteTo(clientConn)
			return
		}
		meta.touchActive()
	}

	connMu := sync.Mutex{}
	stop := make(chan struct{})
	meta.AddOnClose(func() { closeOnce(stop) })

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 130*time.Second)
		defer cancel()

		go func() {
			buf := make([]byte, 1)
			_, _ = clientConn.Read(buf)
			cancel()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-outputCh:
				outs, next, nextOutputCh := meta.SnapshotOutputSince(cursor)
				cursor = next
				outputCh = nextOutputCh
				for _, out := range outs {
					m := protocol.NewMessage(protocol.MsgOutput, meta.SessionID, out)
					connMu.Lock()
					_, err := m.WriteTo(clientConn)
					connMu.Unlock()
					if err != nil {
						log.Printf("[daemon] forward output write err: %v", err)
						return
					}
				}
			case <-terminalCh:
				terminals, next, nextTerminalCh := meta.SnapshotTerminalsSince(terminalCursor)
				terminalCursor = next
				terminalCh = nextTerminalCh
				for _, terminal := range terminals {
					payload, err := json.Marshal(terminal)
					if err != nil {
						log.Printf("[daemon] encode turn terminal: %v", err)
						return
					}
					connMu.Lock()
					_, err = protocol.NewMessage(protocol.MsgTurnCompleted, meta.SessionID, string(payload)).WriteTo(clientConn)
					connMu.Unlock()
					if err != nil {
						log.Printf("[daemon] forward turn terminal write err: %v", err)
					}
					return
				}
			}
		}
	}()
}

func isClientFirstMessage(m *protocol.Message) bool {
	if m == nil {
		return false
	}
	switch m.Type {
	case protocol.MsgNewSession,
		protocol.MsgUserInput,
		protocol.MsgClose,
		protocol.MsgAck,
		protocol.MsgListSessions,
		protocol.MsgHistory,
		protocol.MsgCloseSession:
		return true
	}
	return m.SessionID == ""
}

func safeShort(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

func closeOnce(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}

func isClosedChan(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (d *Daemon) routeMessage(msg *protocol.Message, meta *SessionMeta, h *WorkerHandle) {
	if h != nil && !d.isCurrentSessionWorker(meta, h) {
		log.Printf("[daemon] session %s ignored message from stale worker", safeShort(meta.SessionID))
		return
	}

	switch msg.Type {
	case protocol.MsgReady:
		d.sessionsMu.Lock()
		if !d.isCurrentSessionLocked(meta) {
			d.sessionsMu.Unlock()
			log.Printf("[daemon] session %s ignored ready from stale session", safeShort(meta.SessionID))
			return
		}
		if d.pendingRestart[meta] == h {
			d.sessionsMu.Unlock()
			log.Printf("[daemon] session %s ignored late ready from restarting worker", safeShort(meta.SessionID))
			return
		}
		if !meta.Closed {
			meta.Status = StatusReady
		}
		d.sessionsMu.Unlock()
		h.markReady()
		d.clearSpawnFailure(meta.SessionID)
		log.Printf("[daemon] session %s -> READY", safeShort(meta.SessionID))
		meta.touchActive()
		if err := d.persistCurrentSession(meta, func() error {
			return d.store.UpdateLastActive(meta.SessionID)
		}); err != nil {
			log.Printf("[daemon] session %s persist ready activity: %v", safeShort(meta.SessionID), err)
		}
	case protocol.MsgHeartbeat:
		h.TouchHb()
	case protocol.MsgOutput:
		meta.AddOutput(msg.Payload)
		meta.touchActive()
		if err := d.persistCurrentSession(meta, func() error {
			return d.store.UpdateOutput(meta.SessionID, msg.Payload)
		}); err != nil {
			log.Printf("[daemon] session %s persist output: %v", safeShort(meta.SessionID), err)
		}
		fmt.Printf("[session=%s] %s\n", safeShort(meta.SessionID), msg.Payload)
	case protocol.MsgError:
		log.Printf("[daemon] session %s error: %s", safeShort(meta.SessionID), msg.Payload)
	case protocol.MsgCliSessionBound:
		if !isCodexSessionID(msg.Payload) {
			log.Printf("[daemon] session %s ignored invalid Codex session ID", safeShort(meta.SessionID))
			return
		}
		meta.SetCliSessionID(msg.Payload)
		meta.touchActive()
		if err := d.persistCurrentSession(meta, func() error {
			return d.store.UpdateCliSessionID(meta.SessionID, msg.Payload)
		}); err != nil {
			log.Printf("[daemon] session %s persist Codex session ID: %v", safeShort(meta.SessionID), err)
		}
	case protocol.MsgTurnCompleted:
		var terminal protocol.TurnTerminal
		if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
			terminal = protocol.TurnTerminal{
				Status:      protocol.TurnFailed,
				ErrorCode:   "invalid_terminal",
				ErrorDetail: err.Error(),
			}
		}
		if !meta.CompleteTurn(terminal) {
			log.Printf("[daemon] session %s ignored late turn terminal", safeShort(meta.SessionID))
		}
	case protocol.MsgClose:
		log.Printf("[daemon] session %s closed by worker", safeShort(meta.SessionID))
		d.removeSessionMeta(meta)
	default:
		log.Printf("[daemon] session %s unknown msg type=%s", safeShort(meta.SessionID), msg.Type)
	}
}

func isCodexSessionID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, b := range value {
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if b != '-' {
				return false
			}
		case (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F'):
		default:
			return false
		}
	}
	return true
}

func (d *Daemon) periodicGC() {
	defer d.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.runGC()
		}
	}
}

func (d *Daemon) runGC() {
	now := time.Now()
	d.sessionsMu.RLock()
	metas := make([]*SessionMeta, 0, len(d.sessions))
	for _, m := range d.sessions {
		metas = append(metas, m)
	}
	d.sessionsMu.RUnlock()
	idleLimit := 15 * time.Minute
	hbLimit := 20 * time.Second
	for _, meta := range metas {
		if meta.Closed {
			continue
		}
		lastActive := meta.LastActive()
		d.workersMu.RLock()
		h, hasH := d.workers[meta.SessionID]
		var lastHb time.Time
		if hasH {
			lastHb = h.LastHb()
		}
		d.workersMu.RUnlock()

		if hasH && now.Sub(lastHb) > hbLimit && h.IsReady() {
			log.Printf("[daemon] session %s heartbeat stale (>%s), closing", safeShort(meta.SessionID), hbLimit)
			d.CloseSession(meta.SessionID, "heartbeat stale")
			continue
		}
		if now.Sub(lastActive) > idleLimit {
			log.Printf("[daemon] session %s idle (>%s), closing", safeShort(meta.SessionID), idleLimit)
			d.CloseSession(meta.SessionID, "idle")
		}
	}
}
