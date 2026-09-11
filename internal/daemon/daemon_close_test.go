package daemon

import (
	"context"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"botmux-go/internal/config"
	"botmux-go/internal/protocol"
)

func TestSpawnWorkerRejectsClosedDaemon(t *testing.T) {
	meta := NewSessionMeta("closed-daemon-spawn", "bot-test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &Daemon{
		cfg:            &config.DaemonConfig{ListenAddr: "127.0.0.1:1", SessionsDir: t.TempDir()},
		selfExe:        "/bin/sh",
		store:          NewSessionStore(t.TempDir()),
		ctx:            ctx,
		cancel:         cancel,
		sessions:       map[string]*SessionMeta{meta.SessionID: meta},
		workers:        make(map[string]*WorkerHandle),
		workerOwners:   make(map[*WorkerHandle]*SessionMeta),
		pendingRestart: make(map[*SessionMeta]*WorkerHandle),
	}
	d.closeMu.Lock()
	d.closed = true
	d.closeMu.Unlock()

	err := d.spawnWorkerForSession(meta)
	if err == nil || !strings.Contains(err.Error(), "daemon closed") {
		t.Fatalf("spawn closed daemon error = %v, want daemon closed", err)
	}

	d.workersMu.RLock()
	handle, exists := d.workers[meta.SessionID]
	d.workersMu.RUnlock()
	if exists || handle != nil {
		t.Fatalf("closed daemon installed worker handle = %#v", handle)
	}
	d.sessionsMu.RLock()
	status := meta.Status
	d.sessionsMu.RUnlock()
	if status != StatusCreated {
		t.Fatalf("closed daemon changed session status to %s, want %s", status, StatusCreated)
	}
}

func TestSessionFileLockRegistryReleasesUnusedEntries(t *testing.T) {
	d := &Daemon{}

	release := d.lockSessionFile("purged")
	release()

	d.sessionFileLocksMu.Lock()
	_, exists := d.sessionFileLocks["purged"]
	d.sessionFileLocksMu.Unlock()
	if exists {
		t.Fatal("released session file lock remained in the registry")
	}
}

func TestSessionFileLockRegistryRetainsWaitingEntries(t *testing.T) {
	const sessionID = "purged"

	d := &Daemon{}
	releaseFirst := d.lockSessionFile(sessionID)

	waiterStarted := make(chan struct{})
	waiterAcquired := make(chan struct{})
	releaseWaiter := make(chan struct{}, 1)
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		close(waiterStarted)
		releaseSecond := d.lockSessionFile(sessionID)
		close(waiterAcquired)
		<-releaseWaiter
		releaseSecond()
	}()
	t.Cleanup(func() {
		select {
		case releaseWaiter <- struct{}{}:
		default:
		}
		<-waiterDone
	})

	<-waiterStarted
	waitForSessionFileLockRefs(t, d, sessionID, 2)

	releaseFirst()
	<-waiterAcquired
	if exists, refs := sessionFileLockRegistryState(d, sessionID); !exists || refs != 1 {
		t.Fatalf("registry after first release = exists:%t refs:%d, want exists:true refs:1", exists, refs)
	}

	releaseWaiter <- struct{}{}
	<-waiterDone
	if exists, refs := sessionFileLockRegistryState(d, sessionID); exists || refs != 0 {
		t.Fatalf("registry after second release = exists:%t refs:%d, want exists:false refs:0", exists, refs)
	}
}

func waitForSessionFileLockRefs(t *testing.T, d *Daemon, sessionID string, want int) {
	t.Helper()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		exists, refs := sessionFileLockRegistryState(d, sessionID)
		if exists && refs == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("registry for %q = exists:%t refs:%d, want exists:true refs:%d", sessionID, exists, refs, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func sessionFileLockRegistryState(d *Daemon, sessionID string) (bool, int) {
	d.sessionFileLocksMu.Lock()
	defer d.sessionFileLocksMu.Unlock()

	entry, exists := d.sessionFileLocks[sessionID]
	if !exists {
		return false, 0
	}
	return true, entry.refs
}

func TestStopWaitsForWorkerExitBeforeCancelingDaemonContext(t *testing.T) {
	meta := NewSessionMeta("daemon-stop-waits-for-worker", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 0.15")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		ctx:           ctx,
		cancel:        cancel,
		sessions:      map[string]*SessionMeta{meta.SessionID: meta},
		workers:       map[string]*WorkerHandle{meta.SessionID: handle},
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}
	go d.waitWorkerExit(handle)

	contextCanceledAfterWorkerExit := make(chan bool, 1)
	go func() {
		<-ctx.Done()
		select {
		case <-handle.ExitDone:
			contextCanceledAfterWorkerExit <- true
		default:
			contextCanceledAfterWorkerExit <- false
		}
	}()

	started := time.Now()
	if err := d.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("Stop returned before the worker exited: %v", elapsed)
	}
	if canceledAfterExit := <-contextCanceledAfterWorkerExit; !canceledAfterExit {
		t.Fatal("daemon context was canceled before the worker exit was reaped")
	}
}

func TestStopPreservesSessionsForRestore(t *testing.T) {
	const nativeSessionID = "01234567-89ab-cdef-0123-456789abcdef"

	sessionsDir := t.TempDir()
	meta := NewSessionMeta("daemon-stop-preserves-for-restore", "bot-codex")
	meta.CliType = string(config.CliCodex)
	meta.CliSessionID = nativeSessionID
	handle := NewWorkerHandle(meta.SessionID)
	daemonConn, workerConn := net.Pipe()
	handle.SetConn(daemonConn)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 0.15")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		cfg:              &config.DaemonConfig{SessionsDir: sessionsDir},
		ctx:              ctx,
		cancel:           cancel,
		store:            NewSessionStore(sessionsDir),
		sessions:         map[string]*SessionMeta{meta.SessionID: meta},
		sessionFileLocks: make(map[string]*sessionFileLockEntry),
		pendingRestart:   make(map[*SessionMeta]*WorkerHandle),
		workers:          map[string]*WorkerHandle{meta.SessionID: handle},
		workerOwners:     map[*WorkerHandle]*SessionMeta{handle: meta},
		connToSess:       make(map[net.Conn]string),
		spawnFailures:    make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	workerExited := make(chan struct{})
	go func() {
		d.waitWorkerExit(handle)
		close(workerExited)
	}()
	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = workerConn.Close()
		if handle.Cmd.Process != nil {
			_ = handle.Cmd.Process.Kill()
		}
		select {
		case <-workerExited:
		case <-time.After(2 * time.Second):
			t.Error("worker fixture was not reaped")
		}
	})

	restartMessage := make(chan *protocol.Message, 1)
	readErr := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(workerConn).Read()
		if err != nil {
			readErr <- err
			return
		}
		restartMessage <- msg
	}()

	if err := d.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-readErr:
		t.Fatalf("read worker cleanup message: %v", err)
	case msg := <-restartMessage:
		if msg.Type != protocol.MsgRestartWorker || msg.SessionID != meta.SessionID {
			t.Fatalf("cleanup message = %#v, want restart_worker for %q", msg, meta.SessionID)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not receive restart_worker cleanup message")
	}

	select {
	case <-handle.ExitDone:
	default:
		t.Fatal("Stop returned before the worker exit was reaped")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Stop did not cancel the daemon context")
	}

	persisted, err := d.store.load(meta.SessionID)
	if err != nil {
		t.Fatalf("load session after Stop: %v", err)
	}
	if persisted.Closed {
		t.Fatal("Stop persisted the session as closed")
	}
	if persisted.CliSessionID != nativeSessionID {
		t.Fatalf("persisted native session ID = %q, want %q", persisted.CliSessionID, nativeSessionID)
	}

	stored, err := d.store.list()
	if err != nil {
		t.Fatalf("list persisted sessions: %v", err)
	}
	if len(stored) != 1 || stored[0].SessionID != meta.SessionID {
		t.Fatalf("store.list() = %#v, want open session %q", stored, meta.SessionID)
	}
}

func TestStopKillsWorkerThatMissesGracePeriod(t *testing.T) {
	meta := NewSessionMeta("daemon-stop-kills-stuck-worker", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		ctx:           ctx,
		cancel:        cancel,
		sessions:      map[string]*SessionMeta{meta.SessionID: meta},
		workers:       map[string]*WorkerHandle{meta.SessionID: handle},
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}
	go d.waitWorkerExit(handle)

	started := time.Now()
	if err := d.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("Stop exceeded grace period: %v", elapsed)
	}
	select {
	case <-handle.ExitDone:
	default:
		t.Fatal("stuck worker was not reaped after shutdown grace period")
	}
}

func TestStopKillsAllWorkersThatMissGracePeriod(t *testing.T) {
	metas := []*SessionMeta{
		NewSessionMeta("daemon-stop-kills-first-stuck-worker", "bot-test"),
		NewSessionMeta("daemon-stop-kills-second-stuck-worker", "bot-test"),
	}
	handles := make([]*WorkerHandle, len(metas))
	for i, meta := range metas {
		handle := NewWorkerHandle(meta.SessionID)
		handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
		if err := handle.Cmd.Start(); err != nil {
			t.Fatalf("start worker fixture %d: %v", i, err)
		}
		handle.Pid = handle.Cmd.Process.Pid
		handles[i] = handle
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		ctx:           ctx,
		cancel:        cancel,
		sessions:      make(map[string]*SessionMeta, len(metas)),
		workers:       make(map[string]*WorkerHandle, len(handles)),
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	for i, meta := range metas {
		handle := handles[i]
		d.sessions[meta.SessionID] = meta
		d.workers[meta.SessionID] = handle
		if err := d.store.save(meta.ToPersisted()); err != nil {
			t.Fatalf("persist session fixture %d: %v", i, err)
		}
		go d.waitWorkerExit(handle)
	}
	t.Cleanup(func() {
		cancel()
		for _, handle := range handles {
			if handle.Cmd != nil && handle.Cmd.Process != nil {
				_ = handle.Cmd.Process.Kill()
			}
		}
		for i, handle := range handles {
			select {
			case <-handle.ExitDone:
			case <-time.After(2 * time.Second):
				t.Errorf("worker fixture %d was not reaped", i)
			}
		}
	})

	stopDone := make(chan error, 1)
	started := time.Now()
	go func() {
		stopDone <- d.Stop()
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Stop did not return after the global shutdown grace period")
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("Stop exceeded grace period: %v", elapsed)
	}
	for i, handle := range handles {
		select {
		case <-handle.ExitDone:
		default:
			t.Fatalf("stuck worker %d was not reaped after shutdown grace period", i)
		}
	}
}

func TestCloseSessionPersistsBeforeWorkerExit(t *testing.T) {
	meta := NewSessionMeta("session-close-persisted-first", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	daemonConn, workerConn := net.Pipe()
	handle.Conn = daemonConn
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	d := &Daemon{
		sessions:      map[string]*SessionMeta{meta.SessionID: meta},
		workers:       map[string]*WorkerHandle{meta.SessionID: handle},
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	workerExited := make(chan struct{})
	go func() {
		d.waitWorkerExit(handle)
		close(workerExited)
	}()
	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = workerConn.Close()
		if handle.Cmd.Process != nil {
			_ = handle.Cmd.Process.Kill()
		}
		select {
		case <-workerExited:
		case <-time.After(2 * time.Second):
			t.Error("worker fixture was not reaped")
		}
	})

	closeMessage := make(chan *protocol.Message, 1)
	closeReadErr := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(workerConn).Read()
		if err != nil {
			closeReadErr <- err
			return
		}
		closeMessage <- msg
	}()

	closed := make(chan struct{})
	go func() {
		d.CloseSession(meta.SessionID, "test")
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("CloseSession blocked waiting for the worker to exit")
	}

	persisted, err := d.store.load(meta.SessionID)
	if err != nil {
		t.Fatalf("load closed session: %v", err)
	}
	if !persisted.Closed {
		t.Fatal("closed session was not persisted before worker exit")
	}

	select {
	case err := <-closeReadErr:
		t.Fatalf("read close message: %v", err)
	case msg := <-closeMessage:
		if msg.Type != protocol.MsgClose || msg.SessionID != meta.SessionID || msg.Payload != "test" {
			t.Fatalf("close message = %#v, want close for %q with reason %q", msg, meta.SessionID, "test")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not receive close message")
	}

	select {
	case <-workerExited:
		t.Fatal("worker exited before the test could verify asynchronous cleanup")
	default:
	}
}

func TestOldWorkerCloseCannotMarkRecreatedSessionClosed(t *testing.T) {
	const sessionID = "old-worker-close-recreated"

	oldMeta := NewSessionMeta(sessionID, "bot-old")
	oldHandle := NewWorkerHandle(sessionID)
	d := &Daemon{
		sessions: map[string]*SessionMeta{
			sessionID: oldMeta,
		},
		workers: map[string]*WorkerHandle{
			sessionID: oldHandle,
		},
		workerOwners: map[*WorkerHandle]*SessionMeta{
			oldHandle: oldMeta,
		},
		store: NewSessionStore(t.TempDir()),
	}
	if err := d.store.save(oldMeta.ToPersisted()); err != nil {
		t.Fatalf("persist old session fixture: %v", err)
	}

	d.CloseSession(sessionID, "test")
	oldPersisted, err := d.store.load(sessionID)
	if err != nil {
		t.Fatalf("load daemon-closed session: %v", err)
	}
	if !oldPersisted.Closed {
		t.Fatal("daemon did not persist the old session as closed")
	}

	d.PurgeSession(sessionID)
	replacementMeta := NewSessionMeta(sessionID, "bot-replacement")
	replacementHandle := NewWorkerHandle(sessionID)
	d.sessions[sessionID] = replacementMeta
	d.workers[sessionID] = replacementHandle
	d.workerOwners[replacementHandle] = replacementMeta
	if err := d.store.save(replacementMeta.ToPersisted()); err != nil {
		t.Fatalf("persist replacement session: %v", err)
	}

	// A delayed old-worker close is routed through daemon identity checks. The
	// worker itself must not write session storage; worker_close_test covers that
	// boundary directly.
	d.routeMessage(protocol.NewMessage(protocol.MsgClose, sessionID, ""), oldMeta, oldHandle)

	persisted, err := d.store.load(sessionID)
	if err != nil {
		t.Fatalf("load replacement session: %v", err)
	}
	if persisted.Closed {
		t.Fatal("old worker close marked the replacement session closed")
	}
	if persisted.BotID != "bot-replacement" {
		t.Fatalf("replacement BotID = %q, want %q", persisted.BotID, "bot-replacement")
	}
}

func TestCloseSessionMetaDoesNotCloseReusedSession(t *testing.T) {
	const sessionID = "session-close-reused"

	oldMeta := NewSessionMeta(sessionID, "bot-old")
	replacementMeta := NewSessionMeta(sessionID, "bot-replacement")
	oldHandle := NewWorkerHandle(sessionID)
	replacementHandle := NewWorkerHandle(sessionID)
	oldDaemonConn, oldWorkerConn := net.Pipe()
	replacementDaemonConn, replacementWorkerConn := net.Pipe()
	oldHandle.Conn = oldDaemonConn
	replacementHandle.Conn = replacementDaemonConn
	t.Cleanup(func() {
		_ = oldDaemonConn.Close()
		_ = oldWorkerConn.Close()
		_ = replacementDaemonConn.Close()
		_ = replacementWorkerConn.Close()
	})

	d := &Daemon{
		sessions: map[string]*SessionMeta{
			sessionID: oldMeta,
		},
		workers: map[string]*WorkerHandle{
			sessionID: oldHandle,
		},
		workerOwners: map[*WorkerHandle]*SessionMeta{
			oldHandle: oldMeta,
		},
		store: NewSessionStore(t.TempDir()),
	}
	if err := d.store.save(oldMeta.ToPersisted()); err != nil {
		t.Fatalf("persist old session fixture: %v", err)
	}

	d.sessions[sessionID] = replacementMeta
	d.workers[sessionID] = replacementHandle
	d.workerOwners[replacementHandle] = replacementMeta
	if err := d.store.save(replacementMeta.ToPersisted()); err != nil {
		t.Fatalf("persist replacement session fixture: %v", err)
	}

	callbackDrained := make(chan struct{}, 1)
	oldMeta.AddOnClose(func() {
		callbackDrained <- struct{}{}
	})

	replacementWorkerConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	replacementMessage := make(chan *protocol.Message, 1)
	replacementReadErr := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(replacementWorkerConn).Read()
		if err != nil {
			replacementReadErr <- err
			return
		}
		replacementMessage <- msg
	}()

	d.closeSessionMeta(oldMeta, "stale close")

	if oldMeta.Closed || oldMeta.Status == StatusClosed {
		t.Fatal("stale close mutated the old session meta")
	}
	if replacementMeta.Closed || replacementMeta.Status == StatusClosed {
		t.Fatal("stale close mutated the replacement session meta")
	}

	d.sessionsMu.RLock()
	currentMeta := d.sessions[sessionID]
	d.sessionsMu.RUnlock()
	if currentMeta != replacementMeta {
		t.Fatalf("current session meta = %p, want replacement %p", currentMeta, replacementMeta)
	}
	d.workersMu.RLock()
	currentHandle := d.workers[sessionID]
	oldOwner := d.workerOwners[oldHandle]
	replacementOwner := d.workerOwners[replacementHandle]
	d.workersMu.RUnlock()
	if currentHandle != replacementHandle {
		t.Fatalf("current worker = %p, want replacement %p", currentHandle, replacementHandle)
	}
	if oldOwner != oldMeta || replacementOwner != replacementMeta {
		t.Fatalf("worker owners changed: old=%p replacement=%p", oldOwner, replacementOwner)
	}

	persisted, err := d.store.load(sessionID)
	if err != nil {
		t.Fatalf("load replacement session: %v", err)
	}
	if persisted.Closed {
		t.Fatal("stale close persisted the replacement session as closed")
	}

	select {
	case msg := <-replacementMessage:
		t.Fatalf("replacement worker received stale close: %#v", msg)
	case readErr := <-replacementReadErr:
		if netErr, ok := readErr.(net.Error); !ok || !netErr.Timeout() {
			t.Fatalf("read replacement worker: %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to confirm replacement worker received no close")
	}

	select {
	case <-callbackDrained:
		t.Fatal("stale close drained old session callbacks")
	default:
	}

	d.removeSessionMeta(oldMeta)

	if oldMeta.Closed || oldMeta.Status == StatusClosed {
		t.Fatal("stale worker close mutated the old session meta")
	}
	if replacementMeta.Closed || replacementMeta.Status == StatusClosed {
		t.Fatal("stale worker close mutated the replacement session meta")
	}
	d.sessionsMu.RLock()
	currentMeta = d.sessions[sessionID]
	d.sessionsMu.RUnlock()
	d.workersMu.RLock()
	currentHandle = d.workers[sessionID]
	oldOwner = d.workerOwners[oldHandle]
	replacementOwner = d.workerOwners[replacementHandle]
	d.workersMu.RUnlock()
	if currentMeta != replacementMeta || currentHandle != replacementHandle ||
		oldOwner != oldMeta || replacementOwner != replacementMeta {
		t.Fatal("stale worker close changed replacement maps")
	}
	persisted, err = d.store.load(sessionID)
	if err != nil {
		t.Fatalf("reload replacement session: %v", err)
	}
	if persisted.Closed {
		t.Fatal("stale worker close persisted the replacement session as closed")
	}
	select {
	case <-callbackDrained:
		t.Fatal("stale worker close drained old session callbacks")
	default:
	}
}

func TestPersistCurrentSessionSkipsStaleMeta(t *testing.T) {
	const sessionID = "session-persist-reused"

	oldMeta := NewSessionMeta(sessionID, "bot-old")
	replacementMeta := NewSessionMeta(sessionID, "bot-replacement")
	replacementMeta.AddOutput("replacement output")
	d := &Daemon{
		sessions: map[string]*SessionMeta{
			sessionID: oldMeta,
		},
		store: NewSessionStore(t.TempDir()),
	}
	if err := d.store.save(oldMeta.ToPersisted()); err != nil {
		t.Fatalf("persist old session fixture: %v", err)
	}

	d.sessions[sessionID] = replacementMeta
	if err := d.store.save(replacementMeta.ToPersisted()); err != nil {
		t.Fatalf("persist replacement session fixture: %v", err)
	}

	persisted := false
	if err := d.persistCurrentSession(oldMeta, func() error {
		persisted = true
		return d.store.UpdateOutput(sessionID, "stale output")
	}); err != nil {
		t.Fatalf("persist stale session: %v", err)
	}
	if persisted {
		t.Fatal("stale session persistence callback was invoked")
	}

	got, err := d.store.load(sessionID)
	if err != nil {
		t.Fatalf("load replacement session: %v", err)
	}
	if got.Closed {
		t.Fatal("replacement persisted session is closed")
	}
	if len(got.LastOutput) != 1 || got.LastOutput[0] != "replacement output" {
		t.Fatalf("replacement LastOutput = %#v, want unchanged output", got.LastOutput)
	}
}

func TestPurgeSessionDoesNotHoldGlobalMapsDuringFileRemove(t *testing.T) {
	const sessionID = "slow-purge"

	meta := NewSessionMeta(sessionID, "bot-test")
	d := &Daemon{
		sessions: map[string]*SessionMeta{sessionID: meta},
		workers:  make(map[string]*WorkerHandle),
		store:    NewSessionStore(t.TempDir()),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	removeStarted := make(chan struct{})
	releaseRemove := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseRemove)
		}
	}()
	d.store.beforeRemove = func() {
		close(removeStarted)
		<-releaseRemove
	}

	purgeDone := make(chan struct{})
	go func() {
		d.PurgeSession(sessionID)
		close(purgeDone)
	}()

	select {
	case <-removeStarted:
	case <-time.After(time.Second):
		t.Fatal("PurgeSession did not reach file removal")
	}

	mapReadDone := make(chan struct{})
	go func() {
		d.sessionsMu.RLock()
		d.sessionsMu.RUnlock()
		close(mapReadDone)
	}()
	select {
	case <-mapReadDone:
	case <-time.After(time.Second):
		t.Fatal("sessionsMu remained locked while session file removal was blocked")
	}

	close(releaseRemove)
	released = true
	select {
	case <-purgeDone:
	case <-time.After(time.Second):
		t.Fatal("PurgeSession did not finish after file removal was released")
	}
}

func TestPurgeAndNewSameSessionIDSerializeFileLifecycle(t *testing.T) {
	const sessionID = "purge-recreate"

	cfg := config.DefaultConfig()
	cfg.SessionsDir = t.TempDir()
	cfg.Bots[0].BotID = "bot-replacement"
	oldMeta := NewSessionMeta(sessionID, "bot-old")
	d := &Daemon{
		cfg:      cfg,
		selfExe:  "",
		sessions: map[string]*SessionMeta{sessionID: oldMeta},
		workers:  make(map[string]*WorkerHandle),
		store:    NewSessionStore(cfg.SessionsDir),
	}
	if err := d.store.save(oldMeta.ToPersisted()); err != nil {
		t.Fatalf("persist old session fixture: %v", err)
	}

	removeStarted := make(chan struct{})
	releaseRemove := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseRemove)
		}
	}()
	replacementSaved := make(chan struct{})
	d.store.beforeRemove = func() {
		close(removeStarted)
		<-releaseRemove
	}
	d.store.beforeSave = func(ps *PersistedSession) {
		if ps.SessionID == sessionID && ps.BotID == "bot-replacement" {
			close(replacementSaved)
		}
	}

	purgeDone := make(chan struct{})
	go func() {
		d.PurgeSession(sessionID)
		close(purgeDone)
	}()
	select {
	case <-removeStarted:
	case <-time.After(time.Second):
		t.Fatal("PurgeSession did not reach file removal")
	}

	newStarted := make(chan struct{})
	newDone := make(chan error, 1)
	go func() {
		close(newStarted)
		_, err := d.NewSession(NewSessionOpts{
			SessionID: sessionID,
			BotID:     "bot-replacement",
		})
		newDone <- err
	}()
	<-newStarted

	replacementFileLockAcquired := make(chan struct{})
	go func() {
		unlock := d.lockSessionFile(sessionID)
		close(replacementFileLockAcquired)
		unlock()
	}()

	select {
	case <-replacementSaved:
		t.Fatal("replacement session persisted before old file removal was released")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-replacementFileLockAcquired:
		t.Fatal("same-ID file lock was available while old file removal was blocked")
	case <-time.After(time.Second):
	}

	close(releaseRemove)
	released = true
	select {
	case <-purgeDone:
	case <-time.After(time.Second):
		t.Fatal("PurgeSession did not finish after file removal was released")
	}
	select {
	case <-replacementFileLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("same-ID file lock was not released after purge")
	}
	if err := <-newDone; err == nil {
		t.Fatal("NewSession unexpectedly spawned a worker with an empty executable")
	}
	select {
	case <-replacementSaved:
	case <-time.After(time.Second):
		t.Fatal("replacement session was not persisted after old file removal completed")
	}

	persisted, err := d.store.load(sessionID)
	if err != nil {
		t.Fatalf("load replacement session: %v", err)
	}
	if persisted.BotID != "bot-replacement" {
		t.Fatalf("persisted BotID = %q, want replacement", persisted.BotID)
	}
	if persisted.Closed {
		t.Fatal("replacement session persisted as closed")
	}
}
