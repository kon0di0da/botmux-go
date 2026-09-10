package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"botmux-go/internal/config"
	"botmux-go/internal/protocol"
)

func TestCancelTurnSendsIPCAndMarksCancelling(t *testing.T) {
	d, meta, handle, workerConn := newCancelTestDaemon(t)
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	msg := mustReadMessage(t, workerConn)
	if msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	snapshot := meta.TurnSnapshot()
	if !snapshot.Active || !snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want active cancelling turn", snapshot)
	}
	if got, ok := d.GetWorkerHandle(meta.SessionID); !ok || got != handle {
		t.Fatalf("worker handle = %p, want original handle %p", got, handle)
	}

	meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted})
}

func TestCancelTurnWatchdogRunsWhileCancelWriteBlocks(t *testing.T) {
	d, meta, handle, workerConn := newCancelTestDaemon(t)
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	select {
	case err := <-cancelErr:
		if err != nil {
			t.Fatalf("CancelTurn: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("CancelTurn did not return while cancel IPC write was blocked")
	}

	waitForCondition(t, time.Second, func() bool {
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		if len(terminals) != 1 {
			return false
		}
		d.sessionsMu.RLock()
		status := meta.Status
		d.sessionsMu.RUnlock()
		return status == StatusRecovering
	})
	terminals, _, _ := meta.SnapshotTerminalsSince(0)
	if got := terminals[0]; got.Status != protocol.TurnFailed || got.ErrorCode != "codex_cancel_timeout" {
		t.Fatalf("terminal = %#v, want codex_cancel_timeout failure", got)
	}
	if snapshot := meta.TurnSnapshot(); snapshot.Active || snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want inactive non-cancelling turn", snapshot)
	}

	cursor, terminalNotify := meta.TerminalSubscription()
	if cursor != 1 {
		t.Fatalf("terminal cursor = %d, want 1", cursor)
	}
	if err := workerConn.Close(); err != nil {
		t.Fatalf("close worker pipe: %v", err)
	}
	handle.CloseConn()

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-terminalNotify:
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		t.Fatalf("late cancel delivery published a second terminal: %#v", terminals)
	case <-timer.C:
	}
}

func TestCancelTurnRetriesReplacementWorker(t *testing.T) {
	d, meta, original, _ := newCancelTestDaemon(t)
	d.turnCancelTimeout = time.Second
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	replacement, replacementWorkerConn := newCancelTestWorker(t, d, meta.SessionID)
	originalConn := original.Conn
	original.SetConn(&replaceOnFirstWriteConn{
		Conn: originalConn,
		replace: func() {
			installTestWorker(d, meta, replacement)
		},
		err: errors.New("original worker write failed"),
	})

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	if msg := mustReadMessage(t, replacementWorkerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("replacement message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	snapshot := meta.TurnSnapshot()
	if !snapshot.Active || !snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want active cancelling turn", snapshot)
	}
	meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted})
}

func TestCancelTurnAcceptsBeforeWorkerReplacementRetryExhaustionFailure(t *testing.T) {
	d, meta, original, _ := newCancelTestDaemon(t)
	d.turnCancelTimeout = 100 * time.Millisecond
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	handles := make([]*WorkerHandle, maxCurrentWorkerSendTries+1)
	handles[0] = original
	for i := 1; i < len(handles); i++ {
		handles[i], _ = newCancelTestWorker(t, d, meta.SessionID)
	}
	for i := 0; i < maxCurrentWorkerSendTries; i++ {
		current, next, attempt := handles[i], handles[i+1], i
		currentConn := current.Conn
		current.SetConn(&replaceOnFirstWriteConn{
			Conn: currentConn,
			replace: func() {
				installTestWorker(d, meta, next)
			},
			err: fmt.Errorf("worker generation %d write failed", attempt),
		})
	}

	if err := d.CancelTurn(meta.SessionID); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		return len(terminals) == 1
	})
	terminals, _, _ := meta.SnapshotTerminalsSince(0)
	if len(terminals) != 1 {
		t.Fatalf("terminal count = %d, want 1: %#v", len(terminals), terminals)
	}
	if got := terminals[0]; got.Status != protocol.TurnFailed || got.ErrorCode != "codex_cancel_failed" {
		t.Fatalf("terminal = %#v, want codex_cancel_failed failure", got)
	}
	if terminals[0].ErrorDetail == "" {
		t.Fatal("terminal failure did not include the delivery error")
	}
	if snapshot := meta.TurnSnapshot(); snapshot.Active || snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want terminal non-cancelling turn", snapshot)
	}

	cursor, terminalNotify := meta.TerminalSubscription()
	if cursor != 1 {
		t.Fatalf("terminal cursor = %d, want 1", cursor)
	}
	timer := time.NewTimer(3 * d.turnCancelTimeout)
	defer timer.Stop()
	select {
	case <-terminalNotify:
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		t.Fatalf("delayed cancellation watchdog published a second terminal: %#v", terminals)
	case <-timer.C:
	}
}

func TestCancelTurnTimeoutPublishesOneFailureAndRestartsWorker(t *testing.T) {
	d, meta, handle, workerConn := newCancelTestDaemon(t)
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	if msg := mustReadMessage(t, workerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("first message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
	if msg := mustReadMessage(t, workerConn); msg.Type != protocol.MsgRestartWorker {
		t.Fatalf("second message type = %s, want %s", msg.Type, protocol.MsgRestartWorker)
	}

	waitForCondition(t, time.Second, func() bool {
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		return len(terminals) == 1
	})
	terminals, _, _ := meta.SnapshotTerminalsSince(0)
	if got := terminals[0]; got.Status != protocol.TurnFailed || got.ErrorCode != "codex_cancel_timeout" {
		t.Fatalf("terminal = %#v, want codex_cancel_timeout failure", got)
	}
	d.sessionsMu.RLock()
	closed := meta.Closed
	status := meta.Status
	d.sessionsMu.RUnlock()
	if closed {
		t.Fatal("session closed after cancellation timeout")
	}
	if status != StatusRecovering {
		t.Fatalf("session status = %s, want %s", status, StatusRecovering)
	}
	if meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted}) {
		t.Fatal("late turn_aborted committed a second terminal")
	}
	select {
	case <-handle.ExitDone:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after restart grace period")
	}
}

func TestCancelTimeoutLateReadyDoesNotAdmitTurn(t *testing.T) {
	d, meta, handle, workerConn := newCancelTestDaemon(t)
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	if msg := mustReadMessage(t, workerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("cancel message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	// Do not read the restart IPC. This holds the watchdog in restartWorkerForSession
	// after it has committed the timeout terminal and RECOVERING status.
	waitForCondition(t, time.Second, func() bool {
		d.sessionsMu.RLock()
		status := meta.Status
		pending := d.pendingRestart[meta]
		d.sessionsMu.RUnlock()
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		return status == StatusRecovering && pending == handle && len(terminals) == 1
	})

	inputDone := make(chan struct{})
	inputStarted := false
	defer func() {
		_ = workerConn.Close()
		if !inputStarted {
			return
		}
		select {
		case <-inputDone:
		case <-time.After(time.Second):
			t.Error("SendInput goroutine did not return after unblocking worker IPC")
		}
	}()

	// The current worker is already headed for restart. Its late READY must not
	// reopen the session while the restart IPC write is blocked.
	d.routeMessage(protocol.NewMessage(protocol.MsgReady, meta.SessionID, ""), meta, handle)
	d.sessionsMu.RLock()
	status := meta.Status
	d.sessionsMu.RUnlock()
	if status != StatusRecovering {
		t.Fatalf("session status after late ready = %s, want %s", status, StatusRecovering)
	}

	inputErr := make(chan error, 1)
	inputStarted = true
	go func() {
		defer close(inputDone)
		inputErr <- d.SendInput(meta.SessionID, "next turn")
	}()

	select {
	case err := <-inputErr:
		if err == nil || !strings.Contains(err.Error(), string(StatusRecovering)) {
			t.Fatalf("SendInput error = %v, want RECOVERING rejection", err)
		}
	case <-time.After(100 * time.Millisecond):
		snapshot := meta.TurnSnapshot()
		if snapshot.Active {
			t.Fatalf("SendInput admitted a turn during recovery: %#v", snapshot)
		}
		t.Fatal("SendInput did not promptly reject a recovering session")
	}

	if snapshot := meta.TurnSnapshot(); snapshot.Active || snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want inactive non-cancelling turn", snapshot)
	}

	replacement := NewWorkerHandle(meta.SessionID)
	installTestWorker(d, meta, replacement)
	d.routeMessage(protocol.NewMessage(protocol.MsgReady, meta.SessionID, ""), meta, replacement)
	if !replacement.IsReady() {
		t.Fatal("replacement worker was not marked ready")
	}
	d.sessionsMu.RLock()
	status = meta.Status
	d.sessionsMu.RUnlock()
	if status != StatusReady {
		t.Fatalf("session status after replacement ready = %s, want %s", status, StatusReady)
	}
}

func TestCancelTimeoutRestartsCurrentReplacementWorker(t *testing.T) {
	d, meta, _, workerConn := newCancelTestDaemon(t)
	d.turnCancelTimeout = 100 * time.Millisecond
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	if msg := mustReadMessage(t, workerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("cancel message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	replacement, replacementWorkerConn := newCancelTestWorker(t, d, meta.SessionID)
	installTestWorker(d, meta, replacement)

	if msg := mustReadMessage(t, replacementWorkerConn); msg.Type != protocol.MsgRestartWorker {
		t.Fatalf("replacement message type = %s, want %s", msg.Type, protocol.MsgRestartWorker)
	}

	waitForCondition(t, time.Second, func() bool {
		terminals, _, _ := meta.SnapshotTerminalsSince(0)
		return len(terminals) == 1
	})
	terminals, _, _ := meta.SnapshotTerminalsSince(0)
	if got := terminals[0]; got.Status != protocol.TurnFailed || got.ErrorCode != "codex_cancel_timeout" {
		t.Fatalf("terminal = %#v, want codex_cancel_timeout failure", got)
	}
	d.sessionsMu.RLock()
	closed := meta.Closed
	status := meta.Status
	d.sessionsMu.RUnlock()
	if closed {
		t.Fatal("session closed after cancellation timeout")
	}
	if status != StatusRecovering {
		t.Fatalf("session status = %s, want %s", status, StatusRecovering)
	}
	select {
	case <-replacement.ExitDone:
	case <-time.After(time.Second):
		t.Fatal("replacement worker did not exit after restart grace period")
	}
}

func TestCancelWatchdogDoesNotRestartReusedSessionID(t *testing.T) {
	d, oldMeta, _, oldWorkerConn := newCancelTestDaemon(t)
	if !oldMeta.BeginTurn() {
		t.Fatal("begin old turn")
	}
	token, ok := oldMeta.BeginTurnCancel()
	if !ok {
		t.Fatal("begin old turn cancellation")
	}
	sendErr := make(chan error, 1)
	go func() {
		_, err := d.sendToCurrentWorker(oldMeta, protocol.MsgCancelTurn, "", nil)
		sendErr <- err
	}()
	if msg := mustReadMessage(t, oldWorkerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("old worker message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("send old cancel: %v", err)
	}

	newMeta := NewSessionMeta(oldMeta.SessionID, "bot-codex")
	newMeta.CliType = string(config.CliCodex)
	newMeta.Status = StatusReady
	newHandle, newWorkerConn := newCancelTestWorker(t, d, oldMeta.SessionID)
	d.sessionsMu.Lock()
	d.sessions[oldMeta.SessionID] = newMeta
	d.sessionsMu.Unlock()
	installTestWorker(d, newMeta, newHandle)

	watchDone := make(chan struct{})
	go func() {
		d.watchCancelledTurn(oldMeta, token)
		close(watchDone)
	}()

	msg, err := readMessageWithDeadline(newWorkerConn, 100*time.Millisecond)
	if err == nil {
		t.Fatalf("reused session worker received %s, want no message", msg.Type)
	}
	if !isTimeout(err) {
		t.Fatalf("read reused session worker: %v, want timeout", err)
	}
	select {
	case <-watchDone:
	case <-time.After(time.Second):
		t.Fatal("old cancellation watchdog did not return")
	}

	if terminals, _, _ := oldMeta.SnapshotTerminalsSince(0); len(terminals) != 0 {
		t.Fatalf("old meta terminals = %#v, want none", terminals)
	}
	if terminals, _, _ := newMeta.SnapshotTerminalsSince(0); len(terminals) != 0 {
		t.Fatalf("new meta terminals = %#v, want none", terminals)
	}
	d.sessionsMu.RLock()
	currentMeta := d.sessions[newMeta.SessionID]
	status := newMeta.Status
	d.sessionsMu.RUnlock()
	if currentMeta != newMeta {
		t.Fatalf("current meta = %p, want replacement %p", currentMeta, newMeta)
	}
	if status != StatusReady {
		t.Fatalf("new meta status = %s, want %s", status, StatusReady)
	}
}

func TestBeginTurnCancelIfCurrentRejectsReusedSessionID(t *testing.T) {
	d, oldMeta, _, _ := newCancelTestDaemon(t)
	if !oldMeta.BeginTurn() {
		t.Fatal("begin old turn")
	}

	replacement := NewSessionMeta(oldMeta.SessionID, "bot-codex")
	replacement.CliType = string(config.CliCodex)
	replacement.Status = StatusReady
	d.sessionsMu.Lock()
	d.sessions[oldMeta.SessionID] = replacement
	d.sessionsMu.Unlock()

	if token, ok := d.beginTurnCancelIfCurrent(oldMeta); ok || token != 0 {
		t.Fatalf("begin stale cancellation = (%d, %t), want (0, false)", token, ok)
	}
	if snapshot := oldMeta.TurnSnapshot(); !snapshot.Active || snapshot.Cancelling {
		t.Fatalf("old turn snapshot = %#v, want unchanged active non-cancelling turn", snapshot)
	}
}

func TestCommitCancelSendFailureRetriesReplacementWorker(t *testing.T) {
	d, meta, original, _ := newCancelTestDaemon(t)
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}
	token, ok := meta.BeginTurnCancel()
	if !ok {
		t.Fatal("begin turn cancellation")
	}

	replacement, replacementWorkerConn := newCancelTestWorker(t, d, meta.SessionID)
	installTestWorker(d, meta, replacement)

	if got := d.commitCancelSendFailureIfCurrent(meta, token, original, errors.New("original worker write failed")); got != cancelSendFailureRetry {
		t.Fatalf("commit cancel send failure result = %v, want retry", got)
	}
	if terminals, _, _ := meta.SnapshotTerminalsSince(0); len(terminals) != 0 {
		t.Fatalf("terminals = %#v, want none", terminals)
	}

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- d.sendCancelToCurrentWorker(meta, token)
	}()
	if msg := mustReadMessage(t, replacementWorkerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("replacement message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("send cancellation to replacement worker: %v", err)
	}
}

func TestCancelTurnAbortedTerminalKeepsSessionOpen(t *testing.T) {
	d, meta, handle, workerConn := newCancelTestDaemon(t)
	d.turnCancelTimeout = time.Second
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- d.CancelTurn(meta.SessionID)
	}()

	if msg := mustReadMessage(t, workerConn); msg.Type != protocol.MsgCancelTurn {
		t.Fatalf("message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	payload, err := json.Marshal(protocol.TurnTerminal{Status: protocol.TurnAborted})
	if err != nil {
		t.Fatalf("marshal terminal: %v", err)
	}
	d.routeMessage(protocol.NewMessage(protocol.MsgTurnCompleted, meta.SessionID, string(payload)), meta, handle)

	snapshot := meta.TurnSnapshot()
	if snapshot.Active || snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want completed non-cancelling turn", snapshot)
	}
	d.sessionsMu.RLock()
	closed := meta.Closed
	status := meta.Status
	d.sessionsMu.RUnlock()
	if closed {
		t.Fatal("session closed after turn_aborted")
	}
	if status != StatusReady {
		t.Fatalf("session status = %s, want %s", status, StatusReady)
	}
}

func TestRouteMessageIgnoresTerminalFromStaleWorker(t *testing.T) {
	d, meta, stale, _ := newCancelTestDaemon(t)
	current := NewWorkerHandle(meta.SessionID)
	installTestWorker(d, meta, current)
	if !meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	payload, err := json.Marshal(protocol.TurnTerminal{Status: protocol.TurnAborted})
	if err != nil {
		t.Fatalf("marshal terminal: %v", err)
	}
	d.routeMessage(protocol.NewMessage(protocol.MsgTurnCompleted, meta.SessionID, string(payload)), meta, stale)

	if snapshot := meta.TurnSnapshot(); !snapshot.Active {
		t.Fatalf("turn snapshot = %#v, want active turn", snapshot)
	}
}

func TestSpawnStaleSessionDoesNotReplaceReusedSessionWorker(t *testing.T) {
	oldMeta := NewSessionMeta("reused-session", "bot-old")
	oldMeta.Status = StatusReady
	oldHandle := NewWorkerHandle(oldMeta.SessionID)
	replacementMeta := NewSessionMeta(oldMeta.SessionID, "bot-new")
	replacementMeta.Status = StatusReady
	replacement := NewWorkerHandle(oldMeta.SessionID)
	replacementConn, replacementPeer := net.Pipe()
	replacement.SetConn(replacementConn)
	t.Cleanup(func() {
		_ = replacementConn.Close()
		_ = replacementPeer.Close()
	})

	d := &Daemon{
		cfg:            &config.DaemonConfig{ListenAddr: "127.0.0.1:1", SessionsDir: t.TempDir()},
		selfExe:        "/definitely/not/a/botmux-worker",
		store:          NewSessionStore(t.TempDir()),
		sessions:       map[string]*SessionMeta{oldMeta.SessionID: oldMeta},
		workers:        map[string]*WorkerHandle{oldMeta.SessionID: oldHandle},
		workerOwners:   map[*WorkerHandle]*SessionMeta{oldHandle: oldMeta},
		pendingRestart: make(map[*SessionMeta]*WorkerHandle),
	}

	d.workersMu.Lock()
	spawnErr := make(chan error, 1)
	go func() {
		spawnErr <- d.spawnWorkerForSession(oldMeta)
	}()

	// The old implementation validates under sessionsMu, drops it, then waits
	// for workersMu. A correct implementation waits on workersMu first.
	oldSpawnObserved := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		d.sessionsMu.RLock()
		oldSpawnObserved = oldMeta.Status == StatusSpawning
		d.sessionsMu.RUnlock()
		if oldSpawnObserved {
			break
		}
		time.Sleep(time.Millisecond)
	}

	d.sessionsMu.Lock()
	d.sessions[oldMeta.SessionID] = replacementMeta
	d.sessionsMu.Unlock()
	d.workers[oldMeta.SessionID] = replacement
	d.workerOwners[replacement] = replacementMeta
	d.workersMu.Unlock()

	err := <-spawnErr
	if err == nil || !strings.Contains(err.Error(), "no longer current") {
		t.Fatalf("spawn stale session error = %v, want current-session error (old phase observed=%t)", err, oldSpawnObserved)
	}

	d.sessionsMu.RLock()
	currentMeta := d.sessions[oldMeta.SessionID]
	status := replacementMeta.Status
	d.sessionsMu.RUnlock()
	if currentMeta != replacementMeta {
		t.Fatalf("current session = %p, want replacement %p", currentMeta, replacementMeta)
	}
	if status != StatusReady || replacementMeta.Closed {
		t.Fatalf("replacement session state = (%s, closed=%t), want READY and open", status, replacementMeta.Closed)
	}
	d.workersMu.RLock()
	currentHandle := d.workers[oldMeta.SessionID]
	owner := d.workerOwners[replacement]
	d.workersMu.RUnlock()
	if currentHandle != replacement {
		t.Fatalf("current worker = %p, want replacement %p", currentHandle, replacement)
	}
	if owner != replacementMeta {
		t.Fatalf("replacement owner = %p, want %p", owner, replacementMeta)
	}
}

func TestWorkerExitDoesNotRecoverReusedSession(t *testing.T) {
	oldMeta := NewSessionMeta("reused-session", "bot-old")
	oldMeta.Status = StatusReady
	newMeta := NewSessionMeta(oldMeta.SessionID, "bot-new")
	newMeta.Status = StatusReady
	callbackDrained := make(chan struct{}, 1)
	newMeta.AddOnClose(func() { callbackDrained <- struct{}{} })

	handle := NewWorkerHandle(oldMeta.SessionID)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid
	d := &Daemon{
		sessions:      map[string]*SessionMeta{oldMeta.SessionID: oldMeta},
		workers:       map[string]*WorkerHandle{oldMeta.SessionID: handle},
		workerOwners:  map[*WorkerHandle]*SessionMeta{handle: oldMeta},
		spawnFailures: make(map[string]*spawnFailure),
	}
	t.Cleanup(func() {
		if handle.Cmd.Process != nil {
			_ = handle.Cmd.Process.Kill()
		}
		select {
		case <-handle.ExitDone:
		case <-time.After(time.Second):
			t.Error("worker fixture was not reaped")
		}
	})

	d.sessionsMu.Lock()
	d.sessions[oldMeta.SessionID] = newMeta
	d.sessionsMu.Unlock()
	go d.waitWorkerExit(handle)
	if err := handle.Cmd.Process.Kill(); err != nil {
		t.Fatalf("kill old worker: %v", err)
	}
	select {
	case <-handle.ExitDone:
	case <-time.After(time.Second):
		t.Fatal("old worker did not exit")
	}

	d.sessionsMu.RLock()
	status := newMeta.Status
	closed := newMeta.Closed
	d.sessionsMu.RUnlock()
	if status != StatusReady || closed {
		t.Fatalf("replacement session state = (%s, closed=%t), want READY and open", status, closed)
	}
	select {
	case <-callbackDrained:
		t.Fatal("replacement session callback was drained by old worker exit")
	default:
	}
	d.workersMu.RLock()
	current := d.workers[newMeta.SessionID]
	d.workersMu.RUnlock()
	if current != handle {
		t.Fatalf("current worker = %p, want old handle %p", current, handle)
	}
}

func TestRouteMessageCloseDoesNotCloseReusedSession(t *testing.T) {
	d, oldMeta, oldHandle, _ := newCancelTestDaemon(t)
	newMeta := NewSessionMeta(oldMeta.SessionID, "bot-new")
	newMeta.Status = StatusReady
	callbackDrained := make(chan struct{}, 1)
	newMeta.AddOnClose(func() { callbackDrained <- struct{}{} })

	d.sessionsMu.Lock()
	d.sessions[oldMeta.SessionID] = newMeta
	d.sessionsMu.Unlock()

	d.routeMessage(protocol.NewMessage(protocol.MsgClose, oldMeta.SessionID, ""), oldMeta, oldHandle)

	d.sessionsMu.RLock()
	currentMeta := d.sessions[oldMeta.SessionID]
	status := newMeta.Status
	closed := newMeta.Closed
	d.sessionsMu.RUnlock()
	if currentMeta != newMeta {
		t.Fatalf("current session = %p, want replacement %p", currentMeta, newMeta)
	}
	if status != StatusReady || closed {
		t.Fatalf("replacement session state = (%s, closed=%t), want READY and open", status, closed)
	}
	select {
	case <-callbackDrained:
		t.Fatal("replacement session callback was drained by old worker close")
	default:
	}
	d.workersMu.RLock()
	currentHandle := d.workers[oldMeta.SessionID]
	d.workersMu.RUnlock()
	if currentHandle != oldHandle {
		t.Fatalf("current worker = %p, want old handle %p", currentHandle, oldHandle)
	}
}

func newCancelTestDaemon(t *testing.T) (*Daemon, *SessionMeta, *WorkerHandle, net.Conn) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	meta := NewSessionMeta("cancel-turn-test", "bot-codex")
	meta.CliType = string(config.CliCodex)
	meta.Status = StatusReady
	store := NewSessionStore(t.TempDir())
	if err := store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	daemonConn, workerConn := net.Pipe()
	handle := NewWorkerHandle(meta.SessionID)
	handle.SetConn(daemonConn)
	handle.markReady()
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	d := &Daemon{
		ctx:                ctx,
		cancel:             cancel,
		store:              store,
		sessions:           map[string]*SessionMeta{meta.SessionID: meta},
		workers:            map[string]*WorkerHandle{meta.SessionID: handle},
		workerOwners:       map[*WorkerHandle]*SessionMeta{handle: meta},
		spawnFailures:      make(map[string]*spawnFailure),
		turnCancelTimeout:  20 * time.Millisecond,
		restartWorkerGrace: 20 * time.Millisecond,
	}
	go d.waitWorkerExit(handle)

	t.Cleanup(func() {
		cancel()
		_ = daemonConn.Close()
		_ = workerConn.Close()
		if handle.Cmd.Process != nil {
			_ = handle.Cmd.Process.Kill()
		}
		select {
		case <-handle.ExitDone:
		case <-time.After(time.Second):
			t.Error("worker fixture was not reaped")
		}
	})
	return d, meta, handle, workerConn
}

func newCancelTestWorker(t *testing.T, d *Daemon, sessionID string) (*WorkerHandle, net.Conn) {
	t.Helper()

	daemonConn, workerConn := net.Pipe()
	handle := NewWorkerHandle(sessionID)
	handle.SetConn(daemonConn)
	handle.markReady()
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid
	go d.waitWorkerExit(handle)

	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = workerConn.Close()
		if handle.Cmd.Process != nil {
			_ = handle.Cmd.Process.Kill()
		}
		select {
		case <-handle.ExitDone:
		case <-time.After(time.Second):
			t.Error("worker fixture was not reaped")
		}
	})
	return handle, workerConn
}

func installTestWorker(d *Daemon, meta *SessionMeta, handle *WorkerHandle) {
	d.workersMu.Lock()
	if d.workerOwners == nil {
		d.workerOwners = make(map[*WorkerHandle]*SessionMeta)
	}
	d.workers[meta.SessionID] = handle
	d.workerOwners[handle] = meta
	d.workersMu.Unlock()
}

type replaceOnFirstWriteConn struct {
	net.Conn
	replace func()
	err     error
	done    bool
}

func (c *replaceOnFirstWriteConn) Write(p []byte) (int, error) {
	if !c.done {
		c.done = true
		c.replace()
		return 0, c.err
	}
	return c.Conn.Write(p)
}

func mustReadMessage(t *testing.T, conn net.Conn) *protocol.Message {
	t.Helper()
	msg, err := readMessageWithDeadline(conn, time.Second)
	if err != nil {
		t.Fatalf("read worker message: %v", err)
	}
	return msg
}

func readMessageWithDeadline(conn net.Conn, timeout time.Duration) (*protocol.Message, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	msg, err := protocol.DecodeMessage(conn)
	return msg, err
}

func isTimeout(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal("condition did not become true before timeout")
	}
}
