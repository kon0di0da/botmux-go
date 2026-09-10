package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os/exec"
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
			d.workersMu.Lock()
			d.workers[meta.SessionID] = replacement
			d.workersMu.Unlock()
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
	d.workersMu.Lock()
	d.workers[meta.SessionID] = replacement
	d.workersMu.Unlock()

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
	d.workersMu.Lock()
	d.workers[meta.SessionID] = current
	d.workersMu.Unlock()
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
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	msg, err := protocol.DecodeMessage(conn)
	if err != nil {
		t.Fatalf("read worker message: %v", err)
	}
	return msg
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
