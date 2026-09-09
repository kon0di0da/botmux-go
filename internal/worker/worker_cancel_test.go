package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/daemon"
	"botmux-go/internal/protocol"
)

type interruptTestAdapter struct {
	interrupts   atomic.Int32
	interruptErr error
}

func (a *interruptTestAdapter) Name() string {
	return "interrupt-test"
}

func (a *interruptTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	return nil, errors.New("Start must not be called")
}

func (a *interruptTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *interruptTestAdapter) Close() error {
	return nil
}

func (a *interruptTestAdapter) Interrupt(context.Context) error {
	a.interrupts.Add(1)
	return a.interruptErr
}

func newDaemonMessageWorker(cli adapter.CliAdapter) (*Worker, net.Conn, net.Conn) {
	workerConn, daemonConn := net.Pipe()
	w := New(Options{SessionID: "worker-cancel-test", CliType: "codex"})
	w.cliAdapter = cli
	w.conn = workerConn
	w.msgReader = protocol.NewMessageReader(workerConn)
	return w, daemonConn, workerConn
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func mustReadWorkerMessage(t *testing.T, conn net.Conn) *protocol.Message {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			t.Errorf("reset read deadline: %v", err)
		}
	}()

	msg, err := protocol.NewMessageReader(conn).Read()
	if err != nil {
		t.Fatalf("read worker message: %v", err)
	}
	return msg
}

func TestWorkerCancelTurnCallsAdapterInterrupt(t *testing.T) {
	cli := &interruptTestAdapter{}
	w, daemonConn, workerConn := newDaemonMessageWorker(cli)
	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = workerConn.Close()
		w.Cancel()
	})

	close(w.readyCh)
	if !w.beginTurn() {
		t.Fatal("begin turn")
	}
	w.wg.Add(1)
	go w.readDaemonMessages()

	if _, err := protocol.NewMessage(protocol.MsgCancelTurn, w.sessionID, "").WriteTo(daemonConn); err != nil {
		t.Fatalf("send cancel turn: %v", err)
	}
	waitFor(t, func() bool {
		return cli.interrupts.Load() == 1
	})

	w.Cancel()
	w.wg.Wait()
}

func TestWorkerCancelFailurePublishesFailedTerminal(t *testing.T) {
	cli := &interruptTestAdapter{interruptErr: errors.New("Esc write failed")}
	w, daemonConn, workerConn := newDaemonMessageWorker(cli)
	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = workerConn.Close()
		w.Cancel()
	})

	close(w.readyCh)
	if !w.beginTurn() {
		t.Fatal("begin turn")
	}
	w.wg.Add(1)
	go w.readDaemonMessages()

	if _, err := protocol.NewMessage(protocol.MsgCancelTurn, w.sessionID, "").WriteTo(daemonConn); err != nil {
		t.Fatalf("send cancel turn: %v", err)
	}
	msg := mustReadWorkerMessage(t, daemonConn)
	if msg.Type != protocol.MsgTurnCompleted {
		t.Fatalf("worker message type = %s, want %s", msg.Type, protocol.MsgTurnCompleted)
	}

	var terminal protocol.TurnTerminal
	if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
		t.Fatalf("decode turn terminal: %v", err)
	}
	if terminal.Status != protocol.TurnFailed {
		t.Fatalf("terminal status = %s, want %s", terminal.Status, protocol.TurnFailed)
	}
	if terminal.ErrorCode != "codex_cancel_failed" {
		t.Fatalf("terminal error code = %q, want %q", terminal.ErrorCode, "codex_cancel_failed")
	}

	w.Cancel()
	w.wg.Wait()
}

func TestWorkerRestartDoesNotMarkSessionClosed(t *testing.T) {
	const sessionID = "worker-restart-open"
	storeDir := t.TempDir()
	sessionPath := filepath.Join(storeDir, sessionID+".json")
	fixture, err := json.Marshal(&daemon.PersistedSession{SessionID: sessionID, BotID: "bot-test"})
	if err != nil {
		t.Fatalf("marshal session fixture: %v", err)
	}
	if err := os.WriteFile(sessionPath, fixture, 0o644); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	w := New(Options{
		SessionID:  sessionID,
		DaemonAddr: listener.Addr().String(),
		CliType:    "mock",
		StoreDir:   storeDir,
	})
	runDone := make(chan error, 1)
	go func() {
		runDone <- w.Run()
	}()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept worker: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		w.Cancel()
	})

	ready := mustReadWorkerMessage(t, conn)
	if ready.Type != protocol.MsgReady {
		t.Fatalf("worker first message = %s, want %s", ready.Type, protocol.MsgReady)
	}
	if _, err := protocol.NewMessage(protocol.MsgRestartWorker, sessionID, "").WriteTo(conn); err != nil {
		t.Fatalf("send restart worker: %v", err)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgRestartWorker")
	}

	data, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatalf("load restarted session: %v", err)
	}
	var persisted daemon.PersistedSession
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("decode restarted session: %v", err)
	}
	if persisted.Closed {
		t.Fatal("restart worker marked the session closed")
	}
}
