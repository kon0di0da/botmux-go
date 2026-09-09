package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/daemon"
	"botmux-go/internal/protocol"
)

type interruptTestAdapter struct {
	interrupts        atomic.Int32
	interruptErr      error
	interruptStarted  chan struct{}
	interruptRelease  <-chan struct{}
	interruptReturned chan struct{}
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
	if a.interruptStarted != nil {
		close(a.interruptStarted)
	}
	if a.interruptRelease != nil {
		<-a.interruptRelease
	}
	if a.interruptReturned != nil {
		close(a.interruptReturned)
	}
	return a.interruptErr
}

type blockingSendTestAdapter struct {
	sendStarted      chan struct{}
	sendRelease      <-chan struct{}
	interruptStarted chan struct{}
}

func (a *blockingSendTestAdapter) Name() string {
	return "blocking-send-test"
}

func (a *blockingSendTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	return nil, errors.New("Start must not be called")
}

func (a *blockingSendTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	close(a.sendStarted)
	<-a.sendRelease
	return adapter.SendResult{}, errors.New("history confirmation interrupted")
}

func (a *blockingSendTestAdapter) Close() error {
	return nil
}

func (a *blockingSendTestAdapter) Interrupt(context.Context) error {
	close(a.interruptStarted)
	return nil
}

func newDaemonMessageWorker(cli adapter.CliAdapter) (*Worker, net.Conn, net.Conn) {
	workerConn, daemonConn := net.Pipe()
	w := New(Options{SessionID: "worker-cancel-test", CliType: "codex"})
	w.cliAdapter = cli
	w.conn = workerConn
	w.msgReader = protocol.NewMessageReader(workerConn)
	return w, daemonConn, workerConn
}

func waitForSignal(t *testing.T, signal <-chan struct{}, action string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", action)
	}
}

func readWorkerMessages(conn net.Conn) <-chan *protocol.Message {
	messages := make(chan *protocol.Message, 4)
	go func() {
		defer close(messages)
		reader := protocol.NewMessageReader(conn)
		for {
			msg, err := reader.Read()
			if err != nil {
				return
			}
			messages <- msg
		}
	}()
	return messages
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

func TestWorkerCancelFailureAfterNormalTerminalDoesNotPublishSecondTerminal(t *testing.T) {
	interruptRelease := make(chan struct{})
	cli := &interruptTestAdapter{
		interruptErr:      errors.New("Esc write failed"),
		interruptStarted:  make(chan struct{}),
		interruptRelease:  interruptRelease,
		interruptReturned: make(chan struct{}),
	}
	w, daemonConn, workerConn := newDaemonMessageWorker(cli)
	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = workerConn.Close()
		w.Cancel()
	})

	events := make(chan adapter.AdapterEvent, 1)
	w.startResult = &adapter.CliStartResult{Events: events}
	close(w.readyCh)
	if !w.beginTurn() {
		t.Fatal("begin turn")
	}
	messages := readWorkerMessages(daemonConn)
	w.wg.Add(2)
	go w.readDaemonMessages()
	go w.readAdapterEvents()

	if _, err := protocol.NewMessage(protocol.MsgCancelTurn, w.sessionID, "").WriteTo(daemonConn); err != nil {
		t.Fatalf("send cancel turn: %v", err)
	}
	waitForSignal(t, cli.interruptStarted, "adapter interrupt")

	events <- adapter.AdapterEvent{Kind: adapter.AdapterTurnTerminal, Status: adapter.TurnCompleted}
	normal, ok := <-messages
	if !ok {
		t.Fatal("worker connection closed before normal terminal")
	}
	if normal.Type != protocol.MsgTurnCompleted {
		t.Fatalf("worker message type = %s, want %s", normal.Type, protocol.MsgTurnCompleted)
	}

	close(interruptRelease)
	waitForSignal(t, cli.interruptReturned, "failed adapter interrupt return")

	if _, err := protocol.NewMessage(protocol.MsgClose, w.sessionID, "").WriteTo(daemonConn); err != nil {
		t.Fatalf("send close: %v", err)
	}
	w.wg.Wait()

	terminals := []*protocol.Message{normal}
	for msg := range messages {
		if msg.Type == protocol.MsgTurnCompleted {
			terminals = append(terminals, msg)
		}
	}
	if len(terminals) != 1 {
		t.Fatalf("turn terminal count = %d, want 1", len(terminals))
	}

	var terminal protocol.TurnTerminal
	if err := json.Unmarshal([]byte(terminals[0].Payload), &terminal); err != nil {
		t.Fatalf("decode turn terminal: %v", err)
	}
	if terminal.Status != protocol.TurnCompleted {
		t.Fatalf("terminal status = %s, want %s", terminal.Status, protocol.TurnCompleted)
	}
	if terminal.ErrorCode == "codex_cancel_failed" {
		t.Fatal("normal terminal was followed by codex_cancel_failed")
	}
}

func TestWorkerQueuedInterruptDoesNotTargetNextTurn(t *testing.T) {
	cli := &interruptTestAdapter{}
	w := New(Options{SessionID: "worker-cancel-test", CliType: "codex"})
	w.cliAdapter = cli
	t.Cleanup(w.Cancel)

	firstTurnID, ok := w.beginTurnWithID()
	if !ok {
		t.Fatal("begin first turn")
	}
	if _, ok := w.requestTurnInterrupt(); !ok {
		t.Fatal("request first turn interrupt")
	}
	if !w.completeTurn(firstTurnID, protocol.TurnTerminal{Status: protocol.TurnCompleted}) {
		t.Fatal("complete first turn")
	}
	if _, ok := w.beginTurnWithID(); ok {
		t.Fatal("accepted next turn before queued interrupt completed")
	}

	w.wg.Add(1)
	go w.interruptTurn(firstTurnID)
	w.wg.Wait()

	if got := cli.interrupts.Load(); got != 0 {
		t.Fatalf("adapter interrupts = %d, want 0 for completed turn", got)
	}
	if _, ok := w.beginTurnWithID(); !ok {
		t.Fatal("begin next turn after queued interrupt completed")
	}
}

func TestWorkerProcessesCancelWhileCodexSendBlocks(t *testing.T) {
	sendRelease := make(chan struct{})
	var releaseSendOnce sync.Once
	releaseSend := func() {
		releaseSendOnce.Do(func() {
			close(sendRelease)
		})
	}
	cli := &blockingSendTestAdapter{
		sendStarted:      make(chan struct{}),
		sendRelease:      sendRelease,
		interruptStarted: make(chan struct{}),
	}
	w, daemonConn, workerConn := newDaemonMessageWorker(cli)
	t.Cleanup(func() {
		releaseSend()
		_ = daemonConn.Close()
		_ = workerConn.Close()
		w.Cancel()
		w.wg.Wait()
	})

	close(w.readyCh)
	messages := readWorkerMessages(daemonConn)
	w.wg.Add(1)
	go w.readDaemonMessages()

	if _, err := protocol.NewMessage(protocol.MsgUserInput, w.sessionID, "hello").WriteTo(daemonConn); err != nil {
		t.Fatalf("send user input: %v", err)
	}
	waitForSignal(t, cli.sendStarted, "adapter Send")

	cancelWrite := make(chan error, 1)
	go func() {
		_, err := protocol.NewMessage(protocol.MsgCancelTurn, w.sessionID, "").WriteTo(daemonConn)
		cancelWrite <- err
	}()
	waitForSignal(t, cli.interruptStarted, "adapter Interrupt before Send release")

	releaseSend()
	select {
	case err := <-cancelWrite:
		if err != nil {
			t.Fatalf("send cancel turn: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel turn write did not complete")
	}
	select {
	case msg := <-messages:
		if msg == nil {
			t.Fatal("worker connection closed before Send returned")
		}
		if msg.Type != protocol.MsgTurnCompleted {
			t.Fatalf("worker message type = %s, want no terminal", msg.Type)
		}
		var terminal protocol.TurnTerminal
		if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
			t.Fatalf("decode turn terminal: %v", err)
		}
		t.Fatalf("old Send published terminal after cancel: %#v", terminal)
	case <-time.After(200 * time.Millisecond):
	}
	w.Cancel()
	w.wg.Wait()

	for msg := range messages {
		if msg.Type == protocol.MsgTurnCompleted {
			t.Fatalf("old Send published terminal while worker was closing: %s", msg.Payload)
		}
	}
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
