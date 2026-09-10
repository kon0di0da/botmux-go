package worker

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/protocol"
)

func TestWaitForCLIReadyUsesAuthoritativeResult(t *testing.T) {
	ready := make(chan error, 1)
	ready <- nil
	w := New(Options{CliType: "mock"})
	defer w.Cancel()
	w.startResult = &adapter.CliStartResult{
		ReadyResult: ready,
	}
	if err := w.waitForCLIReady(); err != nil {
		t.Fatalf("waitForCLIReady: %v", err)
	}
}

func TestWorkerReadyIncludesInstanceID(t *testing.T) {
	daemonConn, workerConn := net.Pipe()
	defer daemonConn.Close()
	defer workerConn.Close()

	w := New(Options{
		SessionID:        "worker-ready-nonce",
		CliType:          "mock",
		WorkerInstanceID: "nonce-123",
	})
	defer w.Cancel()
	w.conn = workerConn

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- w.sendReady()
	}()

	msg, err := protocol.DecodeMessage(daemonConn)
	if err != nil {
		t.Fatalf("decode ready message: %v", err)
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("send ready message: %v", err)
	}
	if msg.Type != protocol.MsgReady {
		t.Errorf("type = %q, want %q", msg.Type, protocol.MsgReady)
	}
	if msg.SessionID != "worker-ready-nonce" {
		t.Errorf("session ID = %q, want %q", msg.SessionID, "worker-ready-nonce")
	}
	if msg.WorkerInstanceID != "nonce-123" {
		t.Errorf("worker instance ID = %q, want %q", msg.WorkerInstanceID, "nonce-123")
	}
}

func TestWorkerSendsReadyBeforeHeartbeats(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	ready := make(chan error, 1)
	cli := newDelayedReadyTestAdapter(ready)
	w := New(Options{
		SessionID:  "worker-ready-order",
		DaemonAddr: listener.Addr().String(),
		CliType:    "mock",
	})
	w.cliAdapter = cli
	w.heartbeatInterval = 20 * time.Millisecond

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()
	runExited := false

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept worker: %v", err)
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = cli.Close()
		_ = conn.Close()
		if !runExited {
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Error("worker did not exit during cleanup")
			}
		}
	})

	time.AfterFunc(6*w.heartbeatInterval, func() { ready <- nil })
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	first, readErr := protocol.NewMessageReader(conn).Read()
	if readErr != nil {
		t.Fatalf("read first worker message: %v", readErr)
	}
	if first.Type != protocol.MsgReady {
		t.Fatalf("first worker message = %s, want %s", first.Type, protocol.MsgReady)
	}

	if _, err := protocol.NewMessage(protocol.MsgClose, w.sessionID, "").WriteTo(conn); err != nil {
		t.Fatalf("send close: %v", err)
	}
	select {
	case err := <-runDone:
		runExited = true
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgClose")
	}
}

type delayedReadyTestAdapter struct {
	ready   <-chan error
	outputR *io.PipeReader
	outputW *io.PipeWriter
}

func newDelayedReadyTestAdapter(ready <-chan error) *delayedReadyTestAdapter {
	outputR, outputW := io.Pipe()
	return &delayedReadyTestAdapter{
		ready:   ready,
		outputR: outputR,
		outputW: outputW,
	}
}

func (a *delayedReadyTestAdapter) Name() string {
	return "delayed-ready-test"
}

func (a *delayedReadyTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	return &adapter.CliStartResult{
		Output:      a.outputR,
		ErrCh:       make(chan error),
		ReadyResult: a.ready,
	}, nil
}

func (a *delayedReadyTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *delayedReadyTestAdapter) Close() error {
	_ = a.outputR.Close()
	return a.outputW.Close()
}
