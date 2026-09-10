package worker

import (
	"net"
	"testing"

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
