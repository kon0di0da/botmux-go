package daemon

import (
	"context"
	"net"
	"testing"
	"time"

	"botmux-go/internal/protocol"
)

func TestHandleConnDoesNotMarkWorkerReadyForNonReadyFirstMessage(t *testing.T) {
	tests := []struct {
		name    string
		msgType protocol.MessageType
		payload string
	}{
		{name: "heartbeat", msgType: protocol.MsgHeartbeat},
		{name: "output", msgType: protocol.MsgOutput, payload: "startup output"},
		{name: "error", msgType: protocol.MsgError, payload: "startup failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, handle := newReadyTestDaemon(t)
			serverConn, workerConn := net.Pipe()
			d.wg.Add(1)
			go d.handleConn(serverConn)

			if _, err := protocol.NewMessage(tt.msgType, handle.SessionID, tt.payload).WriteTo(workerConn); err != nil {
				t.Fatalf("write first worker message: %v", err)
			}

			select {
			case <-handle.Ready:
				t.Fatalf("worker became ready after first %s message", tt.msgType)
			case <-time.After(50 * time.Millisecond):
			}

			_ = workerConn.Close()
			d.wg.Wait()
		})
	}
}

func TestHandleConnMarksWorkerReadyForExplicitReadyMessage(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	serverConn, workerConn := net.Pipe()
	d.wg.Add(1)
	go d.handleConn(serverConn)

	if _, err := protocol.NewMessage(protocol.MsgReady, handle.SessionID, "ready").WriteTo(workerConn); err != nil {
		t.Fatalf("write ready message: %v", err)
	}

	select {
	case <-handle.Ready:
	case <-time.After(time.Second):
		t.Fatal("worker did not become ready after MsgReady")
	}
	if !handle.IsReady() {
		t.Fatal("worker handle did not record ready state")
	}

	_ = workerConn.Close()
	d.wg.Wait()
}

func newReadyTestDaemon(t *testing.T) (*Daemon, *WorkerHandle) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	meta := NewSessionMeta("session-ready-test", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	return &Daemon{
		ctx:        ctx,
		sessions:   map[string]*SessionMeta{meta.SessionID: meta},
		workers:    map[string]*WorkerHandle{meta.SessionID: handle},
		connToSess: make(map[net.Conn]string),
		store:      NewSessionStore(t.TempDir()),
	}, handle
}
