package daemon

import (
	"context"
	"encoding/hex"
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

			if _, err := readyTestWorkerMessage(handle, tt.msgType, tt.payload).WriteTo(workerConn); err != nil {
				t.Fatalf("write first worker message: %v", err)
			}

			got := readReadyTestMessage(t, workerConn)
			if got.Type != protocol.MsgError {
				t.Fatalf("response type = %s, want %s", got.Type, protocol.MsgError)
			}
			select {
			case <-handle.Ready:
				t.Fatalf("worker became ready after first %s message", tt.msgType)
			case <-time.After(50 * time.Millisecond):
			}
			if handle.Conn != nil {
				t.Fatal("worker handle connection changed after rejected first message")
			}

			_ = workerConn.Close()
			d.wg.Wait()
		})
	}
}

func TestHandleConnRejectsReadyWithMissingWorkerInstanceID(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	oldConn, oldPeer := net.Pipe()
	defer oldConn.Close()
	defer oldPeer.Close()
	handle.SetConn(oldConn)

	serverConn, workerConn := net.Pipe()
	d.wg.Add(1)
	go d.handleConn(serverConn)

	if _, err := protocol.NewMessage(protocol.MsgReady, handle.SessionID, "ready").WriteTo(workerConn); err != nil {
		t.Fatalf("write ready message: %v", err)
	}

	got := readReadyTestMessage(t, workerConn)
	if got.Type != protocol.MsgError {
		t.Fatalf("response type = %s, want %s", got.Type, protocol.MsgError)
	}
	if handle.Conn != oldConn {
		t.Fatal("worker handle connection was replaced after missing instance ID")
	}
	if isClosedChan(handle.Ready) {
		t.Fatal("worker became ready after missing instance ID")
	}

	_ = workerConn.Close()
	d.wg.Wait()
}

func TestHandleConnRejectsReadyWithMismatchedWorkerInstanceID(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	oldConn, oldPeer := net.Pipe()
	defer oldConn.Close()
	defer oldPeer.Close()
	handle.SetConn(oldConn)

	serverConn, workerConn := net.Pipe()
	d.wg.Add(1)
	go d.handleConn(serverConn)

	msg := protocol.NewMessage(protocol.MsgReady, handle.SessionID, "ready")
	msg.WorkerInstanceID = "other-nonce"
	if _, err := msg.WriteTo(workerConn); err != nil {
		t.Fatalf("write ready message: %v", err)
	}

	got := readReadyTestMessage(t, workerConn)
	if got.Type != protocol.MsgError {
		t.Fatalf("response type = %s, want %s", got.Type, protocol.MsgError)
	}
	if handle.Conn != oldConn {
		t.Fatal("worker handle connection was replaced after mismatched instance ID")
	}
	if isClosedChan(handle.Ready) {
		t.Fatal("worker became ready after mismatched instance ID")
	}

	_ = workerConn.Close()
	d.wg.Wait()
}

func TestHandleConnAcceptsReadyWithMatchingWorkerInstanceID(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	serverConn, workerConn := net.Pipe()
	d.wg.Add(1)
	go d.handleConn(serverConn)

	if _, err := readyTestWorkerMessage(handle, protocol.MsgReady, "ready").WriteTo(workerConn); err != nil {
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
	if handle.Conn != serverConn {
		t.Fatal("worker handle connection was not set to admitted connection")
	}

	_ = workerConn.Close()
	d.wg.Wait()
}

func TestHandleConnRejectsWorkerWithoutExpectedHandle(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	delete(d.workers, handle.SessionID)
	delete(d.workerOwners, handle)

	serverConn, workerConn := net.Pipe()
	d.wg.Add(1)
	go d.handleConn(serverConn)

	if _, err := readyTestWorkerMessage(handle, protocol.MsgReady, "ready").WriteTo(workerConn); err != nil {
		t.Fatalf("write ready message: %v", err)
	}

	got := readReadyTestMessage(t, workerConn)
	if got.Type != protocol.MsgError {
		t.Fatalf("response type = %s, want %s", got.Type, protocol.MsgError)
	}
	if _, ok := d.workers[handle.SessionID]; ok {
		t.Fatal("unexpected worker handle was created")
	}
	if isClosedChan(handle.Ready) {
		t.Fatal("removed worker handle became ready")
	}

	_ = workerConn.Close()
	d.wg.Wait()
}

func TestNewWorkerInstanceIDIsRandomHex(t *testing.T) {
	first, err := newWorkerInstanceID()
	if err != nil {
		t.Fatalf("generate first worker instance ID: %v", err)
	}
	second, err := newWorkerInstanceID()
	if err != nil {
		t.Fatalf("generate second worker instance ID: %v", err)
	}
	for _, id := range []string{first, second} {
		if id == "" {
			t.Fatal("worker instance ID is empty")
		}
		if len(id) != 32 {
			t.Fatalf("worker instance ID length = %d, want 32", len(id))
		}
		if _, err := hex.DecodeString(id); err != nil {
			t.Fatalf("worker instance ID is not hex: %q: %v", id, err)
		}
	}
	if first == second {
		t.Fatal("worker instance IDs unexpectedly match")
	}
}

func newReadyTestDaemon(t *testing.T) (*Daemon, *WorkerHandle) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	meta := NewSessionMeta("session-ready-test", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	handle.InstanceID = "expected-nonce"
	return &Daemon{
		ctx:          ctx,
		sessions:     map[string]*SessionMeta{meta.SessionID: meta},
		workers:      map[string]*WorkerHandle{meta.SessionID: handle},
		workerOwners: map[*WorkerHandle]*SessionMeta{handle: meta},
		connToSess:   make(map[net.Conn]string),
		store:        NewSessionStore(t.TempDir()),
	}, handle
}

func readyTestWorkerMessage(handle *WorkerHandle, typ protocol.MessageType, payload string) *protocol.Message {
	msg := protocol.NewMessage(typ, handle.SessionID, payload)
	msg.WorkerInstanceID = handle.InstanceID
	return msg
}

func readReadyTestMessage(t *testing.T, conn net.Conn) *protocol.Message {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	msg, err := protocol.DecodeMessage(conn)
	if err != nil {
		t.Fatalf("read daemon response: %v", err)
	}
	return msg
}
