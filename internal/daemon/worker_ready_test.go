package daemon

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
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

func TestHandleConnRejectsReadyWithoutSessionID(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	meta := d.sessions[handle.SessionID]
	workerCount := len(d.workers)
	sessionCount := len(d.sessions)
	serverConn, workerConn := net.Pipe()
	defer func() {
		_ = workerConn.Close()
		d.wg.Wait()
	}()
	d.wg.Add(1)
	go d.handleConn(serverConn)

	msg := readyTestWorkerMessage(handle, protocol.MsgReady, "ready")
	msg.SessionID = ""
	if _, err := msg.WriteTo(workerConn); err != nil {
		t.Fatalf("write ready message: %v", err)
	}

	got := readReadyTestMessage(t, workerConn)
	if got.Type != protocol.MsgError {
		t.Fatalf("response type = %s, want %s", got.Type, protocol.MsgError)
	}
	expectReadyTestEOF(t, workerConn)

	d.workersMu.RLock()
	currentHandle := d.workers[handle.SessionID]
	owner := d.workerOwners[handle]
	_, emptyWorkerExists := d.workers[""]
	gotWorkerCount := len(d.workers)
	d.workersMu.RUnlock()
	d.sessionsMu.RLock()
	currentMeta := d.sessions[handle.SessionID]
	_, emptySessionExists := d.sessions[""]
	gotSessionCount := len(d.sessions)
	d.sessionsMu.RUnlock()
	if currentHandle != handle || owner != meta {
		t.Fatal("worker handle map changed after empty-session ready")
	}
	if currentMeta != meta {
		t.Fatal("session map changed after empty-session ready")
	}
	if gotWorkerCount != workerCount || gotSessionCount != sessionCount || emptyWorkerExists || emptySessionExists {
		t.Fatal("daemon maps changed after empty-session ready")
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

func TestHandleConnAcceptsSameWorkerReconnect(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	meta := d.sessions[handle.SessionID]

	serverConn1, workerConn1 := net.Pipe()
	d.wg.Add(1)
	go d.handleConn(serverConn1)
	if _, err := readyTestWorkerMessage(handle, protocol.MsgReady, "ready").WriteTo(workerConn1); err != nil {
		t.Fatalf("write first ready message: %v", err)
	}
	select {
	case <-handle.Ready:
	case <-time.After(time.Second):
		t.Fatal("worker did not become ready after first MsgReady")
	}
	handle.mu.Lock()
	firstConn := handle.Conn
	handle.mu.Unlock()
	if firstConn != serverConn1 {
		t.Fatal("worker handle connection was not set to first admitted connection")
	}

	if err := workerConn1.Close(); err != nil {
		t.Fatalf("close first worker peer: %v", err)
	}
	d.wg.Wait()

	serverConn2, workerConn2 := net.Pipe()
	defer func() {
		_ = workerConn2.Close()
		d.wg.Wait()
	}()
	d.wg.Add(1)
	go d.handleConn(serverConn2)
	if _, err := readyTestWorkerMessage(handle, protocol.MsgReady, "ready").WriteTo(workerConn2); err != nil {
		t.Fatalf("write reconnect ready message: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		handle.mu.Lock()
		conn := handle.Conn
		handle.mu.Unlock()
		return conn == serverConn2
	})
	if !handle.IsReady() || !isClosedChan(handle.Ready) {
		t.Fatal("worker readiness changed after matching reconnect")
	}
	d.workersMu.RLock()
	currentHandle := d.workers[handle.SessionID]
	owner := d.workerOwners[handle]
	d.workersMu.RUnlock()
	if currentHandle != handle || owner != meta {
		t.Fatal("matching reconnect replaced the expected worker handle")
	}

	beforeHeartbeat := handle.LastHb()
	if _, err := readyTestWorkerMessage(handle, protocol.MsgHeartbeat, "").WriteTo(workerConn2); err != nil {
		t.Fatalf("write reconnect heartbeat: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		return handle.LastHb().After(beforeHeartbeat)
	})
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

func TestHandleConnRejectsStaleWorkerAfterSessionReuse(t *testing.T) {
	d, oldHandle := newReadyTestDaemon(t)
	oldMeta := d.sessions[oldHandle.SessionID]
	oldHandle.InstanceID = "old-nonce"

	replacementMeta := NewSessionMeta(oldMeta.SessionID, "bot-replacement")
	replacementHandle := NewWorkerHandle(oldMeta.SessionID)
	replacementHandle.InstanceID = "new-nonce"
	replacementConn, replacementPeer := net.Pipe()
	defer replacementConn.Close()
	defer replacementPeer.Close()
	replacementHandle.SetConn(replacementConn)

	d.workersMu.Lock()
	d.sessionsMu.Lock()
	d.sessions[oldMeta.SessionID] = replacementMeta
	d.workers[oldMeta.SessionID] = replacementHandle
	delete(d.workerOwners, oldHandle)
	d.workerOwners[replacementHandle] = replacementMeta
	d.sessionsMu.Unlock()
	d.workersMu.Unlock()

	serverConn, workerConn := net.Pipe()
	defer func() {
		_ = workerConn.Close()
		d.wg.Wait()
	}()
	d.wg.Add(1)
	go d.handleConn(serverConn)
	if _, err := readyTestWorkerMessage(oldHandle, protocol.MsgReady, "ready").WriteTo(workerConn); err != nil {
		t.Fatalf("write stale ready message: %v", err)
	}

	got := readReadyTestMessage(t, workerConn)
	if got.Type != protocol.MsgError {
		t.Fatalf("response type = %s, want %s", got.Type, protocol.MsgError)
	}
	expectReadyTestEOF(t, workerConn)

	d.workersMu.RLock()
	currentHandle := d.workers[oldMeta.SessionID]
	owner := d.workerOwners[replacementHandle]
	d.workersMu.RUnlock()
	d.sessionsMu.RLock()
	currentMeta := d.sessions[oldMeta.SessionID]
	d.sessionsMu.RUnlock()
	replacementHandle.mu.Lock()
	currentConn := replacementHandle.Conn
	replacementHandle.mu.Unlock()
	if currentMeta != replacementMeta || currentHandle != replacementHandle || owner != replacementMeta {
		t.Fatal("stale ready changed replacement session ownership")
	}
	if currentConn != replacementConn {
		t.Fatal("stale ready replaced replacement worker connection")
	}
	if replacementHandle.IsReady() || isClosedChan(replacementHandle.Ready) {
		t.Fatal("stale ready marked replacement worker ready")
	}
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

func expectReadyTestEOF(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set EOF read deadline: %v", err)
	}
	if _, err := protocol.DecodeMessage(conn); !errors.Is(err, io.EOF) {
		t.Fatalf("read after error = %v, want EOF", err)
	}
}
