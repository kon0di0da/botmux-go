package daemon

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"botmux-go/internal/protocol"
)

func TestSnapshotOutputSinceReturnsOnlyNewOutput(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-1")
	meta.AddOutput("old output")
	cursor := meta.OutputCursor()

	meta.AddOutput("first new output")
	meta.AddOutput("second new output")

	got, next, _ := meta.SnapshotOutputSince(cursor)
	want := []string{"first new output", "second new output"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SnapshotOutputSince() = %#v, want %#v", got, want)
	}
	if wantNext := cursor + uint64(len(want)); next != wantNext {
		t.Fatalf("next cursor = %d, want %d", next, wantNext)
	}
}

func TestSnapshotOutputSinceSurvivesRingBufferTruncation(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-1")
	cursor := meta.OutputCursor()

	for i := 0; i < maxMemoryOutputLines+2; i++ {
		meta.AddOutput(fmt.Sprintf("line-%d", i))
	}

	got, next, _ := meta.SnapshotOutputSince(cursor)
	if len(got) != maxMemoryOutputLines {
		t.Fatalf("got %d lines, want %d", len(got), maxMemoryOutputLines)
	}
	if got[0] != "line-2" {
		t.Fatalf("first retained line = %q, want %q", got[0], "line-2")
	}
	if wantNext := cursor + maxMemoryOutputLines + 2; next != uint64(wantNext) {
		t.Fatalf("next cursor = %d, want %d", next, wantNext)
	}
}

func TestOutputSubscriptionBroadcastsAndRearmsOnNewOutput(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-1")
	_, first := meta.OutputSubscription()
	_, second := meta.OutputSubscription()

	meta.AddOutput("new output")

	assertClosed(t, first, "first subscriber")
	assertClosed(t, second, "second subscriber")

	_, next := meta.OutputSubscription()
	select {
	case <-next:
		t.Fatal("next notification channel is already closed")
	default:
	}
}

func TestForwardClientMessageSkipsOutputBeforeSubscription(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-1")
	meta.AddOutput("old output")

	workerConn, workerPeer := net.Pipe()
	defer workerConn.Close()
	defer workerPeer.Close()
	handle := NewWorkerHandle(meta.SessionID)
	handle.SetConn(workerConn)
	handle.markReady()

	d := &Daemon{
		sessions: map[string]*SessionMeta{meta.SessionID: meta},
		workers:  map[string]*WorkerHandle{meta.SessionID: handle},
	}

	workerInput := make(chan *protocol.Message, 1)
	go func() {
		msg, _ := protocol.NewMessageReader(workerPeer).Read()
		workerInput <- msg
	}()

	clientConn, clientPeer := net.Pipe()
	defer clientConn.Close()
	defer clientPeer.Close()

	d.forwardClientMessage(clientConn, protocol.NewMessage(
		protocol.MsgUserInput,
		meta.SessionID,
		"hello",
	))

	if msg := <-workerInput; msg == nil || msg.Payload != "hello" {
		t.Fatalf("worker input = %#v, want payload %q", msg, "hello")
	}

	if err := clientPeer.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if msg, err := protocol.NewMessageReader(clientPeer).Read(); err == nil {
		t.Fatalf("received pre-subscription output: %#v", msg)
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("read before new output: %v, want timeout", err)
	}

	if err := clientPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	meta.AddOutput("new output")
	msg, err := protocol.NewMessageReader(clientPeer).Read()
	if err != nil {
		t.Fatalf("read new output: %v", err)
	}
	if msg.Payload != "new output" {
		t.Fatalf("new output payload = %q, want %q", msg.Payload, "new output")
	}
}

func assertClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("%s was not notified", name)
	}
}
