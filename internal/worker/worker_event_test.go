package worker

import (
	"net"
	"testing"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/protocol"
)

func TestStructuredAdapterOutputUsesProtocolOutput(t *testing.T) {
	w := New(Options{CliType: "mock", SessionID: "session-1"})
	defer w.Cancel()
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	w.conn = conn
	events := make(chan adapter.AdapterEvent, 1)
	w.startResult = &adapter.CliStartResult{Events: events, StructuredOutput: true}

	w.wg.Add(1)
	go w.readAdapterEvents()
	events <- adapter.AdapterEvent{Kind: adapter.AdapterOutput, Output: "final answer"}

	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	msg, err := protocol.NewMessageReader(peer).Read()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Type != protocol.MsgOutput || msg.Payload != "final answer" {
		t.Fatalf("message = %#v, want output final answer", msg)
	}
	w.Cancel()
	w.wg.Wait()
}
