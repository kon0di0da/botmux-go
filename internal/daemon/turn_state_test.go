package daemon

import (
	"testing"
	"time"

	"botmux-go/internal/protocol"
)

func TestSessionMetaAllowsOneActiveTurn(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-codex")
	if !meta.BeginTurn() {
		t.Fatal("first turn rejected")
	}
	if meta.BeginTurn() {
		t.Fatal("concurrent turn accepted")
	}
	meta.FinishTurn()
	if !meta.BeginTurn() {
		t.Fatal("turn rejected after finish")
	}
}

func TestSessionMetaPublishesTerminalToSubscribers(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-codex")
	cursor, notify := meta.TerminalSubscription()
	want := protocol.TurnTerminal{Status: protocol.TurnCompleted}
	meta.PublishTerminal(want)

	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("terminal subscriber was not notified")
	}
	got, next, nextNotify := meta.SnapshotTerminalsSince(cursor)
	if len(got) != 1 || got[0] != want {
		t.Fatalf("terminals = %#v, want %#v", got, []protocol.TurnTerminal{want})
	}
	if next != cursor+1 {
		t.Fatalf("cursor = %d, want %d", next, cursor+1)
	}
	select {
	case <-nextNotify:
		t.Fatal("new terminal subscription is already closed")
	default:
	}
}
