package daemon

import (
	"testing"
	"time"

	"botmux-go/internal/protocol"
)

func TestSessionMetaCancelTurnRequiresActiveTurn(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-codex")
	if token, ok := meta.BeginTurnCancel(); ok || token != 0 {
		t.Fatalf("idle BeginTurnCancel() = (%d, %t), want (0, false)", token, ok)
	}
	if !meta.BeginTurn() {
		t.Fatal("first turn rejected")
	}
	token, ok := meta.BeginTurnCancel()
	if !ok || token == 0 {
		t.Fatalf("BeginTurnCancel() = (%d, %t), want (non-zero, true)", token, ok)
	}
	if duplicateToken, ok := meta.BeginTurnCancel(); ok || duplicateToken != 0 {
		t.Fatalf("second BeginTurnCancel() = (%d, %t), want (0, false)", duplicateToken, ok)
	}

	snapshot := meta.TurnSnapshot()
	if !snapshot.Active || !snapshot.Cancelling || snapshot.Token != token {
		t.Fatalf("TurnSnapshot() = %#v, want active cancelling turn token %d", snapshot, token)
	}
}

func TestSessionMetaCancelTimeoutCommitsOnlyMatchingTurn(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-codex")
	if !meta.BeginTurn() {
		t.Fatal("first turn rejected")
	}
	oldToken, ok := meta.BeginTurnCancel()
	if !ok {
		t.Fatal("first turn cancellation rejected")
	}
	if !meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted}) {
		t.Fatal("first turn terminal rejected")
	}

	if !meta.BeginTurn() {
		t.Fatal("second turn rejected")
	}
	newToken, ok := meta.BeginTurnCancel()
	if !ok {
		t.Fatal("second turn cancellation rejected")
	}
	if newToken == oldToken {
		t.Fatalf("turn token did not advance: old %d, new %d", oldToken, newToken)
	}
	failed := protocol.TurnTerminal{
		Status:    protocol.TurnFailed,
		ErrorCode: "codex_cancel_timeout",
	}
	if meta.FailCancellingTurn(oldToken, failed) {
		t.Fatal("late cancellation timeout committed")
	}
	if !meta.FailCancellingTurn(newToken, failed) {
		t.Fatal("matching cancellation timeout rejected")
	}
}

func TestSessionMetaCommitsOnlyOneTerminalPerTurn(t *testing.T) {
	meta := NewSessionMeta("session-1", "bot-codex")
	if !meta.BeginTurn() {
		t.Fatal("turn rejected")
	}
	if !meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnFailed}) {
		t.Fatal("failed terminal rejected")
	}
	if meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted}) {
		t.Fatal("second terminal accepted")
	}

	terminals, _, _ := meta.SnapshotTerminalsSince(0)
	want := protocol.TurnTerminal{Status: protocol.TurnFailed}
	if len(terminals) != 1 || terminals[0] != want {
		t.Fatalf("terminals = %#v, want %#v", terminals, []protocol.TurnTerminal{want})
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
