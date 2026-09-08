package daemon

import (
	"testing"

	"botmux-go/internal/protocol"
)

func TestSessionUpdatePersistsCodexNativeSessionID(t *testing.T) {
	store := NewSessionStore(t.TempDir())
	meta := NewSessionMeta("session-1", "bot-codex")
	if err := store.save(meta.ToPersisted()); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{store: store}

	d.routeMessage(
		protocol.NewMessage(protocol.MsgCliSessionBound, meta.SessionID, "01234567-89ab-cdef-0123-456789abcdef"),
		meta,
		nil,
	)

	if meta.CliSessionID != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("CliSessionID = %q, want native session ID", meta.CliSessionID)
	}
	persisted, err := store.load(meta.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.CliSessionID != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("persisted CliSessionID = %q, want native session ID", persisted.CliSessionID)
	}
}
