package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionStoreSaveLoadRoundTripPreservesModel(t *testing.T) {
	store := NewSessionStore(t.TempDir())
	createdAt := time.Now().Add(-time.Hour).Round(0)
	lastActive := time.Now().Add(-time.Minute).Round(0)
	want := &PersistedSession{
		SessionID:    "session-1",
		BotID:        "bot-1",
		CliType:      "aiden",
		CliPath:      "/usr/local/bin/aiden",
		Model:        "gpt-5.5",
		CodexProfile: "arkcli",
		CliSessionID: "native-codex-session",
		WorkingDir:   "/tmp/workspace",
		WorkerPID:    1234,
		LastOutput:   []string{"line-1", "line-2"},
		LastActive:   lastActive,
		CreatedAt:    createdAt,
	}

	if err := store.save(want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.load(want.SessionID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.Model != want.Model {
		t.Fatalf("Model = %q, want %q", got.Model, want.Model)
	}
	if got.CodexProfile != want.CodexProfile {
		t.Fatalf("CodexProfile = %q, want %q", got.CodexProfile, want.CodexProfile)
	}
	if got.CliSessionID != want.CliSessionID {
		t.Fatalf("CliSessionID = %q, want %q", got.CliSessionID, want.CliSessionID)
	}
	if got.WorkerPID != want.WorkerPID {
		t.Fatalf("WorkerPID = %d, want %d", got.WorkerPID, want.WorkerPID)
	}
	if len(got.LastOutput) != len(want.LastOutput) || got.LastOutput[1] != want.LastOutput[1] {
		t.Fatalf("LastOutput = %#v, want %#v", got.LastOutput, want.LastOutput)
	}
	if !got.CreatedAt.Equal(createdAt) {
		t.Fatalf("CreatedAt = %s, want %s", got.CreatedAt, createdAt)
	}
	if _, err := os.Stat(filepath.Join(store.dir, want.SessionID+".json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary save file still exists or stat failed: %v", err)
	}
}

func TestSessionStoreUpdateOutputKeepsLatestPersistedLines(t *testing.T) {
	store := NewSessionStore(t.TempDir())
	sessionID := "session-output"
	initialOutput := make([]string, 0, maxPersistedOutputLines+1)
	for i := 0; i < maxPersistedOutputLines+1; i++ {
		initialOutput = append(initialOutput, fmt.Sprintf("line-%d", i))
	}
	if err := store.save(&PersistedSession{
		SessionID:  sessionID,
		BotID:      "bot-1",
		LastOutput: initialOutput,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := store.UpdateOutput(sessionID, fmt.Sprintf("line-%d", maxPersistedOutputLines+1)); err != nil {
		t.Fatalf("UpdateOutput: %v", err)
	}

	got, err := store.load(sessionID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.LastOutput) != maxPersistedOutputLines {
		t.Fatalf("LastOutput length = %d, want %d", len(got.LastOutput), maxPersistedOutputLines)
	}
	if got.LastOutput[0] != "line-2" {
		t.Fatalf("first retained output = %q, want %q", got.LastOutput[0], "line-2")
	}
	if got.LastOutput[len(got.LastOutput)-1] != fmt.Sprintf("line-%d", maxPersistedOutputLines+1) {
		t.Fatalf("last retained output = %q", got.LastOutput[len(got.LastOutput)-1])
	}
	if got.LastActive.IsZero() {
		t.Fatal("LastActive was not updated")
	}
}

func TestSessionStoreListSkipsClosedInvalidAndNonJSONEntries(t *testing.T) {
	dir := t.TempDir()
	store := NewSessionStore(dir)
	if err := store.save(&PersistedSession{SessionID: "open", BotID: "bot-1"}); err != nil {
		t.Fatalf("save open: %v", err)
	}
	if err := store.save(&PersistedSession{SessionID: "closed", BotID: "bot-1", Closed: true}); err != nil {
		t.Fatalf("save closed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "invalid.json"), []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignored.tmp"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write ignored tmp: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "nested.json"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	got, err := store.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].SessionID != "open" {
		t.Fatalf("List() = %#v, want only open session", got)
	}
}

func TestSessionStoreMarkClosedAndRemove(t *testing.T) {
	store := NewSessionStore(t.TempDir())
	sessionID := "session-close"
	if err := store.save(&PersistedSession{SessionID: sessionID, BotID: "bot-1"}); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := store.MarkClosed(sessionID); err != nil {
		t.Fatalf("MarkClosed: %v", err)
	}
	closed, err := store.load(sessionID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !closed.Closed {
		t.Fatal("Closed = false, want true")
	}
	if closed.LastActive.IsZero() {
		t.Fatal("LastActive was not updated")
	}

	if err := store.remove(sessionID); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := store.load(sessionID); !os.IsNotExist(err) {
		t.Fatalf("load after remove error = %v, want not exist", err)
	}
	if err := store.remove(sessionID); err != nil {
		t.Fatalf("second remove: %v", err)
	}
}
