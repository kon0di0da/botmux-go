package adapter

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestMatchCodexHistoryDelta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	old := `{"session_id":"old","text":"old"}` + "\n"
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	baseline := int64(len(old))
	appendCodexHistory(t, path,
		`{"session_id":"foreign","text":"same"}`+"\n"+
			`{"session_id":"owned","text":"first\r\nsecond"}`+"\n")

	got, ok := matchCodexHistoryDelta(
		path, baseline, "first\nsecond",
		func(id string) bool { return id == "owned" },
	)
	if !ok || got != "owned" {
		t.Fatalf("got (%q,%v), want owned,true", got, ok)
	}
}

func TestMatchCodexHistoryDeltaIgnoresPartialRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	const partial = `{"session_id":"owned","text":"hello"`
	if err := os.WriteFile(path, []byte(partial), 0o600); err != nil {
		t.Fatal(err)
	}

	if got, ok := matchCodexHistoryDelta(path, 0, "hello", nil); ok || got != "" {
		t.Fatalf("got (%q,%v), want empty,false", got, ok)
	}
}

func TestMatchCodexHistoryDeltaRequiresNewlineTerminator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	if err := os.WriteFile(path, []byte(`{"session_id":"owned","text":"hello"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if got, ok := matchCodexHistoryDelta(path, 0, "hello", nil); ok || got != "" {
		t.Fatalf("got (%q,%v), want empty,false", got, ok)
	}
}

func appendCodexHistory(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := io.WriteString(f, content); err != nil {
		t.Fatal(err)
	}
}
