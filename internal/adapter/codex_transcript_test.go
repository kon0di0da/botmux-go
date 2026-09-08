package adapter

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCodexTranscriptEmitsFinalOutputAfterExpectedUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	content := "" +
		`{"timestamp":"2026-09-07T10:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}` + "\n" +
		`{"timestamp":"2026-09-07T10:00:01Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","last_agent_message":"world"}}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cursor := codexTranscriptCursor{Path: path, Input: "hello"}
	got, err := cursor.ReadNew()
	if err != nil {
		t.Fatal(err)
	}
	want := []AdapterEvent{
		{Kind: AdapterOutput, Output: "world"},
		{Kind: AdapterTurnTerminal, Status: TurnCompleted},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadNew() = %#v, want %#v", got, want)
	}
}

func TestCodexTranscriptIgnoresTerminalBeforeExpectedUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	content := "" +
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"old","last_agent_message":"old answer"}}` + "\n" +
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"new prompt"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cursor := codexTranscriptCursor{Path: path, Input: "new prompt"}
	got, err := cursor.ReadNew()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("ReadNew() = %#v, want no terminal before current turn", got)
	}
}
