package adapter

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCodexAdapterFakeProcessCompletesMultilineTurn(t *testing.T) {
	fakeBin := buildFakeCodex(t)
	home := t.TempDir()
	argvPath := filepath.Join(home, "argv.txt")
	t.Setenv("CODEX_HOME", home)
	t.Setenv("FAKE_CODEX_ARGV_PATH", argvPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := NewCodexAdapter(AdapterOptions{
		CliType: "codex",
		CliPath: fakeBin,
		Model:   "model-a",
		Profile: "arkcli",
	})
	start, err := a.Start(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	go io.Copy(io.Discard, start.Output)

	select {
	case err := <-start.ReadyResult:
		if err != nil {
			t.Fatalf("ready: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fake Codex did not reach READY")
	}
	result, err := a.Send(ctx, "line one\nline two")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.CliSessionID != fakeCodexSessionID {
		t.Fatalf("CliSessionID = %q, want %q", result.CliSessionID, fakeCodexSessionID)
	}

	var events []AdapterEvent
	deadline := time.After(3 * time.Second)
	for len(events) < 2 {
		select {
		case event := <-start.Events:
			events = append(events, event)
		case <-deadline:
			t.Fatalf("events = %#v, want output and terminal", events)
		}
	}
	if events[0] != (AdapterEvent{Kind: AdapterOutput, Output: "fake final: line one\nline two"}) {
		t.Fatalf("output event = %#v", events[0])
	}
	if events[1].Kind != AdapterTurnTerminal || events[1].Status != TurnCompleted {
		t.Fatalf("terminal event = %#v", events[1])
	}

	history, err := os.ReadFile(filepath.Join(home, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(history), "line one\\nline two") != 1 {
		t.Fatalf("history does not contain one normalized multiline input: %s", history)
	}
	argv, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"--profile", "arkcli", "--model", "model-a"} {
		if !strings.Contains(string(argv), value) {
			t.Fatalf("fresh argv %q missing %q", argv, value)
		}
	}
}

func TestCodexAdapterFakeProcessResumeKeepsProfileButOmitsModel(t *testing.T) {
	fakeBin := buildFakeCodex(t)
	home := t.TempDir()
	argvPath := filepath.Join(home, "argv.txt")
	t.Setenv("CODEX_HOME", home)
	t.Setenv("FAKE_CODEX_ARGV_PATH", argvPath)

	a := NewCodexAdapter(AdapterOptions{
		CliType:         "codex",
		CliPath:         fakeBin,
		Model:           "new-default",
		Profile:         "arkcli",
		ResumeSessionID: fakeCodexSessionID,
	})
	start, err := a.Start(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	go io.Copy(io.Discard, start.Output)
	select {
	case err := <-start.ReadyResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fake Codex did not reach READY")
	}
	argv, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(argv)
	for _, value := range []string{"resume", fakeCodexSessionID} {
		if !strings.Contains(got, value) {
			t.Fatalf("resume argv %q missing %q", got, value)
		}
	}
	for _, required := range []string{"--profile", "arkcli"} {
		if !strings.Contains(got, required) {
			t.Fatalf("resume argv %q missing %q", got, required)
		}
	}
	for _, forbidden := range []string{"--model", "new-default"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("resume argv unexpectedly contains %q: %q", forbidden, got)
		}
	}
}

const fakeCodexSessionID = "01234567-89ab-cdef-0123-456789abcdef"

func buildFakeCodex(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "fake-codex")
	source := filepath.Join("..", "..", "testdata", "fake-codex")
	cmd := exec.Command("go", "build", "-o", bin, source)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fake Codex: %v\n%s", err, output)
	}
	return bin
}
