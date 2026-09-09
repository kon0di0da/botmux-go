package adapter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCodexBuildArgsFresh(t *testing.T) {
	a := NewCodexAdapter(AdapterOptions{
		CliType: "codex",
		Model:   "gpt-5.5",
		Profile: "arkcli",
	})
	got := a.buildArgs("/tmp/repo")
	want := []string{
		"--dangerously-bypass-approvals-and-sandbox",
		"--dangerously-bypass-hook-trust",
		"--no-alt-screen",
		"-c", "check_for_update_on_startup=false",
		"--profile", "arkcli",
		"--model", "gpt-5.5",
		"-C", "/tmp/repo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}

func TestCodexBuildArgsResumeKeepsProfileButDoesNotOverrideModel(t *testing.T) {
	a := NewCodexAdapter(AdapterOptions{
		CliType:         "codex",
		Model:           "new-default",
		Profile:         "arkcli",
		ResumeSessionID: "native-session",
	})
	got := a.buildArgs("/tmp/repo")
	want := []string{
		"resume",
		"--dangerously-bypass-approvals-and-sandbox",
		"--dangerously-bypass-hook-trust",
		"--no-alt-screen",
		"-c", "check_for_update_on_startup=false",
		"--profile", "arkcli",
		"native-session",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}

func TestCodexResumeUsesPersistedNativeSessionID(t *testing.T) {
	a := NewCodexAdapter(AdapterOptions{
		CliType:         "codex",
		ResumeSessionID: "01234567-89ab-cdef-0123-456789abcdef",
	})
	got := a.buildArgs("/tmp/repo")
	if got[0] != "resume" || got[len(got)-1] != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("resume args = %#v, want persisted native session ID", got)
	}
}

func TestCodexComposerReadyRejectsResumeLoadingScreen(t *testing.T) {
	screen := "model: loading\nResuming session… › Ask Codex to do anything"
	if codexComposerReady(screen) {
		t.Fatal("resume loading screen was treated as READY")
	}
}

func TestCodexComposerReadyAcceptsLoadedComposer(t *testing.T) {
	screen := "model: gpt-5.5\n› Ask Codex to do anything"
	if !codexComposerReady(screen) {
		t.Fatal("loaded composer was not treated as READY")
	}
}

func TestCodexInterruptWritesEsc(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	a := NewCodexAdapter(AdapterOptions{CliType: "codex"})
	a.ptmx = writer

	gotCh := make(chan []byte, 1)
	go func() {
		got := make([]byte, 1)
		if _, err := io.ReadFull(reader, got); err != nil {
			t.Errorf("read interrupt byte: %v", err)
			return
		}
		gotCh <- got
	}()

	if err := a.Interrupt(context.Background()); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}
	select {
	case got := <-gotCh:
		want := []byte{0x1b}
		if !bytes.Equal(got, want) {
			t.Fatalf("interrupt bytes = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Codex interrupt byte")
	}
}

func TestCodexInterruptPreemptsBlockedHistoryConfirmation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if err := os.WriteFile(filepath.Join(home, "history.jsonl"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	sendCtx, cancelSend := context.WithCancel(context.Background())
	sendDone := make(chan error, 1)
	interruptDone := make(chan error, 1)
	var goroutines sync.WaitGroup
	t.Cleanup(func() {
		cancelSend()
		_ = writer.Close()
		_ = reader.Close()

		goroutinesDone := make(chan struct{})
		go func() {
			goroutines.Wait()
			close(goroutinesDone)
		}()
		select {
		case <-goroutinesDone:
		case <-time.After(time.Second):
			t.Error("test goroutines did not return during cleanup")
		}
	})

	a := NewCodexAdapter(AdapterOptions{CliType: "codex"})
	a.ptmx = writer

	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		_, err := a.Send(sendCtx, "first")
		sendDone <- err
	}()

	wantInput := []byte("\x1b[200~first\x1b[201~\r")
	gotInput := make(chan []byte, 1)
	readInputErr := make(chan error, 1)
	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		got := make([]byte, len(wantInput))
		if _, err := io.ReadFull(reader, got); err != nil {
			readInputErr <- err
			return
		}
		gotInput <- got
	}()

	select {
	case got := <-gotInput:
		if !bytes.Equal(got, wantInput) {
			t.Fatalf("initial input = %q, want %q", got, wantInput)
		}
	case err := <-readInputErr:
		t.Fatalf("read initial input: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial Codex input")
	}

	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		interruptDone <- a.Interrupt(context.Background())
	}()

	gotEsc := make(chan []byte, 1)
	readEscErr := make(chan error, 1)
	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		got := make([]byte, 1)
		if _, err := io.ReadFull(reader, got); err != nil {
			readEscErr <- err
			return
		}
		gotEsc <- got
	}()

	select {
	case got := <-gotEsc:
		if !bytes.Equal(got, []byte{0x1b}) {
			t.Fatalf("interrupt byte = %#v, want Esc", got)
		}
	case err := <-readEscErr:
		t.Fatalf("read interrupt byte: %v", err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Interrupt did not preempt blocked history confirmation")
	}

	select {
	case err := <-interruptDone:
		if err != nil {
			t.Fatalf("Interrupt: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Interrupt did not return after writing Esc")
	}

	select {
	case err := <-sendDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send error = %v, want context cancellation", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Send did not return after interrupt")
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	remaining, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("writes after Esc = %#v, want none", remaining)
	}
}

func TestCodexSendUsesBracketedPasteAndReturnsConfirmedSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	historyPath := filepath.Join(home, "history.jsonl")
	if err := os.WriteFile(historyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	a := NewCodexAdapter(AdapterOptions{CliType: "codex"})
	a.ptmx = writer
	a.cmd = &exec.Cmd{Process: &os.Process{Pid: 42}}
	a.deps.ownedRollouts = func(pid int) (map[string]struct{}, error) {
		if pid != 42 {
			t.Fatalf("pid = %d, want 42", pid)
		}
		return map[string]struct{}{"owned": {}}, nil
	}

	gotWrite := make(chan []byte, 1)
	go func() {
		var data bytes.Buffer
		buf := make([]byte, 256)
		for {
			n, readErr := reader.Read(buf)
			if n > 0 {
				data.Write(buf[:n])
				if bytes.Contains(data.Bytes(), []byte{'\r'}) {
					_ = os.WriteFile(
						historyPath,
						[]byte(`{"session_id":"owned","text":"first\n你"}`+"\n"),
						0o600,
					)
					gotWrite <- append([]byte(nil), data.Bytes()...)
					return
				}
			}
			if readErr != nil {
				return
			}
		}
	}()

	result, err := a.Send(context.Background(), "first\r\n你\n")
	if err != nil {
		t.Fatal(err)
	}
	if result.CliSessionID != "owned" {
		t.Fatalf("CliSessionID = %q, want owned", result.CliSessionID)
	}
	select {
	case got := <-gotWrite:
		want := []byte("\x1b[200~first\n你\x1b[201~\r")
		if !bytes.Equal(got, want) {
			t.Fatalf("written input = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Codex input")
	}
}

func TestCodexSendWatcherEventsCarryTurnID(t *testing.T) {
	const (
		sessionID = "owned"
		turnID    = uint64(42)
	)
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	historyPath := filepath.Join(home, "history.jsonl")
	if err := os.WriteFile(historyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	rolloutDir := filepath.Join(home, "sessions", "2026", "09", "09")
	if err := os.MkdirAll(rolloutDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rolloutPath := filepath.Join(rolloutDir, "rollout-2026-09-09T00-00-00-"+sessionID+".jsonl")
	rollout := "" +
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","last_agent_message":"world"}}` + "\n"
	if err := os.WriteFile(rolloutPath, []byte(rollout), 0o600); err != nil {
		t.Fatal(err)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	a := NewCodexAdapter(AdapterOptions{CliType: "codex"})
	a.ptmx = writer
	a.cmd = &exec.Cmd{Process: &os.Process{Pid: 42}}
	a.ctx = context.Background()
	a.started = true
	a.deps.ownedRollouts = func(pid int) (map[string]struct{}, error) {
		if pid != 42 {
			t.Fatalf("pid = %d, want 42", pid)
		}
		return map[string]struct{}{sessionID: {}}, nil
	}

	inputWritten := make(chan error, 1)
	go func() {
		data := make([]byte, len("\x1b[200~hello\x1b[201~\r"))
		if _, err := io.ReadFull(reader, data); err != nil {
			inputWritten <- err
			return
		}
		inputWritten <- os.WriteFile(
			historyPath,
			[]byte(`{"session_id":"owned","text":"hello"}`+"\n"),
			0o600,
		)
	}()

	if _, err := a.Send(WithTurnID(context.Background(), turnID), "hello"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case err := <-inputWritten:
		if err != nil {
			t.Fatalf("write history: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Codex input")
	}

	var events []AdapterEvent
	for len(events) < 2 {
		select {
		case event := <-a.events:
			events = append(events, event)
		case <-time.After(time.Second):
			t.Fatalf("events = %#v, want output and terminal", events)
		}
	}
	for _, event := range events {
		if event.TurnID != turnID {
			t.Fatalf("event TurnID = %d, want %d: %#v", event.TurnID, turnID, event)
		}
	}
	if events[0].Kind != AdapterOutput || events[0].Output != "world" {
		t.Fatalf("output event = %#v", events[0])
	}
	if events[1].Kind != AdapterTurnTerminal || events[1].Status != TurnCompleted {
		t.Fatalf("terminal event = %#v", events[1])
	}
}

func TestCodexCloseReturnsWhileSendMutexIsHeld(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	a := NewCodexAdapter(AdapterOptions{CliType: "codex"})
	a.ptmx = writer
	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- a.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Close waited for send mutex")
	}

	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if !closed {
		t.Fatal("Close did not mark adapter closed")
	}

	secondCloseDone := make(chan error, 1)
	go func() {
		secondCloseDone <- a.Close()
	}()
	select {
	case err := <-secondCloseDone:
		if err != nil {
			t.Fatalf("second Close: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("idempotent Close waited for send mutex")
	}
}
