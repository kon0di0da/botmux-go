package adapter

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

func TestCodexBuildArgsResumeDoesNotOverrideModelOrProfile(t *testing.T) {
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
		"native-session",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
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
