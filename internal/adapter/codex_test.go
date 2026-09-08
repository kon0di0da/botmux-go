package adapter

import (
	"reflect"
	"testing"
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
