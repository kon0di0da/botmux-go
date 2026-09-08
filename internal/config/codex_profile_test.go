package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiscoverCodexProfiles(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	for _, name := range []string{
		"config.toml",
		"arkcli.config.toml",
		"staging_1.config.toml",
		"config.toml.bak",
		"bad.name.config.toml",
	} {
		if err := os.WriteFile(filepath.Join(home, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := DiscoverCodexProfiles()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"arkcli", "staging_1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DiscoverCodexProfiles() = %#v, want %#v", got, want)
	}
}

func TestValidateCodexProfileRejectsTraversalAndMissingFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if _, err := ValidateCodexProfile("../arkcli"); err == nil {
		t.Fatal("traversal profile accepted")
	}
	if _, err := ValidateCodexProfile("missing"); err == nil {
		t.Fatal("missing profile accepted")
	}
}

func TestValidateCodexProfileReturnsExistingName(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if err := os.WriteFile(filepath.Join(home, "arkcli.config.toml"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := ValidateCodexProfile("arkcli")
	if err != nil {
		t.Fatal(err)
	}
	if got != "arkcli" {
		t.Fatalf("ValidateCodexProfile() = %q, want arkcli", got)
	}
}
