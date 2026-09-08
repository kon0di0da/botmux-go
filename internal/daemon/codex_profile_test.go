package daemon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"botmux-go/internal/config"
)

func TestListCodexProfilesReturnsNamesOnly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if err := os.WriteFile(filepath.Join(home, "arkcli.config.toml"), []byte("model_provider = \"secret\""), 0o600); err != nil {
		t.Fatal(err)
	}

	d := &Daemon{}
	req := httptest.NewRequest(http.MethodGet, "/api/codex/profiles", nil)
	rec := httptest.NewRecorder()
	d.handleListCodexProfiles(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got struct {
		Profiles []string `json:"profiles"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Profiles) != 1 || got.Profiles[0] != "arkcli" {
		t.Fatalf("profiles = %#v, want [arkcli]", got.Profiles)
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("response exposes profile content: %s", rec.Body.String())
	}
}

func TestNewCodexSessionRejectsMissingProfileBeforeSpawn(t *testing.T) {
	t.Setenv("CODEX_HOME", t.TempDir())
	d := &Daemon{
		cfg: &config.DaemonConfig{
			Bots: []config.BotConfig{{
				BotID:      "bot-codex",
				CliType:    config.CliCodex,
				WorkingDir: t.TempDir(),
			}},
		},
		sessions: make(map[string]*SessionMeta),
		workers:  make(map[string]*WorkerHandle),
	}

	if _, err := d.NewSession(NewSessionOpts{
		SessionID:    "session-1",
		BotID:        "bot-codex",
		CodexProfile: "missing",
	}); err == nil {
		t.Fatal("NewSession accepted missing Codex profile")
	}
	if len(d.sessions) != 0 {
		t.Fatalf("sessions = %#v, want no session", d.sessions)
	}
}
