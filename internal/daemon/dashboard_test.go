package daemon

import (
	"strings"
	"testing"
)

func TestDashboardComposerSourceGuards(t *testing.T) {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`<textarea id="chat-input"`,
		`event.metaKey || event.ctrlKey`,
		`function handleComposerKey`,
		`function composerDisabled`,
		`var msg = input.value;`,
		`message: msg`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	if strings.Contains(html, `<input type="text" id="chat-input"`) {
		t.Error("dashboard still renders the legacy single-line chat input")
	}
}

func TestDashboardCancelTurnSourceGuards(t *testing.T) {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`id="chat-cancel-btn"`,
		`function cancelTurn`,
		`'/cancel'`,
		`turn_active`,
		`turn_cancelling`,
		`function updateChatControls`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
}
