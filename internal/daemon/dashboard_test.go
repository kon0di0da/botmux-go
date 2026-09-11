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

func TestDashboardSendTurnStateSourceGuards(t *testing.T) {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	html := string(data)

	sendStart := strings.Index(html, `async function sendMessage(sid) {`)
	cancelStart := strings.Index(html, `async function cancelTurn(sid) {`)
	if sendStart < 0 || cancelStart < 0 || cancelStart <= sendStart {
		t.Fatal("dashboard missing sendMessage or cancelTurn function")
	}
	sendBody := html[sendStart:cancelStart]

	stateUpdate := `chatSessionData = Object.assign({}, chatSessionData || {}, { turn_active: true, turn_cancelling: false });`
	stateIndex := strings.Index(sendBody, stateUpdate)
	controlsIndex := strings.Index(sendBody, `updateChatControls(chatSessionData);`)
	if stateIndex < 0 {
		t.Errorf("sendMessage missing immediate chat session state update %q", stateUpdate)
	}
	if controlsIndex < 0 {
		t.Error("sendMessage missing chat controls update from chatSessionData")
	}
	if stateIndex >= 0 && controlsIndex >= 0 && stateIndex > controlsIndex {
		t.Error("sendMessage updates controls before synchronizing chatSessionData")
	}
}

func TestDashboardInlineSessionHandlerSourceGuards(t *testing.T) {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`function escapeJSString(value)`,
		`replace(/\\/g, '\\\\')`,
		`replace(/'/g, "\\'")`,
		`replace(/"/g, '\\x22')`,
		`replace(/\r/g, '\\r')`,
		`replace(/\n/g, '\\n')`,
		`replace(/</g, '\\x3c')`,
		`replace(/>/g, '\\x3e')`,
		`replace(/&/g, '\\x26')`,
		`closeSession(\'' + escapeJSString(s.session_id)`,
		`closeSessionFromChat(\'' + escapeJSString(sid)`,
		`handleComposerKey(event, \'' + escapeJSString(sid)`,
		`cancelTurn(\'' + escapeJSString(sid)`,
		`sendMessage(\'' + escapeJSString(sid)`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing inline handler guard %q", want)
		}
	}

	for _, unsafe := range []string{
		`onclick="closeSession(\'' + escapeHtml(s.session_id)`,
		`onclick="closeSessionFromChat(\'' + escapeHtml(sid)`,
		`onkeydown="handleComposerKey(event, \'' + escapeHtml(sid)`,
		`onclick="cancelTurn(\'' + escapeHtml(sid)`,
		`onclick="sendMessage(\'' + escapeHtml(sid)`,
	} {
		if strings.Contains(html, unsafe) {
			t.Errorf("dashboard uses escapeHtml in inline handler %q", unsafe)
		}
	}
}
