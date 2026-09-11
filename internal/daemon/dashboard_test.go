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
	controlsIndex := -1
	if stateIndex >= 0 {
		controlsIndex = strings.Index(sendBody[stateIndex:], `updateChatControls(chatSessionData, sid);`)
	}
	if stateIndex < 0 {
		t.Errorf("sendMessage missing immediate chat session state update %q", stateUpdate)
	}
	if controlsIndex < 0 {
		t.Error("sendMessage missing chat controls update after synchronizing chatSessionData")
	}
}

func TestDashboardSessionScopedAsyncSourceGuards(t *testing.T) {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`var chatSendInFlight = Object.create(null);`,
		`function isCurrentChatSession(sid) {`,
		`function focusChatComposer(sid) {`,
		`function composerDisabled(s, sid) {`,
		`chatSendInFlight[sid] = true;`,
		`delete chatSendInFlight[sid];`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	if strings.Contains(html, `var chatSendInFlight = false;`) {
		t.Error("dashboard still uses a global chat send in-flight flag")
	}

	currentStart := strings.Index(html, `function isCurrentChatSession(sid) {`)
	focusStart := strings.Index(html, `function focusChatComposer(sid) {`)
	composerStart := strings.Index(html, `function composerDisabled(s, sid) {`)
	controlsStart := strings.Index(html, `function updateChatControls(s, sid) {`)
	if currentStart < 0 || focusStart < 0 || composerStart < 0 || controlsStart < 0 {
		t.Fatal("dashboard missing session-scoped chat helpers")
	}
	currentBody := html[currentStart:focusStart]
	for _, want := range []string{`resolveRoute()`, `renderedSessionId === sid`, `currentSessionId === sid`} {
		if !strings.Contains(currentBody, want) {
			t.Errorf("isCurrentChatSession missing %q", want)
		}
	}
	focusEnd := strings.Index(html[focusStart:], `function handleComposerKey`)
	if focusEnd < 0 {
		t.Fatal("dashboard missing handleComposerKey after focusChatComposer")
	}
	focusBody := html[focusStart : focusStart+focusEnd]
	for _, want := range []string{
		`if (!isCurrentChatSession(sid)) return;`,
		`if (input && !input.disabled) input.focus();`,
	} {
		if !strings.Contains(focusBody, want) {
			t.Errorf("focusChatComposer missing %q", want)
		}
	}
	if composerStart < 0 || controlsStart < 0 || controlsStart <= composerStart {
		t.Fatal("dashboard missing composerDisabled or updateChatControls")
	}
	composerBody := html[composerStart:controlsStart]
	if !strings.Contains(composerBody, `!!chatSendInFlight[sid]`) {
		t.Error("composerDisabled does not use the session-scoped in-flight state")
	}
	controlsEnd := strings.Index(html[controlsStart:], `function handleComposerKey`)
	if controlsEnd < 0 {
		t.Fatal("dashboard missing handleComposerKey after updateChatControls")
	}
	controlsBody := html[controlsStart : controlsStart+controlsEnd]
	for _, want := range []string{
		`if (sid && !isCurrentChatSession(sid)) return;`,
		`composerDisabled(s, sid)`,
	} {
		if !strings.Contains(controlsBody, want) {
			t.Errorf("updateChatControls missing %q", want)
		}
	}

	sendStart := strings.Index(html, `async function sendMessage(sid) {`)
	cancelStart := strings.Index(html, `async function cancelTurn(sid) {`)
	if sendStart < 0 || cancelStart < 0 || cancelStart <= sendStart {
		t.Fatal("dashboard missing sendMessage or cancelTurn function")
	}
	sendBody := html[sendStart:cancelStart]

	setInFlight := strings.Index(sendBody, `chatSendInFlight[sid] = true;`)
	postRequest := strings.Index(sendBody, `'/send', { method: 'POST', body: { message: msg }`)
	if setInFlight < 0 {
		t.Error("sendMessage does not mark a validated send as in flight for its session")
	}
	if postRequest < 0 {
		t.Error("sendMessage missing send API request")
	}
	if setInFlight >= 0 && postRequest >= 0 && setInFlight > postRequest {
		t.Error("sendMessage marks the request in flight after starting the POST")
	}
	finallyStart := strings.Index(sendBody, `finally {`)
	clearInFlight := -1
	controlsAfterClear := -1
	if finallyStart >= 0 {
		clearInFlight = strings.Index(sendBody[finallyStart:], `delete chatSendInFlight[sid];`)
		if clearInFlight >= 0 {
			clearInFlight += finallyStart
			controlsAfterClear = strings.Index(sendBody[clearInFlight:], `updateChatControls(chatSessionData, sid);`)
		}
	}
	if finallyStart < 0 || clearInFlight < 0 || controlsAfterClear < 0 {
		t.Error("sendMessage does not clear the in-flight state and refresh controls after the request settles")
	}
	if strings.Count(sendBody, `if (!isCurrentChatSession(sid))`) < 3 {
		t.Error("sendMessage does not guard async callbacks against stale chat routes")
	}
	focusIndex := strings.LastIndex(sendBody, `focusChatComposer(sid);`)
	finalControlsIndex := strings.LastIndex(sendBody, `updateChatControls(chatSessionData, sid);`)
	if focusIndex < 0 || finalControlsIndex < 0 || focusIndex < finalControlsIndex {
		t.Error("sendMessage does not restore focus after its final control update")
	}

	cancelEnd := strings.Index(html[cancelStart:], `async function closeSession(sid) {`)
	if cancelEnd < 0 {
		t.Fatal("dashboard missing closeSession after cancelTurn")
	}
	cancelBody := html[cancelStart : cancelStart+cancelEnd]
	if strings.Count(cancelBody, `if (!isCurrentChatSession(sid))`) < 2 {
		t.Error("cancelTurn does not guard request callbacks against stale chat routes")
	}
	if !strings.Contains(cancelBody, `updateChatControls(chatSessionData, sid);`) {
		t.Error("cancelTurn does not pass its session ID when updating controls")
	}

	pollStart := strings.Index(html, `async function poll() {`)
	pollEnd := strings.Index(html[pollStart:], `function updateTopbar()`)
	if pollStart < 0 || pollEnd < 0 {
		t.Fatal("dashboard missing poll or updateTopbar")
	}
	pollBody := html[pollStart : pollStart+pollEnd]
	for _, want := range []string{
		`if (!isCurrentChatSession(sid)) return;`,
		`updateChatControls(chatSessionData, sid);`,
	} {
		if !strings.Contains(pollBody, want) {
			t.Errorf("poll missing %q", want)
		}
	}
}

func TestDashboardSidebarSessionHandlerSourceGuards(t *testing.T) {
	data, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`function openChatSession(sid) {`,
		`location.hash = '#/session/' + encodeURIComponent(sid);`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing sidebar navigation helper %q", want)
		}
	}

	sidebarStart := strings.Index(html, `function buildSidebarHtml(activeSid) {`)
	headerStart := strings.Index(html, `function updateChatHeader(sid, s) {`)
	if sidebarStart < 0 || headerStart < 0 || headerStart <= sidebarStart {
		t.Fatal("dashboard missing buildSidebarHtml or updateChatHeader")
	}
	sidebarBody := html[sidebarStart:headerStart]
	if !strings.Contains(sidebarBody, `openChatSession(\'' + escapeJSString(item.session_id)`) {
		t.Error("sidebar does not escape the session ID for openChatSession")
	}
	if strings.Contains(sidebarBody, `onclick="location.hash=`) {
		t.Error("sidebar still interpolates the session ID in a location.hash inline handler")
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
