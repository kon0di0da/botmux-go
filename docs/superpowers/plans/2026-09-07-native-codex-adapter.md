# Native Codex Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a native Codex CLI adapter with real readiness, verified multiline submission, rollout-based final output, explicit turn completion, and exact session resume.

**Architecture:** `CodexAdapter` owns the PTY, readiness detection, history confirmation, and rollout watcher. Worker translates structured adapter events into protocol messages; Daemon serializes one active turn per session, persists the native Codex session ID, and broadcasts output plus terminal events to the waiting CLI.

**Tech Stack:** Go 1.23, `creack/pty/v2`, JSON Lines, TCP JSON protocol, Go standard library tests.

---

## File Map

### New files

- `internal/adapter/codex.go`: process lifecycle, argv, readiness, input orchestration.
- `internal/adapter/codex_history.go`: append-only history matching and PID ownership proof.
- `internal/adapter/codex_transcript.go`: rollout discovery, cursor reader, event parser.
- `internal/adapter/codex_test.go`: launch, readiness, and input behavior.
- `internal/adapter/codex_history_test.go`: history and ownership tests.
- `internal/adapter/codex_transcript_test.go`: real-shape JSONL parser tests.
- `internal/worker/codex_flow_test.go`: Worker event and terminal forwarding tests.
- `internal/daemon/turn_state_test.go`: active-turn gate and terminal broadcast tests.
- `internal/daemon/session_resume_test.go`: native session persistence and worker env tests.
- `testdata/fake-codex/main.go`: deterministic fake Codex executable for integration tests.
- `docs/versions/v6-architecture.md`: native Codex milestone documentation.

### Modified files

- `internal/adapter/adapter.go`: structured start, send result, event contracts.
- `internal/adapter/mock.go`: return empty `SendResult`.
- `internal/adapter/pty.go`: return empty `SendResult`.
- `internal/adapter/tmux.go`: return empty `SendResult`.
- `internal/adapter/aiden.go`: return empty `SendResult`; restore native Aiden argv.
- `internal/adapter/aiden_test.go`: update signatures and native argv assertion.
- `internal/worker/worker.go`: authoritative READY, adapter event pump, session binding.
- `internal/protocol/protocol.go`: session-bound and turn-completed messages.
- `internal/daemon/session_meta.go`: turn gate, terminal broadcast, native session ID.
- `internal/daemon/session_store.go`: persist native session ID.
- `internal/daemon/daemon.go`: route new worker events, forward terminal to client.
- `cmd/daemon/main.go`: pass resume ID to Worker and exit send on terminal.
- `cmd/daemon/main_test.go`: terminal-driven CLI exit.
- `configs/bots.json`: native Codex bot configuration.
- `docs/README.md`: V6 entry and status.

## Task 0: Establish a Clean V5 Baseline

The current worktree contains the completed V5 implementation but it is not
committed. Codex changes touch the same shared files, so checkpoint V5 before
starting C1. Do not include `docs/.DS_Store`.

- [ ] **Step 1: Review the exact V5 worktree**

Run:

```bash
git status --short
git diff --check
git diff --stat
```

Expected: only the known V5 files are modified/untracked; no unrelated user
files are staged.

- [ ] **Step 2: Run the V5 verification baseline**

Run:

```bash
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

Expected: all commands exit `0`.

- [ ] **Step 3: Remove generated Finder metadata**

Run:

```bash
rm -f docs/.DS_Store
```

Expected: `docs/.DS_Store` no longer appears in `git status --short`.

- [ ] **Step 4: Commit only the V5 implementation**

Run:

```bash
git add \
  cmd/daemon/main.go cmd/daemon/main_test.go \
  configs/bots.json docs/README.md docs/versions/v5-architecture.md \
  internal/adapter/adapter.go internal/adapter/mock.go \
  internal/adapter/pty.go internal/adapter/tmux.go \
  internal/adapter/aiden.go internal/adapter/aiden_test.go \
  internal/config/config.go \
  internal/daemon/daemon.go internal/daemon/session_meta.go \
  internal/daemon/session_monitor.go internal/daemon/session_store.go \
  internal/daemon/worker_handle.go \
  internal/daemon/session_meta_test.go \
  internal/daemon/session_monitor_test.go \
  internal/daemon/worker_ready_test.go \
  internal/worker/worker.go internal/worker/output_idle_observer.go \
  internal/worker/output_idle_observer_test.go \
  internal/worker/tui_screen.go internal/worker/tui_screen_test.go \
  scripts/v5_aiden_e2e.sh
git diff --cached --check
git commit -m "feat(v5): stabilize Aiden and output delivery"
```

Expected: the pre-existing V5 changes are isolated from subsequent Codex
commits.

## Task 1: Add Structured Adapter Contracts

**Files:**

- Modify: `internal/adapter/adapter.go`
- Modify: `internal/adapter/mock.go`
- Modify: `internal/adapter/pty.go`
- Modify: `internal/adapter/tmux.go`
- Modify: `internal/adapter/aiden.go`
- Modify: `internal/adapter/aiden_test.go`
- Create: `internal/adapter/adapter_test.go`

- [ ] **Step 1: Write the failing contract test**

Add:

```go
func TestLegacyAdapterReturnsEmptySendResult(t *testing.T) {
	a := NewMockAdapter("mock")
	start, err := a.Start(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	go io.Copy(io.Discard, start.Output)
	result, err := a.Send(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if result.CliSessionID != "" {
		t.Fatalf("CliSessionID = %q, want empty", result.CliSessionID)
	}
}
```

- [ ] **Step 2: Run the test and confirm the signature failure**

Run:

```bash
go test ./internal/adapter -run TestLegacyAdapterReturnsEmptySendResult -count=1
```

Expected: compile failure because `Send` does not return `SendResult`.

- [ ] **Step 3: Add the shared contracts**

In `internal/adapter/adapter.go`, add:

```go
type TurnStatus string

const (
	TurnCompleted TurnStatus = "completed"
	TurnAborted   TurnStatus = "aborted"
	TurnFailed    TurnStatus = "failed"
)

type AdapterEventKind string

const (
	AdapterOutput       AdapterEventKind = "output"
	AdapterTurnTerminal AdapterEventKind = "turn_terminal"
)

type AdapterEvent struct {
	Kind        AdapterEventKind
	Output      string
	Status      TurnStatus
	ErrorCode   string
	ErrorDetail string
}

type SendResult struct {
	CliSessionID string
}

type CliStartResult struct {
	Input            io.WriteCloser
	Output           io.ReadCloser
	ErrCh            <-chan error
	ReadyResult      <-chan error
	Events           <-chan AdapterEvent
	StructuredOutput bool
	ReadyDelay       time.Duration
}

type CliAdapter interface {
	Start(ctx context.Context, workingDir string) (*CliStartResult, error)
	Send(ctx context.Context, input string) (SendResult, error)
	Close() error
	Name() string
}
```

Change each legacy adapter success return to:

```go
return SendResult{}, err
```

Change Aiden call sites and tests from:

```go
if err := a.Send(ctx, input); err != nil {
```

to:

```go
if _, err := a.Send(ctx, input); err != nil {
```

- [ ] **Step 4: Run adapter and full tests**

Run:

```bash
gofmt -w internal/adapter
go test ./internal/adapter -count=1
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter
git commit -m "refactor(adapter): add structured turn contracts"
```

## Task 2: Implement Native Codex Launch and Real READY

**Files:**

- Create: `internal/adapter/codex.go`
- Create: `internal/adapter/codex_test.go`
- Modify: `internal/adapter/aiden.go`
- Modify: `internal/adapter/aiden_test.go`
- Modify: `internal/worker/worker.go`
- Modify: `configs/bots.json`

- [ ] **Step 1: Write failing argv tests**

Add:

```go
func TestCodexBuildArgsFresh(t *testing.T) {
	a := NewCodexAdapter(AdapterOptions{CliType: "codex", Model: "gpt-5.5"})
	got := a.buildArgs("/tmp/repo")
	want := []string{
		"--dangerously-bypass-approvals-and-sandbox",
		"--no-alt-screen",
		"-c", "check_for_update_on_startup=false",
		"--model", "gpt-5.5",
		"-C", "/tmp/repo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}

func TestCodexBuildArgsResumeDoesNotOverrideModel(t *testing.T) {
	a := NewCodexAdapter(AdapterOptions{
		CliType: "codex", Model: "new-default",
		ResumeSessionID: "native-session",
	})
	got := a.buildArgs("/tmp/repo")
	want := []string{
		"resume",
		"--dangerously-bypass-approvals-and-sandbox",
		"--no-alt-screen",
		"-c", "check_for_update_on_startup=false",
		"native-session",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}
```

Also replace the temporary Aiden test with:

```go
func TestAidenBuildArgsUsesNativeAiden(t *testing.T) {
	a := NewAidenAdapter(AdapterOptions{CliType: "aiden", Model: "model-a"})
	got := a.buildArgs("/tmp/workspace")
	want := []string{"--permission-mode", "agentFull", "--model", "model-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}
```

- [ ] **Step 2: Run tests and confirm they fail**

```bash
go test ./internal/adapter \
  -run 'Test(CodexBuildArgs|AidenBuildArgsUsesNativeAiden)' -count=1
```

Expected: missing `NewCodexAdapter` and wrong Aiden argv.

- [ ] **Step 3: Implement Codex process lifecycle**

Extend `AdapterOptions`:

```go
type AdapterOptions struct {
	CliType         string
	CliPath         string
	Model           string
	ResumeSessionID string
}
```

Implement `CodexAdapter` with these state fields:

```go
type CodexAdapter struct {
	mu       sync.Mutex
	sendMu   sync.Mutex
	cmdPath  string
	model    string
	resumeID string
	cmd      *exec.Cmd
	ptmx     *os.File
	outputR  *io.PipeReader
	outputW  *io.PipeWriter
	errCh    chan error
	ready    chan error
	events   chan AdapterEvent
	readyOne sync.Once
	closed   bool
}
```

Register it:

```go
func init() {
	RegisterFactory("codex", func(opts AdapterOptions) CliAdapter {
		return NewCodexAdapter(opts)
	})
}
```

`Start` must:

1. resolve `codex` with `exec.LookPath`;
2. start it via `pty.StartWithSize`;
3. create an `io.Pipe`;
4. copy PTY chunks into the pipe while feeding `observeReady`;
5. send process exit to `errCh`;
6. send the same pre-ready exit error to `ready` exactly once.

Use this READY predicate:

```go
func codexComposerReady(screen string) bool {
	if strings.Contains(screen, "% left") {
		return true
	}
	for rest := screen; ; {
		i := strings.Index(rest, "›")
		if i < 0 {
			return false
		}
		after := strings.TrimLeft(rest[i+len("›"):], " \t")
		if len(after) < 2 || after[0] < '0' || after[0] > '9' ||
			!strings.Contains(after[:min(len(after), 8)], ".") {
			return true
		}
		rest = after
	}
}
```

Keep a bounded rolling raw screen buffer so prompt fragments spanning reads are
recognized. Add `stripCodexANSI(string) string` in `codex.go` and strip ANSI
before applying the predicate. `ready` is buffered with capacity one;
`observeReady` sends `nil` exactly once, while the `cmd.Wait` goroutine sends
the process error exactly once when readiness has not yet succeeded.

- [ ] **Step 4: Make Worker wait for authoritative readiness**

Add a helper:

```go
func (w *Worker) waitForCLIReady() error {
	if w.startResult.ReadyResult != nil {
		select {
		case err := <-w.startResult.ReadyResult:
			return err
		case <-time.After(45 * time.Second):
			return errors.New("cli ready timeout")
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
	if w.startResult.ReadyDelay > 0 {
		select {
		case <-time.After(w.startResult.ReadyDelay):
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
	return nil
}
```

Call it before closing `w.readyCh`. Keep `ErrCh` exclusively for steady-state
process exit handling, avoiding two consumers racing on one channel.

- [ ] **Step 5: Restore Aiden and configure native Codex**

Restore Aiden argv:

```go
args := []string{"--permission-mode", "agentFull"}
if a.model != "" {
	args = append(args, "--model", a.model)
}
```

Set the sample Codex bot to:

```json
{
  "name": "codex-bot",
  "bot_id": "bot-codex",
  "cli_type": "codex",
  "cli_path": "codex",
  "backend_type": "pty",
  "working_dir": "/Users/bytedance/botmux-go",
  "model": "gpt-5.5",
  "allowed_users": ["*"]
}
```

- [ ] **Step 6: Verify and commit**

```bash
gofmt -w internal/adapter internal/worker
go test ./internal/adapter ./internal/worker -count=1
go test -race ./internal/adapter ./internal/worker -count=1
go build ./...
go vet ./...
git add internal/adapter/codex.go internal/adapter/codex_test.go \
  internal/adapter/aiden.go internal/adapter/aiden_test.go \
  internal/adapter/adapter.go internal/worker/worker.go configs/bots.json
git commit -m "feat(codex): add native launch and readiness"
```

## Task 3: Add Verified Bracketed-Paste Submission

**Files:**

- Create: `internal/adapter/codex_history.go`
- Create: `internal/adapter/codex_history_test.go`
- Modify: `internal/adapter/codex.go`

- [ ] **Step 1: Write failing history parser tests**

Cover complete, partial, unrelated, CRLF-normalized, and same-text foreign
session records:

```go
func TestMatchCodexHistoryDelta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	old := `{"session_id":"old","text":"old"}` + "\n"
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	baseline := int64(len(old))
	appendFile(t, path,
		`{"session_id":"foreign","text":"same"}`+"\n"+
			`{"session_id":"owned","text":"first\r\nsecond"}`+"\n")

	got, ok := matchCodexHistoryDelta(
		path, baseline, "first\nsecond",
		func(id string) bool { return id == "owned" },
	)
	if !ok || got != "owned" {
		t.Fatalf("got (%q,%v), want owned,true", got, ok)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := io.WriteString(f, content); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Confirm red**

```bash
go test ./internal/adapter -run 'TestMatchCodexHistory' -count=1
```

Expected: parser undefined.

- [ ] **Step 3: Implement history scanning**

Implement:

```go
func codexHome() string
func codexHistoryPath() string
func currentFileSize(path string) int64
func matchCodexHistoryDelta(
	path string,
	from int64,
	expected string,
	accept func(string) bool,
) (string, bool)
func waitForCodexHistory(
	ctx context.Context,
	path string,
	from int64,
	expected string,
	accept func(string) bool,
) (string, error)
```

Only parse newline-terminated records. Normalize `\r\n`/`\r` before exact text
comparison.

- [ ] **Step 4: Add PID rollout ownership**

Implement:

```go
func codexRolloutsOwnedByPID(pid int) (map[string]struct{}, error)
func codexSessionIDFromRolloutPath(path string) (string, bool)
```

Linux reads `/proc/<pid>/fd`; macOS runs:

```text
lsof -p <pid> -Fn
```

Only accept:

```text
/sessions/.../rollout-*-<UUID>.jsonl
```

If ownership cannot be proven, history confirmation fails closed.

Inject the ownership lookup for deterministic tests:

```go
type codexDependencies struct {
	ownedRollouts func(pid int) (map[string]struct{}, error)
}

func (a *CodexAdapter) ownsSession(id string) bool {
	owned, err := a.deps.ownedRollouts(a.cmd.Process.Pid)
	if err != nil {
		return false
	}
	_, ok := owned[strings.ToLower(id)]
	return ok
}
```

- [ ] **Step 5: Implement verified input**

`CodexAdapter.Send` must:

```go
baseline := currentFileSize(codexHistoryPath())
body := "\x1b[200~" + normalizeCodexInput(input) + "\x1b[201~"
if err := writeAll(ctx, a.ptmx, []byte(body)); err != nil {
	return SendResult{}, err
}
if err := waitContext(ctx, 200*time.Millisecond); err != nil {
	return SendResult{}, err
}
for attempt := 0; attempt < 3; attempt++ {
	if err := writeAll(ctx, a.ptmx, []byte{'\r'}); err != nil {
		return SendResult{}, err
	}
	sid, err := waitForCodexHistory(ctx, historyPath, baseline, input, a.ownsSession)
	if err == nil {
		return SendResult{CliSessionID: sid}, nil
	}
}
return SendResult{}, errors.New("codex submit not confirmed after 3 Enter attempts")
```

Implement these local helpers in `codex.go`:

```go
func normalizeCodexInput(input string) string {
	input = strings.ReplaceAll(input, "\r\n", "\n")
	input = strings.ReplaceAll(input, "\r", "\n")
	return strings.TrimRight(input, "\n")
}

func writeAll(ctx context.Context, w io.Writer, data []byte) error {
	for len(data) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
```

Never resend `body`. Serialize all sends with `sendMu`.

- [ ] **Step 6: Add input behavior tests**

Assert:

- one bracketed-paste body;
- multiline and UTF-8 unchanged;
- three Enter attempts at most;
- history confirmation returns native ID;
- foreign same-text session is ignored;
- context cancellation stops polling.

- [ ] **Step 7: Verify and commit**

```bash
gofmt -w internal/adapter
go test ./internal/adapter -run 'TestCodex.*(History|Send|Ownership)' -count=1
go test -race ./internal/adapter -run 'TestCodex.*(History|Send|Ownership)' -count=1
go build ./...
go vet ./...
git add internal/adapter/codex.go \
  internal/adapter/codex_history.go internal/adapter/codex_history_test.go \
  internal/adapter/codex_test.go
git commit -m "feat(codex): verify native input submission"
```

## Task 4: Parse Rollout Output and Terminal Events

**Files:**

- Create: `internal/adapter/codex_transcript.go`
- Create: `internal/adapter/codex_transcript_test.go`
- Modify: `internal/adapter/codex.go`

- [ ] **Step 1: Write fixture-driven failing tests**

Use real-shape records:

```go
const rolloutFixture = `
{"timestamp":"2026-09-07T10:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}
{"timestamp":"2026-09-07T10:00:01Z","type":"event_msg","payload":{"type":"task_complete","last_agent_message":"world"}}
`
```

Assert the parser emits:

```go
[]AdapterEvent{
	{Kind: AdapterOutput, Output: "world"},
	{Kind: AdapterTurnTerminal, Status: TurnCompleted},
}
```

Add separate fixtures for `turn_aborted`, structured error, malformed lines,
and an old terminal before the expected user event.

- [ ] **Step 2: Confirm red**

```bash
go test ./internal/adapter -run 'TestCodexTranscript' -count=1
```

Expected: transcript parser undefined.

- [ ] **Step 3: Implement the incremental parser**

Define:

```go
type codexTranscriptCursor struct {
	Path   string
	Offset int64
	Armed  bool
	Input  string
}

func (c *codexTranscriptCursor) ReadNew() ([]AdapterEvent, error)
func parseCodexRolloutLine(line []byte) codexRolloutRecord
func safeCodexErrorDetail(raw string) string
```

Rules:

- ignore records until the expected user message is observed;
- emit only `task_complete.last_agent_message`;
- emit exactly one terminal;
- bound error detail to 320 characters;
- never include fields named token, authorization, secret, password, cookie, or
  credential.

- [ ] **Step 4: Wire the watcher into CodexAdapter**

Before a resumed send, snapshot the existing rollout size. After history
confirmation, locate the owned rollout and start polling from the correct
offset.

Use a bounded discovery timeout:

```go
const codexRolloutDiscoveryTimeout = 10 * time.Second
const codexTranscriptPollInterval = 100 * time.Millisecond
```

Publish events with context-aware blocking sends:

```go
select {
case a.events <- event:
case <-ctx.Done():
	return
}
```

On terminal, release the adapter's serialized turn wait.

- [ ] **Step 5: Verify and commit**

```bash
gofmt -w internal/adapter
go test ./internal/adapter -run 'TestCodexTranscript' -count=1
go test -race ./internal/adapter -run 'TestCodexTranscript' -count=1
go test ./...
git add internal/adapter/codex.go \
  internal/adapter/codex_transcript.go \
  internal/adapter/codex_transcript_test.go
git commit -m "feat(codex): emit rollout terminal events"
```

## Task 5: Add Terminal Protocol and Worker Event Pump

**Files:**

- Modify: `internal/protocol/protocol.go`
- Modify: `internal/worker/worker.go`
- Create: `internal/worker/codex_flow_test.go`
- Modify: `cmd/daemon/main.go`
- Modify: `cmd/daemon/main_test.go`

- [ ] **Step 1: Write failing CLI terminal tests**

Extend the fake daemon test to send:

```go
protocol.NewMessage(protocol.MsgOutput, "session-1", "final answer")
protocol.NewMessage(
	protocol.MsgTurnCompleted,
	"session-1",
	`{"status":"completed"}`,
)
```

Keep the server connection open after terminal and assert `execCommand` returns
without waiting for EOF.

Add failed terminal:

```go
`{"status":"failed","error_code":"codex_upstream_error","error_detail":"retry later"}`
```

Assert the command path returns a non-zero result through a testable
`execSendCommand(...) error` helper rather than calling `os.Exit` internally.

- [ ] **Step 2: Confirm red**

```bash
go test ./cmd/daemon -run 'TestExecCommandSend.*Turn' -count=1
```

Expected: `MsgTurnCompleted` undefined or command does not return.

- [ ] **Step 3: Add protocol types**

In `protocol.go`:

```go
const (
	MsgCliSessionBound MessageType = "cli_session_bound"
	MsgTurnCompleted   MessageType = "turn_completed"
)

type TurnTerminal struct {
	Status      TurnStatus `json:"status"`
	ErrorCode   string `json:"error_code,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`
}

type TurnStatus string

const (
	TurnCompleted TurnStatus = "completed"
	TurnAborted   TurnStatus = "aborted"
	TurnFailed    TurnStatus = "failed"
)
```

- [ ] **Step 4: Add Worker structured event pump**

Worker starts one additional goroutine when `Events != nil`:

```go
func (w *Worker) readAdapterEvents(events <-chan adapter.AdapterEvent) {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			switch event.Kind {
			case adapter.AdapterOutput:
				_ = w.sendMessage(protocol.MsgOutput, event.Output)
			case adapter.AdapterTurnTerminal:
				payload, _ := json.Marshal(protocol.TurnTerminal{
					Status: protocol.TurnStatus(event.Status),
					ErrorCode: event.ErrorCode,
					ErrorDetail: event.ErrorDetail,
				})
				_ = w.sendMessage(protocol.MsgTurnCompleted, string(payload))
			}
		}
	}
}
```

When `StructuredOutput` is true, `readCliOutput` still logs and updates idle
state but skips `MsgOutput`.

After `Send` succeeds:

```go
if result.CliSessionID != "" {
	_ = w.sendMessage(protocol.MsgCliSessionBound, result.CliSessionID)
}
```

After `Send` fails, emit a failed `MsgTurnCompleted` so the client and Daemon
turn gate cannot hang.

- [ ] **Step 5: Exit CLI on terminal**

Extract:

```go
func execSendCommand(addr, sid, message string, out io.Writer) error
```

On terminal:

```go
var terminal protocol.TurnTerminal
if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
	return fmt.Errorf("decode turn terminal: %w", err)
}
if terminal.Status != protocol.TurnCompleted {
	return fmt.Errorf("%s: %s", terminal.ErrorCode, terminal.ErrorDetail)
}
fmt.Fprintln(out, "[cli] done")
return nil
```

The existing absolute 120-second deadline remains.

- [ ] **Step 6: Verify and commit**

```bash
gofmt -w internal/protocol internal/worker cmd/daemon
go test ./internal/worker ./cmd/daemon -count=1
go test -race ./internal/worker ./cmd/daemon -count=1
go test ./...
go build ./...
go vet ./...
git add internal/protocol/protocol.go internal/worker/worker.go \
  internal/worker/codex_flow_test.go \
  cmd/daemon/main.go cmd/daemon/main_test.go
git commit -m "feat(codex): terminate clients from structured turns"
```

## Task 6: Persist Native Session ID and Serialize Turns

**Files:**

- Modify: `internal/daemon/session_meta.go`
- Modify: `internal/daemon/session_store.go`
- Modify: `internal/daemon/daemon.go`
- Modify: `cmd/daemon/main.go`
- Create: `internal/daemon/turn_state_test.go`
- Create: `internal/daemon/session_resume_test.go`

- [ ] **Step 1: Write failing turn-gate tests**

```go
func TestSessionMetaAllowsOneActiveTurn(t *testing.T) {
	m := NewSessionMeta("s1", "b1")
	if !m.BeginTurn() {
		t.Fatal("first turn rejected")
	}
	if m.BeginTurn() {
		t.Fatal("concurrent turn accepted")
	}
	m.FinishTurn()
	if !m.BeginTurn() {
		t.Fatal("next turn rejected after terminal")
	}
}
```

Add terminal subscription coverage:

```go
cursor, ch := m.TerminalSubscription()
m.PublishTerminal(protocol.TurnTerminal{Status: "completed"})
<-ch
events, next, _ := m.SnapshotTerminalsSince(cursor)
```

Assert one terminal, monotonic cursor, and no busy-loop.

- [ ] **Step 2: Write failing persistence tests**

Persist and reload:

```go
CliSessionID: "0199d-session"
```

Assert `SessionMetaFromPersisted` retains it and worker spawn contains:

```text
BOTMUX_CLI_SESSION_ID=0199d-session
```

- [ ] **Step 3: Implement turn and terminal state**

Add to `SessionMeta`:

```go
CliSessionID     string
turnActive       bool
terminalSeq      uint64
terminals        []protocol.TurnTerminal
terminalNotifyCh chan struct{}
```

Implement under `m.mu`:

```go
func (m *SessionMeta) BeginTurn() bool
func (m *SessionMeta) FinishTurn()
func (m *SessionMeta) PublishTerminal(protocol.TurnTerminal)
func (m *SessionMeta) TerminalSubscription() (uint64, <-chan struct{})
func (m *SessionMeta) SnapshotTerminalsSince(
	cursor uint64,
) ([]protocol.TurnTerminal, uint64, <-chan struct{})
func (m *SessionMeta) SetCliSessionID(id string)
func (m *SessionMeta) GetCliSessionID() string
```

Keep only a small bounded terminal history, for example 16 records.

- [ ] **Step 4: Persist and propagate the native ID**

Add:

```go
CliSessionID string `json:"cli_session_id,omitempty"`
```

Implement:

```go
func (s *SessionStore) UpdateCliSessionID(sessionID, cliSessionID string) error
```

Add environment propagation:

```text
BOTMUX_CLI_SESSION_ID=<persisted id>
```

Read it in `runWorker`, add it to `worker.Options`, and pass it into
`adapter.AdapterOptions.ResumeSessionID`.

- [ ] **Step 5: Route session-bound and terminal messages**

In `routeMessage`:

```go
case protocol.MsgCliSessionBound:
	meta.SetCliSessionID(msg.Payload)
	_ = d.store.UpdateCliSessionID(meta.SessionID, msg.Payload)

case protocol.MsgTurnCompleted:
	var terminal protocol.TurnTerminal
	if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
		terminal = protocol.TurnTerminal{
			Status: protocol.TurnFailed, ErrorCode: "invalid_terminal",
			ErrorDetail: err.Error(),
		}
	}
	meta.PublishTerminal(terminal)
	meta.FinishTurn()
```

Before forwarding user input:

```go
if !meta.BeginTurn() {
	write MsgError("session already has an active turn")
	return
}
```

If worker send fails, call `FinishTurn`. Do not clear the turn merely because
the client disconnects or its deadline expires.

`forwardClientMessage` subscribes to output and terminal cursors before sending.
On terminal, it forwards `MsgTurnCompleted` and returns.

Worker process exit while a turn is active publishes a failed terminal and
clears the gate, guarded by current Worker generation identity.

- [ ] **Step 6: Verify and commit**

```bash
gofmt -w internal/daemon cmd/daemon internal/worker
go test ./internal/daemon ./cmd/daemon -count=1
go test -race ./internal/daemon ./cmd/daemon -count=1
go test ./...
go build ./...
go vet ./...
git add internal/daemon/session_meta.go internal/daemon/session_store.go \
  internal/daemon/daemon.go internal/daemon/turn_state_test.go \
  internal/daemon/session_resume_test.go \
  internal/worker/worker.go cmd/daemon/main.go
git commit -m "feat(codex): persist and resume native sessions"
```

## Task 7: Fake-Codex Integration and V6 Documentation

**Files:**

- Create: `testdata/fake-codex/main.go`
- Create: `internal/worker/codex_integration_test.go`
- Create: `scripts/v6_codex_e2e.sh`
- Create: `docs/versions/v6-architecture.md`
- Modify: `docs/README.md`

- [ ] **Step 1: Build a deterministic fake Codex**

The helper must:

- accept fresh and `resume` argv;
- print a real-looking composer marker;
- parse bracketed-paste input until Enter;
- append history with a stable fake session ID;
- create a rollout under `$CODEX_HOME/sessions/YYYY/MM/DD`;
- append user and `task_complete` records;
- record received argv for resume assertions.

Use this terminal record:

```go
fmt.Fprintf(rollout,
	"{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"last_agent_message\":%q}}\n",
	"fake final: "+input,
)
```

- [ ] **Step 2: Add integration tests**

Test:

1. fresh process reaches READY;
2. multiline input appears once in history;
3. final output appears once;
4. terminal arrives before the 120-second fallback;
5. session JSON stores fake native session ID;
6. Worker restart uses `resume <id>`;
7. a concurrent second send returns busy;
8. fake process exit produces failed terminal and clears the gate.

- [ ] **Step 3: Run the complete automated matrix**

```bash
go test ./...
go test -race ./...
go build ./...
go vet ./...
git diff --check
```

Expected: all commands exit `0`.

- [ ] **Step 4: Build and run real Codex acceptance**

Create `scripts/v6_codex_e2e.sh` with:

```bash
set -euo pipefail
cd /Users/bytedance/botmux-go
go build -o /tmp/botmux-go-v6-codex ./cmd/daemon
rm -rf "$HOME/.botmux-go/sessions"
mkdir -p "$HOME/.botmux-go/sessions"
/tmp/botmux-go-v6-codex -config ./configs/bots.json \
  >/tmp/botmux-go-v6-codex-daemon.log 2>&1 &
echo $! >/tmp/botmux-go-v6-codex.pid
```

Then verify:

```bash
SID="v6-codex-$(date +%s)"
/tmp/botmux-go-v6-codex -cmd new "$SID" bot-codex
/tmp/botmux-go-v6-codex -cmd send "$SID" \
  $'Return exactly two lines:\nCODEX_V6_ONE\nCODEX_V6_TWO'
kill "$(cat /tmp/botmux-go-v6-codex.pid)"
```

Restart the daemon without deleting sessions, then send:

```bash
/tmp/botmux-go-v6-codex -config ./configs/bots.json \
  >/tmp/botmux-go-v6-codex-daemon-2.log 2>&1 &
/tmp/botmux-go-v6-codex -cmd send "$SID" \
  "What were the two markers from the previous turn?"
```

Expected:

- first send exits on `MsgTurnCompleted`;
- answer contains both markers once;
- second daemon starts Worker with `codex resume <persisted-id>`;
- second answer proves prior context;
- no `submit not confirmed`, duplicate output, or rapid restart loop.

- [ ] **Step 5: Document the milestone**

`docs/versions/v6-architecture.md` must include:

- V5 to V6 architecture delta;
- fresh/send/terminal and restart/resume sequence diagrams;
- history and rollout ownership rules;
- protocol additions;
- failure matrix;
- automated and manual acceptance evidence;
- deferred Type-ahead/RPC work.

Update `docs/README.md` version table and permanent Feishu index.

- [ ] **Step 6: Commit**

```bash
git add testdata/fake-codex/main.go \
  internal/worker/codex_integration_test.go \
  scripts/v6_codex_e2e.sh \
  docs/versions/v6-architecture.md docs/README.md
git commit -m "test(codex): verify native session lifecycle"
```

## Final Verification

- [ ] Run:

```bash
go test ./...
go test -race ./...
go build ./...
go vet ./...
git diff --check
git status --short
```

- [ ] Confirm the implementation commits are separated as:

```text
refactor(adapter): add structured turn contracts
feat(codex): add native launch and readiness
feat(codex): verify native input submission
feat(codex): emit rollout terminal events
feat(codex): terminate clients from structured turns
feat(codex): persist and resume native sessions
test(codex): verify native session lifecycle
```

- [ ] Update the V6 document with the real test result and remind the user to
sync it to a new Feishu Wiki page without overwriting V5.
