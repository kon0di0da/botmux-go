# Native Codex Adapter Design

Date: 2026-09-07
Status: Approved design, pending implementation plan

## 1. Goal

Add a first-class native Codex CLI adapter to botmux-go. The first release must
provide a reliable single-turn-at-a-time loop:

1. Start native `codex` in the configured working directory.
2. Report READY only after the real Codex composer appears.
3. Submit single-line, multiline, long, and UTF-8 input without splitting it
   into multiple turns.
4. Confirm submission from Codex's structured history.
5. Read final output and terminal status from Codex rollout JSONL.
6. Persist the native Codex session ID and resume it after Worker recreation.
7. Let the client exit on an explicit turn-terminal protocol message.

The 120-second client deadline remains a fault-containment fallback, not the
normal completion mechanism.

## 2. Scope

### Included

- Native `codex` executable.
- Fresh launch and exact `codex resume <cliSessionId>`.
- `--no-alt-screen`, working directory, model, approval/sandbox bypass flags.
- Real prompt-ready detection.
- Bracketed-paste input and separate Enter submission.
- `~/.codex/history.jsonl` submission confirmation.
- Rollout JSONL discovery and incremental parsing.
- `task_complete`, `turn_aborted`, and structured failure handling.
- Session-level serialization: one active Codex turn per botmux session.
- Explicit terminal messages from Worker to CLI.
- Fake-Codex integration tests and real-Codex manual acceptance.

### Excluded

- Type-ahead and active-turn steer.
- Codex app-server / hybrid RPC input.
- Codex App runner.
- Live `/adopt` of an external pane.
- `codex fork`.
- OverlayFS, Seatbelt, or other filesystem isolation.
- CoT/tool timeline, token usage, dynamic model discovery, and writable web
  terminal.

These exclusions keep the first release small while preserving extension
points for the mechanisms already proven in TypeScript botmux.

## 3. Key Decisions

### 3.1 Protocol identity and launcher identity are separate

`cli_type=codex` selects Codex protocol behavior. The first release launches the
native `codex` binary. Gateway wrappers such as `aiden x codex` must not be
hard-coded into `AidenAdapter`; a future launcher abstraction or executable
wrapper may reuse the Codex protocol adapter.

As a cleanup prerequisite, restore `AidenAdapter` to native Aiden semantics and
remove the temporary `aiden x codex` argv introduced during V5 diagnosis.

### 3.2 Structured records are authoritative

PTY output remains useful for readiness, diagnostics, and a local terminal, but
it is not authoritative for final answers. Codex rollout records own:

- turn start,
- final visible answer,
- terminal status,
- structured failure.

This prevents ANSI redraws and duplicate TUI representations from becoming
user-visible duplicate answers.

### 3.3 No concurrent turn ambiguity

The first release allows one active turn per session. A second send while the
prior turn is active receives a deterministic busy error. It is not silently
queued and is not injected into the active Codex turn.

Type-ahead requires a dedicated attribution queue and HOL-block-drop semantics;
it is deferred rather than approximated.

## 4. Components

### 4.1 `CodexAdapter`

New files:

- `internal/adapter/codex.go`
- `internal/adapter/codex_test.go`

Responsibilities:

- resolve the native executable;
- build fresh/resume argv;
- own the PTY process;
- detect the real composer from normalized PTY output;
- write bracketed-paste input;
- verify history append;
- discover the native session ID;
- tail the owned rollout;
- emit structured adapter events.

Fresh argv:

```text
codex --dangerously-bypass-approvals-and-sandbox \
  --no-alt-screen \
  -c check_for_update_on_startup=false \
  -C <workingDir> \
  [--model <model>]
```

Resume argv:

```text
codex resume \
  --dangerously-bypass-approvals-and-sandbox \
  --no-alt-screen \
  -c check_for_update_on_startup=false \
  <cliSessionId>
```

Resume does not reapply model selection. The persisted Codex conversation owns
its provider/model metadata; injecting current defaults can silently drift a
resumed session.

### 4.2 Adapter contract extensions

`CliStartResult` gains:

- `ReadyResult <-chan error` for adapters with authoritative readiness (`nil`
  means ready; a non-nil error means the CLI exited or readiness timed out);
- `Events <-chan AdapterEvent` for structured output and terminal events;
- `StructuredOutput bool` to suppress forwarding raw TUI lines as answers.

Existing `ReadyDelay` remains for legacy adapters.
`ReadyResult` is separate from the existing process-exit `ErrCh`, so startup
waiting and steady-state output handling never race to consume one error.

`CliAdapter.Send` returns `SendResult`:

```go
type SendResult struct {
    CliSessionID string
}
```

Legacy adapters return an empty result. Codex returns the session ID found in
the matching history entry.

`AdapterEvent` has these initial forms:

```go
type AdapterEvent struct {
    Kind        AdapterEventKind
    Output      string
    Status      TurnStatus
    ErrorCode   string
    ErrorDetail string
}
```

The event source is bounded and cancellation-aware. Terminal delivery must not
be dropped; diagnostic/raw output may be logged separately.

### 4.3 Codex history verifier

New file:

- `internal/adapter/codex_history.go`

Algorithm:

1. Record the current byte size of `history.jsonl`.
2. Paste the complete payload using bracketed paste.
3. Wait 200ms, then send Enter.
4. Scan only complete JSON lines appended after the baseline.
5. Compare decoded `text` exactly after CRLF normalization.
6. Verify that the Codex process owns the candidate session rollout through
   `/proc/<pid>/fd` on Linux or `lsof` on macOS; failure to prove ownership is
   fail-closed.
7. On timeout, resend Enter only; never resend the body.
8. Return the matching `session_id`.

The parser ignores malformed/partial trailing records until a later poll. The
first release serializes turns, so no cross-turn fingerprint queue is required.
PID ownership prevents a concurrent external Codex process with the same
`CODEX_HOME` and identical input text from binding the wrong session.

### 4.4 Codex rollout watcher

New files:

- `internal/adapter/codex_transcript.go`
- `internal/adapter/codex_transcript_test.go`

For a resumed session, discover the matching rollout and record its byte size
before input submission. For a fresh session, history confirmation first
provides `cliSessionId`, after which the newly-created rollout is discovered
under:

```text
${CODEX_HOME:-~/.codex}/sessions/YYYY/MM/DD/rollout-*-<cliSessionId>.jsonl
```

Read incrementally from the pre-submit byte offset and process only complete
JSON lines. A fresh rollout starts at offset zero. The watcher arms for the
exact submitted user text before accepting a terminal, so historical records
from a resumed session cannot close the new turn.

Recognized records:

- user `response_item` message: starts the expected turn;
- `event_msg.task_complete`: emits `last_agent_message` and completed terminal;
- `event_msg.turn_aborted`: emits aborted terminal;
- structured task failure: emits failed terminal with a bounded safe summary.

The watcher stops on terminal, context cancellation, process exit, or explicit
session close.

## 5. Worker and Daemon Flow

### 5.1 READY

The Worker starts PTY output consumption immediately. For Codex it feeds
normalized screen text into the adapter readiness detector. Only a nil result
from the adapter's `ReadyResult` channel permits Worker `MsgReady`.

Startup output, heartbeats, and TCP connection establishment cannot mark Codex
READY.

### 5.2 Turn gate

`SessionMeta` gains an in-memory active-turn gate:

- `BeginTurn()` atomically rejects concurrent sends;
- `FinishTurn()` clears the gate on completed, aborted, failed, close, or Worker
  exit.

This gate is not persisted. After restart, the Worker resumes the Codex session,
but an interrupted user turn is not automatically replayed in this release.

### 5.3 Structured event forwarding

The Worker consumes `CliStartResult.Events`:

- output event -> `MsgOutput`;
- completed/aborted/failed event -> `MsgTurnCompleted`.

For structured-output adapters, `readCliOutput` still updates readiness and
idle diagnostics but does not forward cleaned TUI lines as `MsgOutput`.

Daemon stores normal output as before and adds a monotonic terminal broadcast
for the active client. On terminal it clears the turn gate and forwards
`MsgTurnCompleted`.

The CLI send loop exits immediately on `MsgTurnCompleted`, exits non-zero on a
failed/aborted status, and retains the absolute deadline as fallback.

## 6. Persistence and Resume

`SessionMeta` and `PersistedSession` gain:

```text
cli_session_id
```

The Worker sends `MsgCliSessionBound` after a confirmed Codex submission.
Daemon updates both in-memory metadata and `SessionStore` atomically.

Worker spawn receives the persisted ID through its startup configuration. The
Codex adapter uses fresh launch when the ID is absent and exact resume when it
is present.

If a resume ID is invalid:

1. Codex exits before READY.
2. Worker reports a startup error.
3. Daemon applies the existing bounded restart budget.
4. The system does not silently launch a fresh conversation while claiming
   history was restored.

## 7. Failure Semantics

| Failure | Behavior |
|---|---|
| Codex binary missing | Worker startup error, never READY |
| Prompt never appears | readiness timeout, bounded restart |
| Text write failure | fail send; body is not blindly resent |
| Enter unconfirmed | resend Enter only, then explicit submit error |
| History match absent | no session binding; user-visible send failure |
| Rollout missing after confirmed submit | bounded discovery timeout, failed terminal, clear turn gate |
| `turn_aborted` | terminal status `aborted`, CLI exits non-zero |
| Structured task failure | terminal status `failed`, safe bounded detail |
| Worker/process exit mid-turn | clear turn gate and emit failure |
| Daemon restart after completed binding | exact `codex resume <id>` |

## 8. Delivery Slices

1. **C1: Adapter launch and READY**
   Register Codex, build argv, detect composer, restore Aiden launcher semantics.

2. **C2: Reliable input and session binding**
   Bracketed paste, history confirmation, native session ID persistence.

3. **C3: Transcript output and terminal protocol**
   Rollout parser, structured event channel, `MsgTurnCompleted`, CLI exit.

4. **C4: Exact resume**
   Worker startup propagation, `codex resume`, restart behavior tests.

5. **C5: Acceptance and documentation**
   Fake-Codex integration, real two-turn test, daemon restart continuation, V6
   architecture notes.

Each slice must independently pass `go test ./...`, `go build ./...`, and
`go vet ./...`.

## 9. Test Strategy

### Unit tests

- fresh/resume argv and model omission on resume;
- composer READY matcher excludes update menus;
- CRLF normalization and UTF-8/multiline paste;
- history parser ignores old, partial, malformed, and wrong-text entries;
- transcript parser emits exactly one final for `task_complete`;
- aborted and failed terminal mapping;
- old resumed records cannot close the current turn;
- terminal broadcast wakes all current waiters without busy-looping.

### Integration tests

A fake Codex process writes realistic history and rollout fixtures while
rendering a minimal prompt. Cover:

- fresh READY and one completed turn;
- no READY on early process exit;
- Enter retry without body duplication;
- client exits on terminal rather than deadline;
- persisted `cli_session_id`;
- Worker recreation launches exact resume;
- concurrent second send receives busy.

### Manual acceptance

1. Create native Codex session.
2. Send a multiline prompt and receive one complete final answer.
3. Send a second turn without history replay.
4. Restart daemon/Worker.
5. Send a third turn and verify retained Codex context.
6. Confirm no duplicate output, submit warning, or restart storm.

## 10. Deferred Follow-ups

The next Codex phase may add:

- in-flight input journal and replay;
- Type-ahead/steer attribution queue;
- app-server RPC input;
- `/adopt` and `fork`;
- structured CoT and token usage;
- launcher profiles for Aiden X/TTADK;
- filesystem sandbox and writable terminal.
