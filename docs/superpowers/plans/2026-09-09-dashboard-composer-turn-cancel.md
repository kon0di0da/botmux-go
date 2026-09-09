# Dashboard Composer And Codex Turn Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a multiline Dashboard composer and a cancellable Codex turn that sends Esc first, then restarts and resumes the worker after a bounded timeout.

**Architecture:** `SessionMeta` owns a tokenized turn state so exactly one terminal is committed for every accepted turn. The Dashboard calls a new HTTP cancel endpoint; the daemon records cancellation and recovery state, the worker translates daemon IPC into an adapter interrupt or a non-closing restart, and the Codex adapter writes Esc to its existing PTY. The existing session monitor remains responsible for spawning the resumed replacement worker.

**Tech Stack:** Go 1.22, `net/http`, JSON-over-TCP worker IPC, `os/exec`, `creack/pty/v2`, embedded HTML/JavaScript, Go standard-library tests, Bash/curl acceptance script.

---

## File Map

| File | Change |
|---|---|
| `internal/protocol/protocol.go` | Add daemon-to-worker `cancel_turn` and `restart_worker` message types. |
| `internal/daemon/session_meta.go` | Add tokenized active/cancelling turn state, atomic terminal commitment, and a snapshot for HTTP. |
| `internal/daemon/turn_state_test.go` | Prove gate, cancel transition, timeout-token isolation, and terminal de-duplication. |
| `internal/adapter/adapter.go` | Add the optional `CliTurnInterrupter` capability. |
| `internal/adapter/codex.go` | Implement `Interrupt` by writing exactly one Esc byte to the existing PTY. |
| `internal/adapter/codex_test.go` | Verify the Esc byte without launching a real Codex process. |
| `internal/worker/worker.go` | Handle `cancel_turn` and `restart_worker`; split regular close cleanup from non-closing restart cleanup. |
| `internal/worker/worker_cancel_test.go` | Verify worker forwarding of Esc failures and restart persistence behavior. |
| `internal/daemon/daemon.go` | Add `CancelTurn`, its timeout/restart watchdog, stale-worker filtering, and atomic terminal routing. |
| `internal/daemon/cancel_turn_test.go` | Verify daemon cancel acceptance, timeout behavior, stale terminal filtering, and restart grace kill. |
| `internal/daemon/http_server.go` | Add `POST /api/sessions/{id}/cancel` and expose turn state/latest terminal in session detail. |
| `internal/daemon/http_server_test.go` | Verify cancel HTTP semantics and detail JSON fields. |
| `internal/daemon/dashboard.html` | Replace the chat input with a textarea, keyboard shortcut, state-aware controls, and a cancel icon button. |
| `internal/daemon/dashboard_test.go` | Guard Dashboard source against regression to a single-line composer or removal of cancel wiring. |
| `scripts/v6_codex_e2e.sh` | Add an opt-in real-Codex cancel/resume acceptance scenario. |
| `docs/versions/v6-architecture.md` | Document the turn cancellation state machine and recovery guarantee. |

## Constants And State Contract

Implement these daemon constants beside the `Daemon` type:

```go
const (
    defaultCancelTurnTimeout  = 10 * time.Second
    defaultRestartWorkerGrace = 2 * time.Second
)
```

Add test-overridable durations to `Daemon`:

```go
turnCancelTimeout  time.Duration
restartWorkerGrace time.Duration
```

Use helpers so zero-valued test daemons retain production behavior:

```go
func (d *Daemon) cancelTurnTimeoutOrDefault() time.Duration {
    if d.turnCancelTimeout > 0 {
        return d.turnCancelTimeout
    }
    return defaultCancelTurnTimeout
}

func (d *Daemon) restartWorkerGraceOrDefault() time.Duration {
    if d.restartWorkerGrace > 0 {
        return d.restartWorkerGrace
    }
    return defaultRestartWorkerGrace
}
```

Define the session-owned view:

```go
type TurnSnapshot struct {
    Active      bool
    Cancelling  bool
    Token       uint64
    Latest      *protocol.TurnTerminal
}
```

`Token` is never exposed by HTTP. It exists solely to ensure a first turn's
timeout cannot interfere with a later turn that is also being cancelled.

### Task 1: Add Tokenized Turn State And IPC Types

**Files:**
- Modify: `internal/protocol/protocol.go:12-30`
- Modify: `internal/daemon/session_meta.go:31-210`
- Modify: `internal/daemon/turn_state_test.go`

- [ ] **Step 1: Write failing turn-state tests**

Replace the single gate-only test with these focused cases:

```go
func TestSessionMetaCancelTurnRequiresActiveTurn(t *testing.T) {
    meta := NewSessionMeta("session-1", "bot-codex")
    if _, ok := meta.BeginTurnCancel(); ok {
        t.Fatal("idle turn accepted cancellation")
    }
    if !meta.BeginTurn() {
        t.Fatal("first turn rejected")
    }
    token, ok := meta.BeginTurnCancel()
    if !ok || token == 0 {
        t.Fatalf("BeginTurnCancel() = (%d, %t), want non-zero token and true", token, ok)
    }
    if _, ok := meta.BeginTurnCancel(); ok {
        t.Fatal("duplicate cancellation accepted")
    }
    state := meta.TurnSnapshot()
    if !state.Active || !state.Cancelling || state.Token != token {
        t.Fatalf("turn state = %#v, want active cancelling token %d", state, token)
    }
}

func TestSessionMetaCancelTimeoutCommitsOnlyMatchingTurn(t *testing.T) {
    meta := NewSessionMeta("session-1", "bot-codex")
    meta.BeginTurn()
    oldToken, _ := meta.BeginTurnCancel()
    if !meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted}) {
        t.Fatal("aborted first turn was not committed")
    }
    if !meta.BeginTurn() {
        t.Fatal("second turn rejected")
    }
    newToken, _ := meta.BeginTurnCancel()
    if meta.FailCancellingTurn(oldToken, protocol.TurnTerminal{Status: protocol.TurnFailed}) {
        t.Fatal("old cancellation timer completed the new turn")
    }
    if !meta.FailCancellingTurn(newToken, protocol.TurnTerminal{
        Status: protocol.TurnFailed, ErrorCode: "codex_cancel_timeout",
    }) {
        t.Fatal("matching cancellation timer did not complete turn")
    }
}

func TestSessionMetaCommitsOnlyOneTerminalPerTurn(t *testing.T) {
    meta := NewSessionMeta("session-1", "bot-codex")
    meta.BeginTurn()
    if !meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnFailed}) {
        t.Fatal("first terminal rejected")
    }
    if meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted}) {
        t.Fatal("late terminal was accepted")
    }
    terminals, _, _ := meta.SnapshotTerminalsSince(0)
    if len(terminals) != 1 || terminals[0].Status != protocol.TurnFailed {
        t.Fatalf("terminals = %#v, want one failed terminal", terminals)
    }
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
go test ./internal/daemon -run 'TestSessionMeta(Cancel|Commits)' -count=1
```

Expected: compile failure because `BeginTurnCancel`, `TurnSnapshot`,
`CompleteTurn`, and `FailCancellingTurn` do not exist.

- [ ] **Step 3: Add IPC message types and state methods**

Add message constants:

```go
MsgCancelTurn    MessageType = "cancel_turn"
MsgRestartWorker MessageType = "restart_worker"
```

Extend `SessionMeta`:

```go
turnActive     bool
turnCancelling bool
turnToken      uint64
```

Replace direct terminal append logic with one locked helper and add these
methods:

```go
func (m *SessionMeta) BeginTurn() bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    if m.turnActive {
        return false
    }
    m.turnToken++
    m.turnActive = true
    m.turnCancelling = false
    return true
}

func (m *SessionMeta) BeginTurnCancel() (uint64, bool) {
    m.mu.Lock()
    defer m.mu.Unlock()
    if !m.turnActive || m.turnCancelling {
        return 0, false
    }
    m.turnCancelling = true
    return m.turnToken, true
}

func (m *SessionMeta) CompleteTurn(terminal protocol.TurnTerminal) bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    if !m.turnActive {
        return false
    }
    m.finishTurnLocked(terminal)
    return true
}

func (m *SessionMeta) FailCancellingTurn(token uint64, terminal protocol.TurnTerminal) bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    if !m.turnActive || !m.turnCancelling || m.turnToken != token {
        return false
    }
    m.finishTurnLocked(terminal)
    return true
}

func (m *SessionMeta) finishTurnLocked(terminal protocol.TurnTerminal) {
    m.turnActive = false
    m.turnCancelling = false
    m.publishTerminalLocked(terminal)
}

func (m *SessionMeta) TurnSnapshot() TurnSnapshot {
    m.mu.Lock()
    defer m.mu.Unlock()
    var latest *protocol.TurnTerminal
    if n := len(m.terminals); n > 0 {
        terminal := m.terminals[n-1]
        latest = &terminal
    }
    return TurnSnapshot{
        Active: m.turnActive, Cancelling: m.turnCancelling,
        Token: m.turnToken, Latest: latest,
    }
}
```

Keep `PublishTerminal` for non-turn event tests, but make it call
`publishTerminalLocked`. Update `FinishTurn` to clear both `turnActive` and
`turnCancelling`; it remains the rollback for a failed daemon-to-worker send.

- [ ] **Step 4: Run the focused tests and package test**

Run:

```bash
go test ./internal/daemon -run 'TestSessionMeta' -count=1
go test ./internal/protocol ./internal/daemon -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit the state-machine foundation**

```bash
git add internal/protocol/protocol.go internal/daemon/session_meta.go internal/daemon/turn_state_test.go
git commit -m "feat(turn): add tokenized cancellation state"
```

### Task 2: Add Codex Esc Interrupt And Worker Restart Cleanup

**Files:**
- Modify: `internal/adapter/adapter.go:47-56`
- Modify: `internal/adapter/codex.go:167-251`
- Modify: `internal/adapter/codex_test.go`
- Modify: `internal/worker/worker.go:280-360,495-585`
- Create: `internal/worker/worker_cancel_test.go`

- [ ] **Step 1: Write the failing Codex interrupt test**

Add this test to `internal/adapter/codex_test.go`:

```go
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
        _, _ = io.ReadFull(reader, got)
        gotCh <- got
    }()

    if err := a.Interrupt(context.Background()); err != nil {
        t.Fatalf("Interrupt: %v", err)
    }
    select {
    case got := <-gotCh:
        if !bytes.Equal(got, []byte{0x1b}) {
            t.Fatalf("interrupt bytes = %q, want Esc", got)
        }
    case <-time.After(time.Second):
        t.Fatal("timed out waiting for Esc")
    }
}
```

- [ ] **Step 2: Run the adapter test and verify it fails**

Run:

```bash
go test ./internal/adapter -run TestCodexInterruptWritesEsc -count=1
```

Expected: compile failure because `Interrupt` is not defined.

- [ ] **Step 3: Implement the optional interrupt capability**

Add the optional interface without changing `CliAdapter`:

```go
type CliTurnInterrupter interface {
    Interrupt(ctx context.Context) error
}
```

Implement Codex interruption under the existing `sendMu`:

```go
func (a *CodexAdapter) Interrupt(ctx context.Context) error {
    a.sendMu.Lock()
    defer a.sendMu.Unlock()

    a.mu.Lock()
    closed := a.closed
    ptmx := a.ptmx
    a.mu.Unlock()
    if closed {
        return fmt.Errorf("codex adapter is closed")
    }
    if ptmx == nil {
        return fmt.Errorf("codex adapter not started")
    }
    if err := writeAll(ctx, ptmx, []byte{0x1b}); err != nil {
        return fmt.Errorf("codex interrupt: %w", err)
    }
    return nil
}
```

- [ ] **Step 4: Write failing worker cancel and restart tests**

Create a package-local `interruptTestAdapter` in
`internal/worker/worker_cancel_test.go`. It implements `CliAdapter` and
`CliTurnInterrupter`; `Interrupt` increments a counter and can return a
configured error.

Use these complete test fixtures and tests:

```go
type interruptTestAdapter struct {
    interrupts atomic.Int32
    interruptErr error
}

func (a *interruptTestAdapter) Name() string { return "interrupt-test" }

func (a *interruptTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
    return nil, errors.New("Start should not be called by this test")
}

func (a *interruptTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
    return adapter.SendResult{}, nil
}

func (a *interruptTestAdapter) Close() error { return nil }

func (a *interruptTestAdapter) Interrupt(context.Context) error {
    a.interrupts.Add(1)
    return a.interruptErr
}

func newDaemonMessageWorker(t *testing.T, cli adapter.CliAdapter) (*Worker, net.Conn, net.Conn) {
    t.Helper()
    daemonConn, workerConn := net.Pipe()
    w := New(Options{SessionID: "worker-cancel-test", CliType: "codex"})
    w.cliAdapter = cli
    w.conn = workerConn
    w.msgReader = protocol.NewMessageReader(workerConn)
    return w, daemonConn, workerConn
}

func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if predicate() {
            return
        }
        time.Sleep(time.Millisecond)
    }
    t.Fatal("condition was not met before timeout")
}

func TestWorkerCancelTurnCallsAdapterInterrupt(t *testing.T) {
    cli := &interruptTestAdapter{}
    worker, daemonConn, workerConn := newDaemonMessageWorker(t, cli)
    defer daemonConn.Close()
    defer workerConn.Close()
    close(worker.readyCh)
    if !worker.beginTurn() {
        t.Fatal("begin turn")
    }

    worker.wg.Add(1)
    go worker.readDaemonMessages()
    if _, err := protocol.NewMessage(protocol.MsgCancelTurn, worker.sessionID, "").WriteTo(daemonConn); err != nil {
        t.Fatal(err)
    }
    waitFor(t, time.Second, func() bool { return cli.interrupts.Load() == 1 })
    worker.Cancel()
    worker.wg.Wait()
}

func TestWorkerCancelFailurePublishesFailedTerminal(t *testing.T) {
    cli := &interruptTestAdapter{interruptErr: errors.New("Esc write failed")}
    worker, daemonConn, workerConn := newDaemonMessageWorker(t, cli)
    defer daemonConn.Close()
    defer workerConn.Close()
    close(worker.readyCh)
    if !worker.beginTurn() {
        t.Fatal("begin turn")
    }

    worker.wg.Add(1)
    go worker.readDaemonMessages()
    if _, err := protocol.NewMessage(protocol.MsgCancelTurn, worker.sessionID, "").WriteTo(daemonConn); err != nil {
        t.Fatal(err)
    }
    msg := mustReadWorkerMessage(t, daemonConn)
    if msg.Type != protocol.MsgTurnCompleted {
        t.Fatalf("message type = %s, want turn_completed", msg.Type)
    }
    var terminal protocol.TurnTerminal
    if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
        t.Fatal(err)
    }
    if terminal.Status != protocol.TurnFailed || terminal.ErrorCode != "codex_cancel_failed" {
        t.Fatalf("terminal = %#v", terminal)
    }
    worker.Cancel()
    worker.wg.Wait()
}

func TestWorkerRestartDoesNotMarkSessionClosed(t *testing.T) {
    const sessionID = "worker-restart-open"
    storeDir := t.TempDir()
    sessionPath := filepath.Join(storeDir, sessionID+".json")
    fixture, err := json.Marshal(&daemon.PersistedSession{SessionID: sessionID, BotID: "bot-test"})
    if err != nil {
        t.Fatal(err)
    }
    if err := os.WriteFile(sessionPath, fixture, 0o644); err != nil {
        t.Fatal(err)
    }
    listener, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatal(err)
    }
    defer listener.Close()

    w := New(Options{
        SessionID: sessionID, DaemonAddr: listener.Addr().String(),
        CliType: "mock", StoreDir: storeDir,
    })
    runDone := make(chan error, 1)
    go func() { runDone <- w.Run() }()

    conn, err := listener.Accept()
    if err != nil {
        t.Fatal(err)
    }
    defer conn.Close()
    ready, err := protocol.NewMessageReader(conn).Read()
    if err != nil {
        t.Fatal(err)
    }
    if ready.Type != protocol.MsgReady {
        t.Fatalf("first worker message = %s, want ready", ready.Type)
    }
    if _, err := protocol.NewMessage(protocol.MsgRestartWorker, sessionID, "").WriteTo(conn); err != nil {
        t.Fatal(err)
    }
    select {
    case err := <-runDone:
        if err != nil {
            t.Fatalf("Run: %v", err)
        }
    case <-time.After(time.Second):
        t.Fatal("worker did not exit after restart request")
    }
    data, err := os.ReadFile(sessionPath)
    if err != nil {
        t.Fatal(err)
    }
    var persisted daemon.PersistedSession
    if err := json.Unmarshal(data, &persisted); err != nil {
        t.Fatal(err)
    }
    if persisted.Closed {
        t.Fatal("restart persisted closed:true")
    }
}
```

Add the imports used above: `context`, `encoding/json`, `errors`, `net`,
`os`, `path/filepath`, `sync/atomic`, and `botmux-go/internal/adapter`.
Define `mustReadWorkerMessage` beside `waitFor` with the same bounded-read
implementation as `mustReadMessage` in Task 3, but in the `worker` package:

```go
func mustReadWorkerMessage(t *testing.T, conn net.Conn) *protocol.Message {
    t.Helper()
    if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
        t.Fatal(err)
    }
    msg, err := protocol.NewMessageReader(conn).Read()
    _ = conn.SetReadDeadline(time.Time{})
    if err != nil {
        t.Fatal(err)
    }
    return msg
}
```

- [ ] **Step 5: Run worker tests and verify failure**

Run:

```bash
go test ./internal/worker -run 'TestWorker(CancelTurn|Restart|CancelFailure)' -count=1
```

Expected: the worker does not recognize `MsgCancelTurn` or
`MsgRestartWorker`.

- [ ] **Step 6: Implement worker IPC and cleanup modes**

Add the following helpers:

```go
func (w *Worker) interruptTurn() error {
    w.turnMu.Lock()
    active := w.turnInFlight
    w.turnMu.Unlock()
    if !active {
        return errors.New("no Codex turn in progress")
    }
    interrupter, ok := w.cliAdapter.(adapter.CliTurnInterrupter)
    if !ok {
        return errors.New("adapter does not support turn interruption")
    }
    return interrupter.Interrupt(w.ctx)
}

func (w *Worker) failCurrentTurn(code string, err error) {
    w.finishTurn()
    w.outputIdleObserver.CancelInput()
    w.sendTurnTerminal(protocol.TurnTerminal{
        Status: protocol.TurnFailed, ErrorCode: code, ErrorDetail: err.Error(),
    })
}
```

Add daemon message cases:

```go
case protocol.MsgCancelTurn:
    if err := w.interruptTurn(); err != nil {
        log.Printf("[worker:%s] cancel turn: %v", safeShortID(w.sessionID), err)
        w.failCurrentTurn("codex_cancel_failed", err)
    }
case protocol.MsgRestartWorker:
    log.Printf("[worker:%s] restart requested by daemon", safeShortID(w.sessionID))
    w.cleanup(false)
    return
case protocol.MsgClose:
    log.Printf("[worker:%s] close requested by daemon", safeShortID(w.sessionID))
    w.cleanup(true)
    return
```

Change cleanup to accept its persistence decision:

```go
func (w *Worker) cleanup(markClosed bool) {
    w.closeMu.Lock()
    alreadyClosed := w.closed
    w.closed = true
    w.closeMu.Unlock()
    if !alreadyClosed {
        w.cancel()
    }
    if w.startResult != nil {
        if w.startResult.Input != nil {
            _ = w.startResult.Input.Close()
        }
        if w.startResult.Output != nil {
            _ = w.startResult.Output.Close()
        }
    }
    if w.cliAdapter != nil {
        _ = w.cliAdapter.Close()
    }
    w.connMu.Lock()
    if w.conn != nil {
        _ = w.conn.Close()
        w.conn = nil
    }
    w.connMu.Unlock()
    if markClosed && w.storeDir != "" {
        store := daemon.NewSessionStore(w.storeDir)
        _ = store.MarkClosed(w.sessionID)
    }
    log.Printf("[worker:%s] worker exited", safeShortID(w.sessionID))
}
```

No `restart_worker` code path may call `MarkClosed`.

- [ ] **Step 7: Run worker and adapter tests**

Run:

```bash
go test ./internal/adapter ./internal/worker -count=1
go test -race ./internal/adapter ./internal/worker
```

Expected: PASS.

- [ ] **Step 8: Commit worker and adapter behavior**

```bash
git add internal/adapter/adapter.go internal/adapter/codex.go internal/adapter/codex_test.go \
  internal/worker/worker.go internal/worker/worker_cancel_test.go
git commit -m "feat(codex): interrupt turns and restart workers"
```

### Task 3: Add Daemon Cancel Watchdog And Stale Event Rejection

**Files:**
- Modify: `internal/daemon/daemon.go:20-49,466-538,967-1019`
- Create: `internal/daemon/cancel_turn_test.go`

- [ ] **Step 1: Write failing daemon cancellation tests**

Create `internal/daemon/cancel_turn_test.go` with these imports:

```go
import (
    "context"
    "encoding/json"
    "net"
    "os/exec"
    "testing"
    "time"

    "botmux-go/internal/config"
    "botmux-go/internal/protocol"
)
```

Add these complete fixtures. The helper gives the restart grace path a real
worker process while the `net.Pipe` represents the worker IPC connection:

```go
func newCancelTestDaemon(t *testing.T) (*Daemon, *SessionMeta, *WorkerHandle, net.Conn) {
    t.Helper()
    ctx, cancel := context.WithCancel(context.Background())
    meta := NewSessionMeta("cancel-turn-test", "bot-codex")
    meta.CliType = string(config.CliCodex)
    meta.Status = StatusReady
    store := NewSessionStore(t.TempDir())
    if err := store.save(meta.ToPersisted()); err != nil {
        t.Fatal(err)
    }
    daemonConn, workerConn := net.Pipe()
    handle := NewWorkerHandle(meta.SessionID)
    handle.SetConn(daemonConn)
    handle.markReady()
    handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
    if err := handle.Cmd.Start(); err != nil {
        t.Fatal(err)
    }
    handle.Pid = handle.Cmd.Process.Pid
    d := &Daemon{
        ctx: ctx, cancel: cancel, store: store,
        sessions: map[string]*SessionMeta{meta.SessionID: meta},
        workers: map[string]*WorkerHandle{meta.SessionID: handle},
        spawnFailures: make(map[string]*spawnFailure),
        turnCancelTimeout: 20 * time.Millisecond,
        restartWorkerGrace: 20 * time.Millisecond,
    }
    go d.waitWorkerExit(handle)
    t.Cleanup(func() {
        cancel()
        _ = daemonConn.Close()
        _ = workerConn.Close()
        if handle.Cmd.Process != nil {
            _ = handle.Cmd.Process.Kill()
        }
        select {
        case <-handle.ExitDone:
        case <-time.After(time.Second):
            t.Error("worker fixture was not reaped")
        }
    })
    return d, meta, handle, workerConn
}

func mustReadMessage(t *testing.T, conn net.Conn) *protocol.Message {
    t.Helper()
    if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
        t.Fatal(err)
    }
    msg, err := protocol.NewMessageReader(conn).Read()
    _ = conn.SetReadDeadline(time.Time{})
    if err != nil {
        t.Fatal(err)
    }
    return msg
}

func waitForCondition(t *testing.T, timeout time.Duration, predicate func() bool) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if predicate() {
            return
        }
        time.Sleep(time.Millisecond)
    }
    t.Fatal("condition was not met before timeout")
}
```

Add these tests:

```go
func TestCancelTurnSendsIPCAndMarksCancelling(t *testing.T) {
    d, meta, handle, workerConn := newCancelTestDaemon(t)
    defer workerConn.Close()
    d.turnCancelTimeout = time.Second
    if !meta.BeginTurn() {
        t.Fatal("begin turn")
    }
    got := make(chan *protocol.Message, 1)
    go func() { got <- mustReadMessage(t, workerConn) }()

    if err := d.CancelTurn(meta.SessionID); err != nil {
        t.Fatalf("CancelTurn: %v", err)
    }
    msg := <-got
    if msg.Type != protocol.MsgCancelTurn {
        t.Fatalf("message type = %s, want %s", msg.Type, protocol.MsgCancelTurn)
    }
    state := meta.TurnSnapshot()
    if !state.Active || !state.Cancelling {
        t.Fatalf("turn state = %#v, want active cancelling", state)
    }
    if handle != d.workers[meta.SessionID] {
        t.Fatal("cancel replaced worker")
    }
    meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted})
}

func TestCancelTurnTimeoutPublishesOneFailureAndRestartsWorker(t *testing.T) {
    d, meta, handle, workerConn := newCancelTestDaemon(t)
    defer workerConn.Close()
    meta.BeginTurn()
    messages := make(chan *protocol.Message, 2)
    go func() {
        messages <- mustReadMessage(t, workerConn)
        messages <- mustReadMessage(t, workerConn)
    }()

    if err := d.CancelTurn(meta.SessionID); err != nil {
        t.Fatal(err)
    }
    if msg := <-messages; msg.Type != protocol.MsgCancelTurn {
        t.Fatalf("first message = %s, want cancel_turn", msg.Type)
    }
    if msg := <-messages; msg.Type != protocol.MsgRestartWorker {
        t.Fatalf("second message = %s, want restart_worker", msg.Type)
    }
    waitForCondition(t, time.Second, func() bool {
        terminals, _, _ := meta.SnapshotTerminalsSince(0)
        return len(terminals) == 1
    })
    terminals, _, _ := meta.SnapshotTerminalsSince(0)
    if got := terminals[0]; got.Status != protocol.TurnFailed || got.ErrorCode != "codex_cancel_timeout" {
        t.Fatalf("terminal = %#v", got)
    }
    if meta.Closed || meta.Status != StatusRecovering {
        t.Fatalf("session after cancel timeout = closed:%t status:%s", meta.Closed, meta.Status)
    }
    if meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted}) {
        t.Fatal("late terminal after timeout was accepted")
    }
    select {
    case <-handle.ExitDone:
    case <-time.After(time.Second):
        t.Fatal("restart grace did not reap old worker")
    }
}

func TestCancelTurnAbortedTerminalKeepsSessionOpen(t *testing.T) {
    d, meta, handle, workerConn := newCancelTestDaemon(t)
    defer workerConn.Close()
    d.turnCancelTimeout = time.Second
    meta.BeginTurn()
    cancelRead := make(chan *protocol.Message, 1)
    go func() { cancelRead <- mustReadMessage(t, workerConn) }()
    if err := d.CancelTurn(meta.SessionID); err != nil {
        t.Fatal(err)
    }
    if msg := <-cancelRead; msg.Type != protocol.MsgCancelTurn {
        t.Fatalf("message type = %s, want cancel_turn", msg.Type)
    }
    payload, err := json.Marshal(protocol.TurnTerminal{Status: protocol.TurnAborted})
    if err != nil {
        t.Fatal(err)
    }
    d.routeMessage(protocol.NewMessage(protocol.MsgTurnCompleted, meta.SessionID, string(payload)), meta, handle)
    state := meta.TurnSnapshot()
    if state.Active || state.Cancelling || meta.Closed || meta.Status != StatusReady {
        t.Fatalf("after abort: state=%#v closed=%t status=%s", state, meta.Closed, meta.Status)
    }
}

func TestRouteMessageIgnoresTerminalFromStaleWorker(t *testing.T) {
    d, meta, stale, _ := newCancelTestDaemon(t)
    current := NewWorkerHandle(meta.SessionID)
    d.workers[meta.SessionID] = current
    meta.BeginTurn()

    payload, _ := json.Marshal(protocol.TurnTerminal{Status: protocol.TurnAborted})
    d.routeMessage(protocol.NewMessage(protocol.MsgTurnCompleted, meta.SessionID, string(payload)), meta, stale)

    if !meta.TurnSnapshot().Active {
        t.Fatal("stale worker terminal completed current turn")
    }
}
```

- [ ] **Step 2: Run daemon cancellation tests and verify failure**

Run:

```bash
go test ./internal/daemon -run 'Test(CancelTurn|RouteMessageIgnoresTerminal)' -count=1
```

Expected: compile failure because `CancelTurn` and test override duration
fields do not exist.

- [ ] **Step 3: Implement cancellation and restart watchdog**

Add the duration helpers from **Constants And State Contract**, then implement:

```go
func (d *Daemon) CancelTurn(id string) error {
    d.sessionsMu.RLock()
    meta := d.sessions[id]
    d.sessionsMu.RUnlock()
    if meta == nil {
        return fmt.Errorf("session %s not found", id)
    }
    if meta.Closed {
        return fmt.Errorf("session %s is closed", id)
    }
    if meta.CliType != string(config.CliCodex) {
        return fmt.Errorf("session %s does not support turn cancellation", id)
    }

    d.workersMu.RLock()
    handle := d.workers[id]
    d.workersMu.RUnlock()
    if handle == nil || !handle.IsReady() {
        return fmt.Errorf("session %s worker is not ready", id)
    }
    token, ok := meta.BeginTurnCancel()
    if !ok {
        return fmt.Errorf("session %s has no cancellable active turn", id)
    }
    if err := handle.Send(protocol.NewMessage(protocol.MsgCancelTurn, id, "")); err != nil {
        d.finishCancelWithFailure(meta, token, "codex_cancel_failed", err.Error())
        return fmt.Errorf("send cancel_turn: %w", err)
    }
    go d.watchCancelledTurn(meta, token, handle)
    return nil
}

func (d *Daemon) watchCancelledTurn(meta *SessionMeta, token uint64, handle *WorkerHandle) {
    timer := time.NewTimer(d.cancelTurnTimeoutOrDefault())
    defer timer.Stop()
    select {
    case <-d.ctx.Done():
        return
    case <-timer.C:
    }
    if !meta.FailCancellingTurn(token, protocol.TurnTerminal{
        Status: protocol.TurnFailed, ErrorCode: "codex_cancel_timeout",
        ErrorDetail: "Codex did not report turn_aborted after cancellation",
    }) {
        return
    }
    d.sessionsMu.Lock()
    if !meta.Closed {
        meta.Status = StatusRecovering
    }
    d.sessionsMu.Unlock()
    d.restartWorker(handle)
}

func (d *Daemon) finishCancelWithFailure(meta *SessionMeta, token uint64, code, detail string) {
    if !meta.FailCancellingTurn(token, protocol.TurnTerminal{
        Status: protocol.TurnFailed, ErrorCode: code, ErrorDetail: detail,
    }) {
        return
    }
    log.Printf("[daemon] session %s cancel failed: %s", safeShort(meta.SessionID), detail)
}

func (d *Daemon) restartWorker(handle *WorkerHandle) {
    if handle == nil || !d.isCurrentWorker(handle) {
        return
    }
    if err := handle.Send(protocol.NewMessage(protocol.MsgRestartWorker, handle.SessionID, "")); err != nil {
        log.Printf("[daemon] session %s restart IPC: %v", safeShort(handle.SessionID), err)
    }
    go func() {
        timer := time.NewTimer(d.restartWorkerGraceOrDefault())
        defer timer.Stop()
        select {
        case <-handle.ExitDone:
            return
        case <-d.ctx.Done():
            return
        case <-timer.C:
        }
        if handle.Cmd != nil && handle.Cmd.Process != nil {
            _ = handle.Cmd.Process.Kill()
        }
    }()
}
```

`restartWorker` must never invoke `CloseSession` or `store.MarkClosed`.

At the beginning of `routeMessage`, reject messages from a stale worker:

```go
if h != nil && !d.isCurrentWorker(h) {
    log.Printf("[daemon] ignored stale worker message session=%s type=%s",
        safeShort(meta.SessionID), msg.Type)
    return
}
```

In the `MsgTurnCompleted` branch, commit atomically:

```go
if !meta.CompleteTurn(terminal) {
    log.Printf("[daemon] ignored late turn terminal session=%s status=%s",
        safeShort(meta.SessionID), terminal.Status)
}
```

Do not call `PublishTerminal` followed by `FinishTurn`; that split is the
race this task removes.

- [ ] **Step 4: Run daemon cancellation tests**

Run:

```bash
go test ./internal/daemon -run 'Test(CancelTurn|RouteMessageIgnoresTerminal|SessionMeta)' -count=1
go test -race ./internal/daemon
```

Expected: PASS.

- [ ] **Step 5: Commit daemon lifecycle behavior**

```bash
git add internal/daemon/daemon.go internal/daemon/cancel_turn_test.go
git commit -m "feat(daemon): cancel stalled Codex turns safely"
```

### Task 4: Expose Turn State And Cancel Via HTTP

**Files:**
- Modify: `internal/daemon/http_server.go:66-113,282-375,495-565`
- Create: `internal/daemon/http_server_test.go`

- [ ] **Step 1: Write failing HTTP tests**

Create `internal/daemon/http_server_test.go` with `encoding/json`, `net/http`,
`net/http/httptest`, `strings`, `testing`, `time`, and
`botmux-go/internal/protocol` imports. Add these tests:

```go
func TestGetSessionIncludesTurnStateAndLatestTerminal(t *testing.T) {
    d, meta, _, _ := newCancelTestDaemon(t)
    meta.BeginTurn()
    _, _ = meta.BeginTurnCancel()
    meta.CompleteTurn(protocol.TurnTerminal{
        Status: protocol.TurnFailed, ErrorCode: "codex_cancel_timeout",
    })

    req := httptest.NewRequest(http.MethodGet, "/api/sessions/"+meta.SessionID, nil)
    rec := httptest.NewRecorder()
    d.handleGetSession(rec, req, meta.SessionID)

    var got sessionDetailResponse
    if err := json.NewDecoder(rec.Result().Body).Decode(&got); err != nil {
        t.Fatal(err)
    }
    if got.TurnActive || got.TurnCancelling {
        t.Fatalf("turn flags = active:%t cancelling:%t", got.TurnActive, got.TurnCancelling)
    }
    if got.LatestTerminal == nil || got.LatestTerminal.ErrorCode != "codex_cancel_timeout" {
        t.Fatalf("latest terminal = %#v", got.LatestTerminal)
    }
}

func TestCancelEndpointAcceptsOneActiveTurn(t *testing.T) {
    d, meta, _, workerConn := newCancelTestDaemon(t)
    defer workerConn.Close()
    d.turnCancelTimeout = time.Second
    if !meta.BeginTurn() {
        t.Fatal("begin turn")
    }

    req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+meta.SessionID+"/cancel", nil)
    rec := httptest.NewRecorder()
    done := make(chan struct{})
    go func() {
        d.handleSessionsSubrouter(rec, req)
        close(done)
    }()
    msg := mustReadMessage(t, workerConn)
    if msg.Type != protocol.MsgCancelTurn {
        t.Fatalf("message type = %s, want cancel_turn", msg.Type)
    }
    <-done
    if rec.Code != http.StatusAccepted {
        t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
    }

    duplicateReq := httptest.NewRequest(http.MethodPost, "/api/sessions/"+meta.SessionID+"/cancel", nil)
    duplicateRec := httptest.NewRecorder()
    d.handleSessionsSubrouter(duplicateRec, duplicateReq)
    if duplicateRec.Code != http.StatusConflict {
        t.Fatalf("duplicate status = %d, want %d", duplicateRec.Code, http.StatusConflict)
    }
    if !strings.Contains(duplicateRec.Body.String(), "cancellable active turn") {
        t.Fatalf("duplicate response = %s", duplicateRec.Body.String())
    }
    meta.CompleteTurn(protocol.TurnTerminal{Status: protocol.TurnAborted})
}

func TestCancelEndpointRejectsIneligibleSessionWithoutIPC(t *testing.T) {
    cases := []struct {
        name   string
        prepare func(*SessionMeta)
    }{
        {
            name: "closed",
            prepare: func(meta *SessionMeta) {
                meta.Closed = true
            },
        },
        {
            name: "non Codex",
            prepare: func(meta *SessionMeta) {
                meta.CliType = "mock"
            },
        },
        {
            name: "idle",
            prepare: func(*SessionMeta) {},
        },
    }
    for _, tt := range cases {
        t.Run(tt.name, func(t *testing.T) {
            d, meta, _, workerConn := newCancelTestDaemon(t)
            defer workerConn.Close()
            tt.prepare(meta)

            req := httptest.NewRequest(
                http.MethodPost,
                "/api/sessions/"+meta.SessionID+"/cancel",
                nil,
            )
            rec := httptest.NewRecorder()
            done := make(chan struct{})
            go func() {
                d.handleSessionsSubrouter(rec, req)
                close(done)
            }()
            select {
            case <-done:
            case <-time.After(time.Second):
                t.Fatal("ineligible cancellation tried to send worker IPC")
            }
            if rec.Code != http.StatusConflict {
                t.Fatalf("status = %d, want %d; body=%s",
                    rec.Code, http.StatusConflict, rec.Body.String())
            }
            state := meta.TurnSnapshot()
            if state.Active || state.Cancelling {
                t.Fatalf("ineligible cancellation changed state: %#v", state)
            }
        })
    }
}

func TestCancelEndpointReturnsNotFoundForUnknownSession(t *testing.T) {
    d, _, _, _ := newCancelTestDaemon(t)
    req := httptest.NewRequest(http.MethodPost, "/api/sessions/missing/cancel", nil)
    rec := httptest.NewRecorder()
    d.handleSessionsSubrouter(rec, req)
    if rec.Code != http.StatusNotFound {
        t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
    }
}
```

- [ ] **Step 2: Run HTTP tests and verify they fail**

Run:

```bash
go test ./internal/daemon -run 'Test(GetSessionIncludesTurnState|CancelEndpoint)' -count=1
```

Expected: compile failure because the response fields and `/cancel` route do
not exist.

- [ ] **Step 3: Implement HTTP response and cancel route**

Extend the detail response:

```go
TurnActive      bool                   `json:"turn_active"`
TurnCancelling  bool                   `json:"turn_cancelling"`
LatestTerminal  *protocol.TurnTerminal `json:"latest_terminal,omitempty"`
```

Build those fields from `turn := m.TurnSnapshot()` in
`handleGetSession`.

Route `POST /api/sessions/{sid}/cancel`:

```go
case http.MethodPost:
    switch sub {
    case "send":
        d.handleSessionSend(w, r, sid)
    case "cancel":
        d.handleSessionCancel(w, r, sid)
    default:
        d.writeError(w, http.StatusNotFound, "unknown subpath: "+sub)
    }
```

Use this response and status mapping:

```go
type cancelTurnResponse struct {
    OK        bool   `json:"ok"`
    SessionID string `json:"session_id"`
    Cancelling bool  `json:"cancelling"`
}

func (d *Daemon) handleSessionCancel(w http.ResponseWriter, _ *http.Request, sid string) {
    if _, ok := d.GetSessionMeta(sid); !ok {
        d.writeError(w, http.StatusNotFound, "session not found: "+sid)
        return
    }
    if err := d.CancelTurn(sid); err != nil {
        d.writeError(w, http.StatusConflict, err.Error())
        return
    }
    d.writeJSON(w, http.StatusAccepted, cancelTurnResponse{
        OK: true, SessionID: sid, Cancelling: true,
    })
}
```

- [ ] **Step 4: Run HTTP tests**

Run:

```bash
go test ./internal/daemon -run 'Test(GetSessionIncludesTurnState|CancelEndpoint)' -count=1
go test ./internal/daemon -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit HTTP API**

```bash
git add internal/daemon/http_server.go internal/daemon/http_server_test.go
git commit -m "feat(api): expose and cancel Codex turns"
```

### Task 5: Implement Multiline Dashboard Composer And Cancel Controls

**Files:**
- Modify: `internal/daemon/dashboard.html:43-49,84-87,401-486,544-570`
- Create: `internal/daemon/dashboard_test.go`

- [ ] **Step 1: Write failing embedded-Dashboard source tests**

Create `internal/daemon/dashboard_test.go`:

```go
package daemon

import (
    "strings"
    "testing"
)

func TestDashboardUsesMultilineComposerAndShortcut(t *testing.T) {
    data, err := dashboardFS.ReadFile("dashboard.html")
    if err != nil {
        t.Fatal(err)
    }
    page := string(data)
    for _, want := range []string{
        `<textarea id="chat-input"`,
        `event.metaKey || event.ctrlKey`,
        `function handleComposerKey`,
        `message: msg`,
    } {
        if !strings.Contains(page, want) {
            t.Fatalf("dashboard missing %q", want)
        }
    }
    if strings.Contains(page, `<input type="text" id="chat-input"`) {
        t.Fatal("dashboard still has single-line chat input")
    }
}

func TestDashboardRendersAndCallsTurnCancel(t *testing.T) {
    data, err := dashboardFS.ReadFile("dashboard.html")
    if err != nil {
        t.Fatal(err)
    }
    page := string(data)
    for _, want := range []string{
        `id="chat-cancel-btn"`,
        `function cancelTurn`,
        `'/cancel'`,
        `turn_active`,
        `turn_cancelling`,
        `function updateChatControls`,
    } {
        if !strings.Contains(page, want) {
            t.Fatalf("dashboard missing %q", want)
        }
    }
}
```

- [ ] **Step 2: Run Dashboard tests and verify they fail**

Run:

```bash
go test ./internal/daemon -run TestDashboard -count=1
```

Expected: FAIL because the page has a single-line input and no cancel controls.

- [ ] **Step 3: Implement the multiline composer**

Replace the input CSS with:

```css
.chat-input textarea {
  flex: 1; min-height: 64px; max-height: 180px; resize: vertical;
  background: var(--bg); border: 1px solid var(--border); color: var(--fg);
  padding: 8px 10px; border-radius: 4px; font-family: inherit;
  font-size: 12px; line-height: 1.45; outline: none;
}
.chat-input textarea:focus { border-color: var(--accent); }
.chat-input textarea:disabled { opacity: 0.5; }
.btn-icon { width: 30px; justify-content: center; padding: 4px; }
```

Replace the existing input initialization with a state-derived disabled
attribute and render the composer with Enter preserved and the send shortcut:

```javascript
var disabled = composerDisabled(s) ? 'disabled' : '';
html += '<textarea id="chat-input" rows="3" placeholder="Type a message..." ' +
  disabled + ' onkeydown="handleComposerKey(event, \'' + escapeHtml(sid) +
  '\')" autofocus></textarea>';
html += '<button class="btn btn-primary" id="chat-send-btn" onclick="sendMessage(\'' +
  escapeHtml(sid) + '\')" ' + disabled + '>Send</button>';
```

Add:

```javascript
function handleComposerKey(event, sid) {
  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
    event.preventDefault();
    sendMessage(sid);
  }
}
```

Replace `sendMessage` with this implementation. It preserves the submitted
payload, makes the UI non-interactive while the request is in flight, and
locally renders the active-turn state as soon as the daemon accepts it:

```javascript
async function sendMessage(sid) {
  var input = document.getElementById('chat-input');
  var send = document.getElementById('chat-send-btn');
  if (!input) return;
  var msg = input.value;
  if (!msg.trim()) return;

  input.disabled = true;
  if (send) send.disabled = true;
  var resp = await api('/api/sessions/' + encodeURIComponent(sid) + '/send', {
    method: 'POST', body: { message: msg }
  });
  if (resp && resp.ok) {
    input.value = '';
    updateChatControls({ turn_active: true, turn_cancelling: false });
    await new Promise(function(resolve) { setTimeout(resolve, 600); });
    await loadChatSession(sid);
    updateChatMessages(chatSessionData);
    updateChatHeader(sid, chatSessionData);
    updateChatSidebar(sid);
    updateChatControls(chatSessionData);
    return;
  }
  updateChatControls(chatSessionData);
  input.focus();
}
```

Do not call `.trim()` on the transmitted payload.

- [ ] **Step 4: Implement state-aware cancel controls**

Add:

```javascript
function composerDisabled(s) {
  return !s || s.closed || s.turn_active || s.turn_cancelling;
}

function updateChatControls(s) {
  var disabled = composerDisabled(s);
  var input = document.getElementById('chat-input');
  var send = document.getElementById('chat-send-btn');
  var cancel = document.getElementById('chat-cancel-btn');
  if (input) input.disabled = disabled;
  if (send) send.disabled = disabled;
  if (cancel) {
    var canCancel = !!(s && !s.closed && s.turn_active);
    cancel.hidden = !canCancel;
    cancel.disabled = !canCancel || !!s.turn_cancelling;
    cancel.title = s && s.turn_cancelling ? 'Cancelling Codex turn' : 'Cancel current Codex turn';
  }
}

async function cancelTurn(sid) {
  var cancel = document.getElementById('chat-cancel-btn');
  if (cancel) cancel.disabled = true;
  var resp = await api('/api/sessions/' + encodeURIComponent(sid) + '/cancel', {
    method: 'POST'
  });
  if (resp && resp.ok) {
    toast('Cancelling Codex turn...', 'info');
    updateChatControls({ turn_active: true, turn_cancelling: true });
    await loadChatSession(sid);
    updateChatControls(chatSessionData);
    return;
  }
  updateChatControls(chatSessionData);
}
```

In `renderChat`, add this initially hidden button for every open session. It
must already exist before a message is sent so `updateChatControls` can reveal
it without rebuilding the chat page:

```javascript
if (s && !s.closed) {
  html += '<button class="btn btn-sm btn-danger btn-icon" id="chat-cancel-btn" ' +
    'onclick="cancelTurn(\'' + escapeHtml(sid) + '\')" ' +
    'title="Cancel current Codex turn" aria-label="Cancel current Codex turn" hidden>' +
    '&#9632;</button>';
}
```

Use `composerDisabled(s)` in the initial render. Replace the final ad-hoc
input/button disable logic in `poll()` with:

```javascript
updateChatControls(chatSessionData);
```

The successful-send path above calls `updateChatControls` before the next poll
so a second click cannot submit a duplicate turn and the pre-rendered cancel
button becomes visible.

- [ ] **Step 5: Run Dashboard tests and all daemon tests**

Run:

```bash
go test ./internal/daemon -run TestDashboard -count=1
go test ./internal/daemon -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Dashboard functionality**

```bash
git add internal/daemon/dashboard.html internal/daemon/dashboard_test.go
git commit -m "feat(dashboard): support multiline prompts and turn cancel"
```

### Task 6: Extend Acceptance Coverage And Document The Recovery Contract

**Files:**
- Modify: `scripts/v6_codex_e2e.sh`
- Modify: `docs/versions/v6-architecture.md`

- [ ] **Step 1: Add the opt-in real Codex cancellation scenario**

Replace the fixed-message `send_after_restore` helper with the following
parameterized retry helper. It verifies both the original daemon-restart
resume path and the timeout-recovery replacement-worker path:

```bash
send_when_worker_ready() {
  local prompt=$1
  local output
  for _ in $(seq 1 15); do
    if output=$("$BIN" -cmd send "$SID" "$prompt" 2>&1); then
      printf '%s\n' "$output"
      return 0
    fi
    case "$output" in
      *"worker not available"*|*"worker not connected"*)
        sleep 1
        ;;
      *)
        printf '%s\n' "$output" >&2
        return 1
        ;;
    esac
  done
  echo "worker did not become ready" >&2
  return 1
}
```

Append this opt-in cancellation helper:

```bash
run_cancel_scenario() {
  local send_url="http://127.0.0.1:17891/api/sessions/${SID}/send"
  local cancel_url="http://127.0.0.1:17891/api/sessions/${SID}/cancel"
  local detail_url="http://127.0.0.1:17891/api/sessions/${SID}"

  curl --fail --silent --show-error \
    -H 'Content-Type: application/json' \
    -X POST "$send_url" \
    --data '{"message":"Investigate this repository in depth and do not provide a final answer until you have inspected at least twenty files."}' \
    >/tmp/botmux-go-v6-cancel-send.json
  sleep 1
  curl --fail --silent --show-error -X POST "$cancel_url" \
    >/tmp/botmux-go-v6-cancel.json

  for _ in $(seq 1 30); do
    local detail
    detail=$(curl --fail --silent --show-error "$detail_url")
    if printf '%s' "$detail" | grep -q '"turn_active":false' &&
      printf '%s' "$detail" | grep -Eq '"status":"(aborted|failed)"'; then
      printf '%s\n' "$detail"
      return 0
    fi
    sleep 1
  done
  echo "cancelled turn did not reach a terminal state" >&2
  return 1
}
```

Invoke it only when the caller explicitly opts in:

```bash
# Replace the existing post-restart send_after_restore invocation with:
send_when_worker_ready "What were the two markers from the previous turn?"

if [[ "${RUN_CODEX_CANCEL_SCENARIO:-0}" == "1" ]]; then
  run_cancel_scenario
  output=$(send_when_worker_ready "Return exactly: CODEX_V6_AFTER_CANCEL")
  printf '%s\n' "$output"
  grep -F "CODEX_V6_AFTER_CANCEL" <<<"$output"
fi
```

This remains opt-in because it makes a real model call and intentionally
starts a turn that may consume tokens.

- [ ] **Step 2: Document the operational behavior**

Add a “Turn cancellation” section to `docs/versions/v6-architecture.md`:

```markdown
## Turn Cancellation

Codex sessions accept one active turn. Dashboard cancellation sends Esc to the
existing Codex TUI and keeps the native session alive when Codex emits
`turn_aborted`. After 10 seconds without a terminal event, botmux-go commits
one `failed` terminal with `codex_cancel_timeout`, exits the worker without
marking the session closed, and lets the session monitor start
`codex resume <native-session-id>`. A late event from the old worker is
ignored.
```

- [ ] **Step 3: Run non-billed verification**

Run:

```bash
bash -n scripts/v6_codex_e2e.sh
go test ./...
go test -race ./...
go build ./...
go vet ./...
git diff --check
```

Expected: every command succeeds. Do not run the real Codex scenario unless
the user explicitly asks:

```bash
RUN_CODEX_CANCEL_SCENARIO=1 ./scripts/v6_codex_e2e.sh
```

- [ ] **Step 4: Commit verification artifacts and documentation**

```bash
git add scripts/v6_codex_e2e.sh docs/versions/v6-architecture.md
git commit -m "test(codex): cover cancelled turn recovery"
```

## Final Review Checklist

- [ ] `SessionMeta` increments a token on every accepted turn.
- [ ] The daemon is the only component that commits terminal state for a
  successful cancellation timeout.
- [ ] `turn_aborted` clears the gate without closing the native session.
- [ ] `restart_worker` never writes `closed:true`; only `close` does.
- [ ] Timeout terminal and a late old-worker terminal cannot both be published.
- [ ] Dashboard preserves multiline content and sends only on Cmd/Ctrl+Enter.
- [ ] Dashboard disables Send during active/cancelling turns and displays a
  labelled cancel icon only when cancellation is valid.
- [ ] HTTP detail exposes `turn_active`, `turn_cancelling`, and
  `latest_terminal`.
- [ ] Closed, non-Codex, idle, and duplicate cancel requests return a client
  error without sending worker IPC.
- [ ] Real acceptance remains explicit and token-billed only when opted in.
