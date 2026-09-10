# Worker Instance Nonce Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bind each worker TCP connection and close operation to one daemon worker generation and one `SessionMeta` instance.

**Architecture:** The daemon generates an ephemeral 128-bit nonce for every spawned `WorkerHandle`, passes it through worker process environment, and accepts a worker socket only when its first `READY` message returns the matching nonce. `CloseSession` captures a `SessionMeta` pointer and closes only the exact owned handle, while persisting closure before another same-ID session can be installed.

**Tech Stack:** Go 1.22, `crypto/rand`, JSON-over-TCP IPC, `os/exec` environment, Go standard-library tests.

---

## File Map

| File | Change |
|---|---|
| `internal/protocol/protocol.go` | Add the optional worker instance ID field to IPC messages. |
| `internal/daemon/worker_handle.go` | Hold the daemon-generated instance ID for one worker generation. |
| `internal/daemon/daemon.go` | Generate and pass nonce, authenticate initial worker connection, and fence close by `SessionMeta` identity. |
| `internal/daemon/worker_ready_test.go` | Verify valid, missing, and mismatched worker nonce admission. |
| `internal/daemon/daemon_close_test.go` | Verify stale close cannot close a replacement same-ID session or worker. |
| `internal/worker/worker.go` | Store the worker instance ID and include it in worker-originated IPC messages. |
| `internal/worker/worker_ready_test.go` | Verify `READY` transmits the configured nonce. |
| `cmd/daemon/main.go` | Read `BOTMUX_WORKER_INSTANCE_ID` and pass it to `worker.Options`. |
| `cmd/daemon/main_test.go` | Verify worker environment options include the nonce. |
| `docs/versions/v6-architecture.md` | Document worker-generation authentication. |

## Task 1: Carry The Nonce Through IPC And Worker Startup

**Files:**
- Modify: `internal/protocol/protocol.go`
- Modify: `internal/worker/worker.go`
- Modify: `internal/worker/worker_ready_test.go`
- Modify: `cmd/daemon/main.go`
- Create: `cmd/daemon/main_test.go`

- [ ] **Step 1: Write failing worker and command wiring tests**

Add a worker test that constructs:

```go
w := New(Options{
    SessionID:        "session-1",
    CliType:          "mock",
    WorkerInstanceID: "nonce-123",
})
```

Inject a `net.Pipe` connection, call `w.sendReady()`, decode the message from
the other end, and require:

```go
msg.Type == protocol.MsgReady
msg.WorkerInstanceID == "nonce-123"
```

Extract worker option construction in `cmd/daemon/main.go` into a package-local
helper accepting an environment lookup function. Add a command test requiring
`BOTMUX_WORKER_INSTANCE_ID=nonce-123` to populate
`worker.Options.WorkerInstanceID`.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./internal/worker ./cmd/daemon -run 'Test.*WorkerInstance' -count=1
```

Expected: compile failure because `WorkerInstanceID` does not exist.

- [ ] **Step 3: Implement nonce plumbing**

Add the wire field:

```go
WorkerInstanceID string `json:"worker_instance_id,omitempty"`
```

to `protocol.Message`. Add `WorkerInstanceID` to `worker.Options` and
`Worker`, then set it on every message created by `Worker.sendMessage`:

```go
m := protocol.NewMessage(typ, w.sessionID, payload)
m.WorkerInstanceID = w.workerInstanceID
```

Define `EnvWorkerInstanceID = "BOTMUX_WORKER_INSTANCE_ID"` in
`cmd/daemon/main.go`; read it while creating `worker.Options`.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
go test ./internal/protocol ./internal/worker ./cmd/daemon -count=1
git add internal/protocol/protocol.go internal/worker/worker.go \
  internal/worker/worker_ready_test.go cmd/daemon/main.go cmd/daemon/main_test.go
git commit -m "feat(ipc): carry worker instance identity"
```

## Task 2: Authenticate Worker Connection Generations

**Files:**
- Modify: `internal/daemon/worker_handle.go`
- Modify: `internal/daemon/daemon.go`
- Modify: `internal/daemon/worker_ready_test.go`

- [ ] **Step 1: Write failing daemon handshake tests**

Extend the ready fixture so its current handle has:

```go
handle.InstanceID = "expected-nonce"
```

Add tests that send the first worker `READY` over `net.Pipe`:

```go
func TestHandleConnRejectsReadyWithMissingWorkerInstanceID(t *testing.T)
func TestHandleConnRejectsReadyWithMismatchedWorkerInstanceID(t *testing.T)
func TestHandleConnAcceptsReadyWithMatchingWorkerInstanceID(t *testing.T)
```

The mismatch cases must not replace `handle.Conn`, must not close
`handle.Ready`, and must receive `MsgError`. The matching case must attach the
socket and close `handle.Ready`.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./internal/daemon -run 'TestHandleConn.*WorkerInstanceID' -count=1
```

Expected: missing field or rejected-message assertions fail.

- [ ] **Step 3: Generate, propagate, and validate the nonce**

Add `InstanceID string` to `WorkerHandle`. In `daemon.go`, generate 16 random
bytes with `crypto/rand.Read`, encode them with `hex.EncodeToString`, and
assign the result before installing a spawned handle:

```go
instanceID, err := newWorkerInstanceID()
if err != nil {
    return fmt.Errorf("generate worker instance ID: %w", err)
}
handle.InstanceID = instanceID
```

Pass it to the worker:

```go
"BOTMUX_WORKER_INSTANCE_ID="+handle.InstanceID,
```

In `handleConn`, require the first worker message to satisfy:

```go
first.Type == protocol.MsgReady &&
first.WorkerInstanceID != "" &&
first.WorkerInstanceID == h.InstanceID
```

Reject connections without an existing expected handle. Do not create a
handle from an incoming worker socket. Perform validation before `SetConn`.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
go test ./internal/daemon ./internal/worker ./cmd/daemon -count=1
go test -race ./internal/daemon ./internal/worker -count=1
git add internal/daemon/daemon.go internal/daemon/worker_handle.go \
  internal/daemon/worker_ready_test.go
git commit -m "fix(daemon): authenticate worker generations"
```

## Task 3: Fence Session Close To The Captured Instance

**Files:**
- Modify: `internal/daemon/daemon.go`
- Modify: `internal/daemon/daemon_close_test.go`

- [ ] **Step 1: Write a failing replacement-close test**

Create an old and a replacement `SessionMeta` with the same ID. Capture the
old pointer for a close operation, replace daemon maps with the new meta and
its owned worker, then invoke the pointer-based close helper for the old meta.
Assert the replacement remains open, its worker receives no `MsgClose`, and
its persisted JSON remains `closed:false`.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./internal/daemon -run TestCloseSessionDoesNotCloseReusedSession -count=1
```

Expected: the ID-based close path closes the replacement or the helper does
not exist.

- [ ] **Step 3: Implement pointer-fenced close**

Refactor `CloseSession` to capture the current meta then call:

```go
func (d *Daemon) closeSessionMeta(meta *SessionMeta, reason string)
```

While holding the session lock, require `d.sessions[meta.SessionID] == meta`,
mark only that meta closed, and persist its closure before unlocking. Capture
only a handle owned by that exact meta under the existing worker/session lock
order. After unlock, send `MsgClose` and schedule a grace kill only for the
captured old handle.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
go test ./internal/daemon -count=1
go test -race ./internal/daemon -count=1
git add internal/daemon/daemon.go internal/daemon/daemon_close_test.go
git commit -m "fix(daemon): fence close to session instance"
```

## Task 4: Document And Verify The Contract

**Files:**
- Modify: `docs/versions/v6-architecture.md`

- [ ] **Step 1: Document worker instance authentication**

Add a short section describing:

```markdown
Each worker generation has a daemon-issued ephemeral instance nonce. The
worker returns it in its initial READY message; the daemon rejects connections
that do not match the current handle. A daemon restart deliberately replaces
old workers instead of accepting an inherited connection.
```

- [ ] **Step 2: Run full verification**

Run:

```bash
go test ./...
go test -race ./...
go build ./...
go vet ./...
git diff --check
```

- [ ] **Step 3: Commit documentation**

```bash
git add docs/versions/v6-architecture.md
git commit -m "docs(v6): describe worker instance authentication"
```

## Final Checklist

- [ ] Every spawned worker gets a non-empty random instance nonce.
- [ ] Worker `READY` returns the exact nonce.
- [ ] Missing, stale, or mismatched worker connections never replace current
  handles.
- [ ] Same-generation reconnects are accepted.
- [ ] Old daemon workers are rejected after daemon restart until the monitor
  spawns replacements.
- [ ] Close cannot target or persistently close a replacement same-ID session.
- [ ] Full standard and race verification passes.
