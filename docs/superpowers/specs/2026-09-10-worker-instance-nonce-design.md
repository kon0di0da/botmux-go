# Worker Instance Nonce Design

## Goal

Prevent a worker process from attaching to a different daemon worker generation
after a session ID has been purged and reused.

## Problem

Worker IPC currently authenticates a connection with only `session_id`. A stale
worker can reconnect after a purge/recreate cycle and present the same session
ID. The daemon can then attach that socket to the current session's worker
handle, allowing stale messages or daemon commands to cross the session
instance boundary.

`SessionMeta` pointer ownership prevents several in-memory lifecycle races, but
it cannot authenticate a new TCP connection from an old worker process.

## Chosen Design

Each daemon-spawned worker generation receives a cryptographically random,
non-persistent instance nonce.

1. `spawnWorkerForSession` generates a 128-bit random nonce and stores it in
   the new `WorkerHandle`.
2. The daemon passes the nonce to the worker through
   `BOTMUX_WORKER_INSTANCE_ID`.
3. `cmd/daemon` reads that environment variable into `worker.Options`.
4. The worker includes the nonce in every worker-originated protocol message.
   In particular, the first `MsgReady` carries it.
5. Before attaching a worker TCP connection, the daemon requires the first
   worker message to be `MsgReady` with a nonce equal to the currently
   registered handle's nonce. It rejects a missing, unknown, or mismatched
   nonce without replacing the handle's connection.
6. A reconnect from the same live worker is accepted because it retains its
   nonce. A daemon restart intentionally rejects inherited workers because the
   fresh daemon has no matching handle; the existing session monitor spawns a
   new worker generation.

The nonce is not persisted in the session JSON. It is a capability for one
daemon process and one worker process, not durable session state.

## Session Close Fence

`CloseSession` also operates on the exact `SessionMeta` instance captured at
entry. It must not send `MsgClose` to a replacement worker or mark a
replacement persisted session closed after a same-ID purge/recreate. The close
path validates the current session pointer before worker and store side
effects.

## Protocol Contract

`protocol.Message` gains:

```go
WorkerInstanceID string `json:"worker_instance_id,omitempty"`
```

Client messages leave this field empty. Worker-originated messages carry the
worker's current nonce. The daemon validates it only at worker connection
admission; subsequent messages are trusted because they share the admitted
socket and continue through the current-handle/session-owner checks.

## Error Handling

- A mismatched initial nonce receives `MsgError` and the daemon closes the
  socket.
- A worker with no expected current handle is rejected rather than creating a
  handle from its connection.
- Nonce generation failure aborts worker spawning before process start.
- A stale `CloseSession` capture becomes a no-op after a session instance is
  replaced.

## Tests

Tests cover:

- daemon spawn supplies a nonce and worker command wiring reads it;
- worker `READY` includes the nonce;
- matching nonce attaches and reaches `READY`;
- mismatched and missing nonce cannot replace a current worker connection;
- no-handle worker connection is rejected;
- a stale `CloseSession` capture does not close a replacement same-ID worker
  or persist the replacement session as closed.

## Scope

This design does not introduce remote authentication, persisted worker
credentials, or cross-daemon worker takeover. It only binds a local daemon
worker generation to its IPC connection.
