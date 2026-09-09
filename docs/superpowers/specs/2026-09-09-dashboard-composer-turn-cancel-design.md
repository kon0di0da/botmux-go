# Dashboard Composer And Codex Turn Cancellation Design

## Goal

Make the local Codex dashboard usable for development prompts that span multiple
lines and let a user interrupt a running Codex turn without discarding the
native Codex session.

This change preserves the V6 single-turn contract. It does not add type-ahead,
adopt, or Codex RPC support.

## Scope

The change contains two related capabilities:

1. Replace the chat single-line input with a multiline composer.
2. Add a per-turn cancel operation with an Esc-first, restart-on-timeout
   fallback.

## Dashboard Composer

The chat view uses a `textarea` instead of an `input`.

- Enter inserts a newline.
- Cmd+Enter on macOS and Ctrl+Enter on other platforms submit the message.
- The submitted payload preserves the entered whitespace and newlines.
- A whitespace-only message is rejected locally.
- The composer is disabled while the session is closed, cancelling, or has an
  active Codex turn.

The existing Send command remains available. The UI uses an icon button with a
tooltip for cancel while a turn is active.

## Turn State

`SessionMeta` owns the canonical turn state:

- `turnActive` means a Codex input has been accepted and no terminal result has
  been committed.
- `turnCancelling` means a cancel request has been accepted for that active
  turn.

The state API must support these transitions:

```text
idle -> active -> cancelling -> idle
idle -> active -> idle
active/cancelling -> recovering -> idle
```

Only the first terminal transition from active or cancelling to idle is
accepted. Late terminal messages from an old worker are ignored. This ensures
that cancel timeout and a later Codex `turn_aborted` cannot create duplicate
terminal results.

HTTP session detail responses expose `turn_active`, `turn_cancelling`, and the
latest terminal result so the dashboard can render the current turn state and
asynchronous cancellation failures.

## Cancel Flow

The daemon accepts a `cancel_turn` request only for an active Codex turn that
is not already cancelling. It marks the turn as cancelling before sending IPC
to the worker.

The worker receives `cancel_turn` and calls the Codex adapter interrupt
operation. The adapter writes one Esc byte (`0x1b`) to the Codex PTY. It does
not close the adapter or session.

If Codex records `turn_aborted` in its rollout within ten seconds, the existing
structured terminal path publishes an `aborted` terminal, clears the turn
state, and leaves the worker and native Codex session running.

## Timeout Recovery

If the turn remains cancelling after ten seconds:

1. The daemon moves the session to `RECOVERING`.
2. It publishes one failed terminal with error code `codex_cancel_timeout`.
3. It clears the active/cancelling turn state.
4. It sends `restart_worker` to the current worker.

The worker handles `restart_worker` by closing its adapter, PTY, and daemon
connection, then exits without marking the session closed. Its exit watcher
removes the old handle. The existing session monitor starts a replacement
worker, which receives the persisted native Codex session ID and resumes with
`codex resume <id>`.

If the worker does not exit within two seconds of `restart_worker`, the daemon
kills that worker process. The session remains open and recovering so the
monitor can still resume it.

## IPC And Adapter Contracts

Add daemon-to-worker messages:

- `cancel_turn`
- `restart_worker`

Add an optional adapter capability for turn interruption. Codex implements it
by writing Esc to its existing PTY. Adapters without that capability reject
per-turn cancellation rather than closing the entire session.

## Error Handling

- Cancel on a closed, non-Codex, idle, or already-cancelling session returns a
  client error and makes no process change.
- A failed Esc write produces a failed terminal and releases the turn gate.
- Restart is a worker lifecycle action, not `CloseSession`; it must not write
  `closed:true` to the session store.
- Late `turn_aborted` or completed events after timeout recovery are ignored.

## Verification

Automated tests cover:

1. Multiline dashboard composer markup and shortcut behavior.
2. Codex interrupt writes exactly Esc.
3. Normal rollout `turn_aborted` releases the turn and preserves the session.
4. Cancel timeout produces one failed terminal, restarts the worker, and leaves
   the session recoverable.
5. A late terminal after timeout does not produce a second terminal.
6. A restart worker does not mark the session JSON as closed.

The real Codex acceptance script is extended with a long-running turn, cancel,
and a subsequent prompt that confirms resume remains usable.
