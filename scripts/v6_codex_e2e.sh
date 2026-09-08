#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=/tmp/botmux-go-v6-codex
LOG=/tmp/botmux-go-v6-codex-daemon.log
PID=/tmp/botmux-go-v6-codex.pid
SID="v6-codex-$(date +%s)"

wait_for_daemon() {
  for _ in $(seq 1 100); do
    if nc -z 127.0.0.1 17890 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "daemon did not listen on 127.0.0.1:17890" >&2
  cat "$LOG" >&2 || true
  return 1
}

send_after_restore() {
  local output
  for _ in $(seq 1 15); do
    if output=$("$BIN" -cmd send "$SID" "What were the two markers from the previous turn?" 2>&1); then
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
  echo "restored worker did not become ready" >&2
  return 1
}

cd "$ROOT"
go build -o "$BIN" ./cmd/daemon
rm -rf "$HOME/.botmux-go/sessions"
mkdir -p "$HOME/.botmux-go/sessions"
"$BIN" -config ./configs/bots.json >"$LOG" 2>&1 &
echo $! >"$PID"
trap 'kill "$(cat "$PID")" 2>/dev/null || true' EXIT
wait_for_daemon

"$BIN" -cmd new "$SID" bot-codex
"$BIN" -cmd send "$SID" $'Return exactly two lines:\nCODEX_V6_ONE\nCODEX_V6_TWO'

kill "$(cat "$PID")"
"$BIN" -config ./configs/bots.json >"${LOG%.log}-resume.log" 2>&1 &
echo $! >"$PID"
wait_for_daemon
send_after_restore
