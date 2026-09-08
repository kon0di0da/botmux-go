#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=/tmp/botmux-go-v6-codex
LOG=/tmp/botmux-go-v6-codex-daemon.log
PID=/tmp/botmux-go-v6-codex.pid
SID="v6-codex-$(date +%s)"

cd "$ROOT"
go build -o "$BIN" ./cmd/daemon
rm -rf "$HOME/.botmux-go/sessions"
mkdir -p "$HOME/.botmux-go/sessions"
"$BIN" -config ./configs/bots.json >"$LOG" 2>&1 &
echo $! >"$PID"
trap 'kill "$(cat "$PID")" 2>/dev/null || true' EXIT

"$BIN" -cmd new "$SID" bot-codex
"$BIN" -cmd send "$SID" $'Return exactly two lines:\nCODEX_V6_ONE\nCODEX_V6_TWO'

kill "$(cat "$PID")"
"$BIN" -config ./configs/bots.json >"${LOG%.log}-resume.log" 2>&1 &
echo $! >"$PID"
sleep 1
"$BIN" -cmd send "$SID" "What were the two markers from the previous turn?"
