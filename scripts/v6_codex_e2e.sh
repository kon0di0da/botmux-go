#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=/tmp/botmux-go-v6-codex
LOG=/tmp/botmux-go-v6-codex-daemon.log
PID=/tmp/botmux-go-v6-codex.pid
SID="v6-codex-$(date +%s)"

stop_daemon() {
  local pid

  if [[ ! -f "$PID" ]]; then
    return 0
  fi
  pid=$(<"$PID")
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$PID"
}

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
  echo "restored worker did not become ready" >&2
  return 1
}

run_cancel_scenario() {
  local send_url="http://127.0.0.1:17891/api/sessions/${SID}/send"
  local cancel_url="http://127.0.0.1:17891/api/sessions/${SID}/cancel"
  local detail_url="http://127.0.0.1:17891/api/sessions/${SID}"
  local send_response
  local cancel_response
  local detail_response

  send_response=$(mktemp /tmp/botmux-go-v6-codex-cancel-send.XXXXXX)
  cancel_response=$(mktemp /tmp/botmux-go-v6-codex-cancel-cancel.XXXXXX)
  detail_response=$(mktemp /tmp/botmux-go-v6-codex-cancel-detail.XXXXXX)

  curl -fsS -X POST -H 'Content-Type: application/json' \
    --data-binary @- "$send_url" >"$send_response" <<'EOF'
{"message":"Conduct a thorough investigation of this repository. Trace the daemon startup path, worker lifecycle, session persistence, Codex adapter resume behavior, rollout terminal handling, cancellation semantics, and every relevant error path. Inspect each component carefully, compare the observed behavior with the V6 architecture, identify risks and race conditions, and prepare a detailed evidence-based report with concrete file and line references. Do not provide a final answer until the investigation is complete."}
EOF

  sleep 1
  curl -fsS -X POST "$cancel_url" >"$cancel_response"

  for _ in $(seq 1 30); do
    curl -fsS "$detail_url" >"$detail_response"
    if grep -Fq '"turn_active":false' "$detail_response" &&
      { grep -Fq '"status":"aborted"' "$detail_response" ||
        grep -Fq '"status":"failed"' "$detail_response"; }; then
      rm -f "$send_response" "$cancel_response" "$detail_response"
      return 0
    fi
    sleep 1
  done

  echo "cancel scenario did not reach an aborted or failed terminal turn" >&2
  cat "$detail_response" >&2
  rm -f "$send_response" "$cancel_response" "$detail_response"
  return 1
}

cd "$ROOT"
go build -o "$BIN" ./cmd/daemon
rm -rf "$HOME/.botmux-go/sessions"
mkdir -p "$HOME/.botmux-go/sessions"
"$BIN" -config ./configs/bots.json >"$LOG" 2>&1 &
echo $! >"$PID"
trap 'stop_daemon' EXIT
wait_for_daemon

"$BIN" -cmd new "$SID" bot-codex
"$BIN" -cmd send "$SID" $'Return exactly two lines:\nCODEX_V6_ONE\nCODEX_V6_TWO'

stop_daemon
"$BIN" -config ./configs/bots.json >"${LOG%.log}-resume.log" 2>&1 &
echo $! >"$PID"
wait_for_daemon
send_when_worker_ready "What were the two markers from the previous turn?"

if [[ "${RUN_CODEX_CANCEL_SCENARIO:-0}" == "1" ]]; then
  run_cancel_scenario
  send_when_worker_ready "Return exactly: CODEX_V6_AFTER_CANCEL" | grep -F "CODEX_V6_AFTER_CANCEL"
fi
