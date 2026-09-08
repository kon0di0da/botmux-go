set -euo pipefail

cd /Users/bytedance/botmux-go

BIN=/tmp/botmux-go-v5prb
CONFIG=./configs/bots.json
DAEMON_LOG=/tmp/botmux-go-v5-aiden-daemon.log
TEST_LOG=/tmp/botmux-go-v5-aiden-e2e.log
STATUS_FILE=/tmp/botmux-go-v5-aiden-e2e.status
PID_FILE=/tmp/botmux-go-v5prb.pid
SID="v5-aiden-$(date +%s)"
STATUS=FAIL

finish() {
  if [[ "$STATUS" != "PASS" && -x "$BIN" ]]; then
    "$BIN" -cmd close "$SID" "V5 Aiden acceptance failed" >/dev/null 2>&1 || true
  fi
  printf '%s\n' "$STATUS" >"$STATUS_FILE"
}
trap finish EXIT

: >"$TEST_LOG"
: >"$DAEMON_LOG"

if [[ -f "$PID_FILE" ]]; then
  kill "$(cat "$PID_FILE")" 2>/dev/null || true
  rm -f "$PID_FILE"
  sleep 1
fi

rm -rf "$HOME/.botmux-go/sessions"
mkdir -p "$HOME/.botmux-go/sessions"
go build -o "$BIN" ./cmd/daemon

nohup "$BIN" -config "$CONFIG" -listen 127.0.0.1:17890 -dashboard 127.0.0.1:17891 >"$DAEMON_LOG" 2>&1 &
echo $! >"$PID_FILE"
sleep 2
kill -0 "$(cat "$PID_FILE")"

"$BIN" -cmd new "$SID" bot-aiden 2>&1 | tee -a "$TEST_LOG"
grep -q ' READY' "$TEST_LOG"

wait_for_marker() {
  local marker=$1
  local output=$2
  local pid=$3
  for _ in {1..450}; do
    if grep -q "^  << ${marker}$" "$output" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      return 1
    fi
    sleep 0.2
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 1
}

ROUND1=/tmp/botmux-go-v5-aiden-round1.log
: >"$ROUND1"
"$BIN" -cmd send "$SID" "请只输出三行：第一行 AIDEN_V5_A，第二行 AIDEN_V5_B，第三行 AIDEN_V5_C。" >"$ROUND1" 2>&1 &
ROUND1_PID=$!
wait_for_marker AIDEN_V5_C "$ROUND1" "$ROUND1_PID"
cat "$ROUND1" | tee -a "$TEST_LOG"
grep -q '^  << AIDEN_V5_A$' "$ROUND1"
grep -q '^  << AIDEN_V5_B$' "$ROUND1"
grep -q '^  << AIDEN_V5_C$' "$ROUND1"

ROUND2=/tmp/botmux-go-v5-aiden-round2.log
: >"$ROUND2"
"$BIN" -cmd send "$SID" "第二轮，请只输出 AIDEN_V5_ROUND2_OK。" >"$ROUND2" 2>&1 &
ROUND2_PID=$!
wait_for_marker AIDEN_V5_ROUND2_OK "$ROUND2" "$ROUND2_PID"
cat "$ROUND2" | tee -a "$TEST_LOG"
grep -q '^  << AIDEN_V5_ROUND2_OK$' "$ROUND2"
if grep -q '^  << AIDEN_V5_A$' "$ROUND2"; then
  exit 1
fi

"$BIN" -cmd close "$SID" "V5 Aiden acceptance complete" 2>&1 | tee -a "$TEST_LOG"
STATUS=PASS
printf 'V5_AIDEN_E2E_PASS session=%s\n' "$SID" | tee -a "$TEST_LOG"
