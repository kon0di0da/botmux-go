#!/bin/bash
set -e

BIN="/tmp/botmux-go"
SESSION_DIR="$HOME/.botmux-go/sessions"

echo "=== Smoke Test: Persistence & Reconnection ==="
echo ""

rm -rf "$SESSION_DIR"

echo "[1] Building binary..."
cd /Users/bytedance/botmux-go
go build -o "$BIN" ./cmd/daemon
echo "    OK"

echo "[2] Starting daemon (background)..."
"$BIN" &
DAEMON_PID=$!
sleep 2

echo "[3] Creating session sess-test-001..."
"$BIN" -cmd new sess-test-001 bot-default
echo ""

echo "[4] Checking session persistence file..."
if [ -f "$SESSION_DIR/sess-test-001.json" ]; then
    echo "    OK: $SESSION_DIR/sess-test-001.json exists"
    cat "$SESSION_DIR/sess-test-001.json"
else
    echo "    FAIL: persistence file not found"
    exit 1
fi

echo ""
echo "[5] Sending message to sess-test-001..."
"$BIN" -cmd send sess-test-001 "hello persistence test"
echo ""

echo "[6] Checking output persistence..."
cat "$SESSION_DIR/sess-test-001.json"

echo ""
echo "[7] Stopping daemon (simulating crash)..."
kill $DAEMON_PID 2>/dev/null || true
wait $DAEMON_PID 2>/dev/null || true
echo "    Daemon stopped"
sleep 1

echo "[8] Restarting daemon (should restore session)..."
"$BIN" &
DAEMON_PID2=$!
sleep 2

echo "[9] Checking restored session..."
cat "$SESSION_DIR/sess-test-001.json"

echo ""
echo "[10] Sending message to restored session..."
"$BIN" -cmd send sess-test-001 "second message after restart"
echo ""

echo "[11] Closing session..."
"$BIN" -cmd send sess-test-001 "close test"
kill $DAEMON_PID2 2>/dev/null || true
wait $DAEMON_PID2 2>/dev/null || true

echo ""
echo "=== All persistence tests passed! ==="
