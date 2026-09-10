package worker

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"botmux-go/internal/daemon"
	"botmux-go/internal/protocol"
)

func TestRunCleansUpAfterDaemonCloseDoesNotPersistSessionClosed(t *testing.T) {
	const sessionID = "worker-close-cleanup"
	storeDir := t.TempDir()
	sessionPath := filepath.Join(storeDir, sessionID+".json")
	// The daemon persists closure before sending MsgClose. A worker-only
	// close must never modify the session file.
	fixture, err := json.Marshal(&daemon.PersistedSession{SessionID: sessionID, BotID: "bot-test"})
	if err != nil {
		t.Fatalf("marshal session fixture: %v", err)
	}
	if err := os.WriteFile(sessionPath, fixture, 0o644); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	w := New(Options{
		SessionID:  sessionID,
		DaemonAddr: listener.Addr().String(),
		CliType:    "mock",
		StoreDir:   storeDir,
	})
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept worker: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		w.Cancel()
	})

	_ = conn.SetDeadline(time.Now().Add(time.Second))
	ready, err := protocol.NewMessageReader(conn).Read()
	if err != nil {
		t.Fatalf("read worker ready: %v", err)
	}
	if ready.Type != protocol.MsgReady {
		t.Fatalf("worker first message = %s, want %s", ready.Type, protocol.MsgReady)
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, sessionID, "worker_ready").WriteTo(conn); err != nil {
		t.Fatalf("acknowledge worker ready: %v", err)
	}
	if _, err := protocol.NewMessage(protocol.MsgClose, sessionID, "test").WriteTo(conn); err != nil {
		t.Fatalf("send close: %v", err)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgClose")
	}

	data, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatalf("load closed session: %v", err)
	}
	var persisted daemon.PersistedSession
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("decode closed session: %v", err)
	}
	if persisted.Closed {
		t.Fatal("worker cleanup marked the session closed")
	}
}
