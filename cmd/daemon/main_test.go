package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"

	"botmux-go/internal/protocol"
)

func TestExecSendCommandReturnsOnCompletedTurn(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		msg, err := protocol.NewMessageReader(conn).Read()
		if err != nil {
			serverErr <- err
			return
		}
		if msg.Type != protocol.MsgUserInput || msg.SessionID != "session-1" {
			serverErr <- &unexpectedMessageError{msg: msg}
			return
		}
		for _, payload := range []string{"first chunk", "second chunk"} {
			if _, err := protocol.NewMessage(protocol.MsgOutput, "session-1", payload).WriteTo(conn); err != nil {
				serverErr <- err
				return
			}
		}
		terminal, _ := json.Marshal(protocol.TurnTerminal{Status: protocol.TurnCompleted})
		if _, err := protocol.NewMessage(protocol.MsgTurnCompleted, "session-1", string(terminal)).WriteTo(conn); err != nil {
			serverErr <- err
			return
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		serverErr <- nil
	}()

	var output bytes.Buffer
	if err := execSendCommand(listener.Addr().String(), "session-1", "hello", &output); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake daemon: %v", err)
	}

	got := output.String()
	if !strings.Contains(got, "first chunk") {
		t.Fatalf("missing first output chunk:\n%s", got)
	}
	if !strings.Contains(got, "second chunk") {
		t.Fatalf("missing second output chunk:\n%s", got)
	}
}

func TestExecSendCommandReturnsTerminalFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn == nil {
			return
		}
		defer conn.Close()
		_, _ = protocol.NewMessageReader(conn).Read()
		payload, _ := json.Marshal(protocol.TurnTerminal{
			Status:      protocol.TurnFailed,
			ErrorCode:   "codex_upstream_error",
			ErrorDetail: "retry later",
		})
		_, _ = protocol.NewMessage(protocol.MsgTurnCompleted, "session-1", string(payload)).WriteTo(conn)
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()

	err = execSendCommand(listener.Addr().String(), "session-1", "hello", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "codex_upstream_error: retry later") {
		t.Fatalf("error = %v, want failed terminal", err)
	}
}

func TestWorkerOptionsFromEnvIncludesInstanceID(t *testing.T) {
	env := map[string]string{
		EnvSessionID:        "session-123",
		EnvDaemonAddr:       "127.0.0.1:19000",
		EnvCliType:          "codex",
		EnvCliPath:          "/usr/local/bin/codex",
		EnvModel:            "gpt-5",
		EnvCodexProfile:     "development",
		EnvResumeSessionID:  "cli-session-456",
		EnvWorkingDir:       "/tmp/botmux-working",
		EnvStoreDir:         "/tmp/botmux-store",
		EnvWorkerInstanceID: "nonce-123",
	}
	getenv := func(key string) string {
		return env[key]
	}

	opts, err := workerOptionsFromEnv(getenv)
	if err != nil {
		t.Fatalf("workerOptionsFromEnv: %v", err)
	}

	if opts.SessionID != "session-123" {
		t.Errorf("session ID = %q, want %q", opts.SessionID, "session-123")
	}
	if opts.DaemonAddr != "127.0.0.1:19000" {
		t.Errorf("daemon address = %q, want %q", opts.DaemonAddr, "127.0.0.1:19000")
	}
	if opts.CliType != "codex" {
		t.Errorf("CLI type = %q, want %q", opts.CliType, "codex")
	}
	if opts.CliPath != "/usr/local/bin/codex" {
		t.Errorf("CLI path = %q, want %q", opts.CliPath, "/usr/local/bin/codex")
	}
	if opts.Model != "gpt-5" {
		t.Errorf("model = %q, want %q", opts.Model, "gpt-5")
	}
	if opts.CodexProfile != "development" {
		t.Errorf("Codex profile = %q, want %q", opts.CodexProfile, "development")
	}
	if opts.ResumeSessionID != "cli-session-456" {
		t.Errorf("resume session ID = %q, want %q", opts.ResumeSessionID, "cli-session-456")
	}
	if opts.WorkingDir != "/tmp/botmux-working" {
		t.Errorf("working directory = %q, want %q", opts.WorkingDir, "/tmp/botmux-working")
	}
	if opts.StoreDir != "/tmp/botmux-store" {
		t.Errorf("store directory = %q, want %q", opts.StoreDir, "/tmp/botmux-store")
	}
	if opts.WorkerInstanceID != "nonce-123" {
		t.Errorf("worker instance ID = %q, want %q", opts.WorkerInstanceID, "nonce-123")
	}
}

func TestWorkerOptionsFromEnvRequiresSessionID(t *testing.T) {
	_, err := workerOptionsFromEnv(func(string) string { return "" })
	if err == nil {
		t.Fatal("workerOptionsFromEnv returned nil error without a session ID")
	}
}

type unexpectedMessageError struct {
	msg *protocol.Message
}

func (e *unexpectedMessageError) Error() string {
	return "unexpected message: type=" + string(e.msg.Type) + " session=" + e.msg.SessionID
}
