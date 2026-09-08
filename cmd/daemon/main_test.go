package main

import (
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"botmux-go/internal/protocol"
)

func TestExecCommandSendStreamsAllOutputUntilConnectionCloses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		probe, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		_ = probe.Close()

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
		serverErr <- nil
	}()

	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer stdoutReader.Close()
	originalStdout := os.Stdout
	os.Stdout = stdoutWriter
	defer func() { os.Stdout = originalStdout }()

	execCommand(listener.Addr().String(), "send", []string{"session-1", "hello"})

	if err := stdoutWriter.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	output, err := io.ReadAll(stdoutReader)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake daemon: %v", err)
	}

	got := string(output)
	if !strings.Contains(got, "first chunk") {
		t.Fatalf("missing first output chunk:\n%s", got)
	}
	if !strings.Contains(got, "second chunk") {
		t.Fatalf("missing second output chunk:\n%s", got)
	}
}

type unexpectedMessageError struct {
	msg *protocol.Message
}

func (e *unexpectedMessageError) Error() string {
	return "unexpected message: type=" + string(e.msg.Type) + " session=" + e.msg.SessionID
}
