package adapter

import (
	"bufio"
	"context"
	"strings"
	"testing"
	"time"
)

func TestMockAdapterEchoesInputAndRecordsHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := NewMockAdapter("mock-test")
	mock.echoDelay = 0
	started, err := mock.Start(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mock.Close()

	if _, err := mock.Send(ctx, "hello"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	lineCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(started.Output).ReadString('\n')
		if err != nil {
			errCh <- err
			return
		}
		lineCh <- line
	}()

	select {
	case line := <-lineCh:
		if line != "[mock-echo] hello\n" {
			t.Fatalf("echo output = %q, want %q", line, "[mock-echo] hello\n")
		}
	case err := <-errCh:
		t.Fatalf("read output: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mock echo")
	}

	history := mock.History()
	if len(history) != 1 || history[0] != "hello" {
		t.Fatalf("history = %#v, want %#v", history, []string{"hello"})
	}
}

func TestMockAdapterSendValidatesLifecycle(t *testing.T) {
	ctx := context.Background()
	mock := NewMockAdapter("")

	if _, err := mock.Send(ctx, "before start"); err == nil || !strings.Contains(err.Error(), "not started") {
		t.Fatalf("Send before Start error = %v, want not started", err)
	}

	started, err := mock.Start(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = started

	if err := mock.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mock.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := mock.Send(ctx, "after close"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Send after Close error = %v, want closed", err)
	}
	if _, err := mock.Start(ctx, t.TempDir()); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("Start after Close error = %v, want already closed", err)
	}
}
