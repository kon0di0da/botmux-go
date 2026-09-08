package adapter

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"
)

func TestAidenBuildArgsUsesConfiguredCodexLauncher(t *testing.T) {
	a := NewAidenAdapter(AdapterOptions{CliType: "aiden", Model: "gpt-5.5"})
	got := a.buildArgs("/tmp/workspace")
	want := []string{
		"x",
		"codex",
		"--dangerously-bypass-approvals-and-sandbox",
		"--no-alt-screen",
		"-C",
		"/tmp/workspace",
		"--model",
		"gpt-5.5",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}

func TestAidenSendPreservesMultilineInput(t *testing.T) {
	var dst bytes.Buffer
	a := newTestAidenAdapter(&dst, 1024)

	if _, err := a.Send(context.Background(), "first\r\n  second\rthird\n"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	want := "first\n  second\nthird\r"
	if got := dst.String(); got != want {
		t.Fatalf("written input = %q, want %q", got, want)
	}
	if strings.Contains(dst.String(), "\x1b[200~") || strings.Contains(dst.String(), "\x1b[201~") {
		t.Fatal("Aiden input contains unsupported bracketed-paste markers")
	}
}

func TestAidenSendChunksWithoutSplittingUTF8(t *testing.T) {
	dst := &recordingWriter{}
	a := newTestAidenAdapter(dst, 5)
	input := "ab你cd好ef"

	if _, err := a.Send(context.Background(), input); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var text bytes.Buffer
	for i, call := range dst.Calls() {
		if i == len(dst.Calls())-1 {
			if string(call) != "\r" {
				t.Fatalf("final write = %q, want Enter", call)
			}
			continue
		}
		if len(call) > 5 {
			t.Fatalf("chunk %d has %d bytes, want <= 5", i, len(call))
		}
		if !utf8.Valid(call) {
			t.Fatalf("chunk %d splits UTF-8: %x", i, call)
		}
		text.Write(call)
	}
	if got := text.String(); got != input {
		t.Fatalf("reassembled input = %q, want %q", got, input)
	}
}

func TestAidenSendCompletesShortWrites(t *testing.T) {
	dst := &recordingWriter{maxWrite: 2}
	a := newTestAidenAdapter(dst, 1024)

	if _, err := a.Send(context.Background(), "abcdef"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := string(dst.Bytes()); got != "abcdef\r" {
		t.Fatalf("written input = %q, want %q", got, "abcdef\r")
	}
}

func TestAidenSendSerializesConcurrentInputs(t *testing.T) {
	dst := newBlockingWriter()
	a := newTestAidenAdapter(dst, 1024)

	firstDone := make(chan error, 1)
	go func() {
		_, err := a.Send(context.Background(), "first")
		firstDone <- err
	}()
	<-dst.firstWrite

	secondDone := make(chan error, 1)
	go func() {
		_, err := a.Send(context.Background(), "second")
		secondDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if calls := dst.CallCount(); calls != 1 {
		t.Fatalf("concurrent Send reached writer before first completed: calls=%d", calls)
	}
	close(dst.release)

	if err := <-firstDone; err != nil {
		t.Fatalf("first Send: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Send: %v", err)
	}
	if got := string(dst.Bytes()); got != "first\rsecond\r" {
		t.Fatalf("serialized input = %q, want %q", got, "first\rsecond\r")
	}
}

func TestAidenSendRetriesRemainingBytesWithoutDuplicatingPrefix(t *testing.T) {
	dst := &partialErrorWriter{}
	a := newTestAidenAdapter(dst, 1024)
	a.writeRetryAttempts = 3

	if _, err := a.Send(context.Background(), "abcdef"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := string(dst.Bytes()); got != "abcdef\r" {
		t.Fatalf("written input = %q, want %q", got, "abcdef\r")
	}
	if dst.Failures() != 1 {
		t.Fatalf("injected failures = %d, want 1", dst.Failures())
	}
}

func TestAidenSendRetriesEnterUntilPTYActivity(t *testing.T) {
	dst := &enterAwareWriter{}
	a := newTestAidenAdapter(dst, 1024)
	a.confirmTimeout = 5 * time.Millisecond
	a.enterRetryDelay = 0
	a.maxEnterAttempts = 3
	dst.onEnter = func(attempt int) {
		if attempt == 2 {
			a.NotifyOutput()
		}
	}

	if _, err := a.Send(context.Background(), "prompt"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if dst.EnterCount() != 2 {
		t.Fatalf("Enter count = %d, want 2", dst.EnterCount())
	}
	if got := string(dst.Bytes()); strings.Count(got, "prompt") != 1 {
		t.Fatalf("prompt written more than once: %q", got)
	}
}

func TestAidenSendFailsWhenEnterCannotBeConfirmed(t *testing.T) {
	dst := &enterAwareWriter{}
	a := newTestAidenAdapter(dst, 1024)
	a.confirmTimeout = 2 * time.Millisecond
	a.enterRetryDelay = 0
	a.maxEnterAttempts = 3

	_, err := a.Send(context.Background(), "prompt")
	if err == nil || !strings.Contains(err.Error(), "not confirmed") {
		t.Fatalf("Send error = %v, want submit confirmation error", err)
	}
	if dst.EnterCount() != 3 {
		t.Fatalf("Enter count = %d, want 3", dst.EnterCount())
	}
	if got := string(dst.Bytes()); strings.Count(got, "prompt") != 1 {
		t.Fatalf("prompt written more than once: %q", got)
	}
}

func newTestAidenAdapter(writer io.Writer, chunkBytes int) *AidenAdapter {
	a := NewAidenAdapter(AdapterOptions{CliType: "aiden"})
	a.writer = writer
	a.sendDelay = 0
	a.chunkDelay = 0
	a.chunkBytes = chunkBytes
	a.confirmTimeout = 0
	a.writeRetryDelay = 0
	a.enterRetryDelay = 0
	return a
}

type recordingWriter struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	calls    [][]byte
	maxWrite int
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	if w.maxWrite > 0 && n > w.maxWrite {
		n = w.maxWrite
	}
	cp := append([]byte(nil), p[:n]...)
	w.calls = append(w.calls, cp)
	_, _ = w.buf.Write(cp)
	return n, nil
}

func (w *recordingWriter) Calls() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([][]byte, len(w.calls))
	for i := range w.calls {
		out[i] = append([]byte(nil), w.calls[i]...)
	}
	return out
}

func (w *recordingWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buf.Bytes()...)
}

type blockingWriter struct {
	recordingWriter
	firstWrite chan struct{}
	release    chan struct{}
	once       sync.Once
	entered    int
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		firstWrite: make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.entered++
	w.mu.Unlock()
	w.once.Do(func() {
		close(w.firstWrite)
		<-w.release
	})
	return w.recordingWriter.Write(p)
}

func (w *blockingWriter) CallCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.entered
}

type partialErrorWriter struct {
	recordingWriter
	failed bool
}

func (w *partialErrorWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	shouldFail := !w.failed && string(p) != "\r"
	if shouldFail {
		w.failed = true
	}
	w.mu.Unlock()
	if shouldFail {
		n := min(2, len(p))
		_, _ = w.recordingWriter.Write(p[:n])
		return n, syscall.EAGAIN
	}
	return w.recordingWriter.Write(p)
}

func (w *partialErrorWriter) Failures() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed {
		return 1
	}
	return 0
}

type enterAwareWriter struct {
	recordingWriter
	onEnter    func(attempt int)
	enterCount int
}

func (w *enterAwareWriter) Write(p []byte) (int, error) {
	attempt := 0
	if string(p) == "\r" {
		w.mu.Lock()
		w.enterCount++
		attempt = w.enterCount
		w.mu.Unlock()
	}
	n, err := w.recordingWriter.Write(p)
	if attempt > 0 && w.onEnter != nil {
		w.onEnter(attempt)
	}
	return n, err
}

func (w *enterAwareWriter) EnterCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enterCount
}
