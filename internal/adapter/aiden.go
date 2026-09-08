package adapter

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/creack/pty/v2"
)

func init() {
	RegisterFactory("aiden", func(opts AdapterOptions) CliAdapter {
		return NewAidenAdapter(opts)
	})
}

type AidenAdapter struct {
	mu      sync.Mutex
	sendMu  sync.Mutex
	cmdPath string
	model   string
	cmd     *exec.Cmd
	ptmx    *os.File
	writer  io.Writer
	errCh   chan error
	closed  bool
	started bool

	chunkBytes int
	chunkDelay time.Duration
	sendDelay  time.Duration

	activityMu     sync.Mutex
	activitySeq    uint64
	activityNotify chan struct{}

	writeRetryAttempts int
	writeRetryDelay    time.Duration
	maxEnterAttempts   int
	enterRetryDelay    time.Duration
	confirmTimeout     time.Duration
}

const (
	aidenInputChunkBytes    = 4 * 1024
	aidenChunkDelay         = 5 * time.Millisecond
	aidenSendDelay          = 200 * time.Millisecond
	aidenWriteRetryAttempts = 3
	aidenWriteRetryDelay    = 20 * time.Millisecond
	aidenMaxEnterAttempts   = 3
	aidenEnterRetryDelay    = 100 * time.Millisecond
	aidenConfirmTimeout     = 2 * time.Second
)

func NewAidenAdapter(opts AdapterOptions) *AidenAdapter {
	cmdPath := opts.CliPath
	if cmdPath == "" {
		cmdPath = "aiden"
	}
	return &AidenAdapter{
		cmdPath:            cmdPath,
		model:              opts.Model,
		errCh:              make(chan error, 8),
		chunkBytes:         aidenInputChunkBytes,
		chunkDelay:         aidenChunkDelay,
		sendDelay:          aidenSendDelay,
		activityNotify:     make(chan struct{}),
		writeRetryAttempts: aidenWriteRetryAttempts,
		writeRetryDelay:    aidenWriteRetryDelay,
		maxEnterAttempts:   aidenMaxEnterAttempts,
		enterRetryDelay:    aidenEnterRetryDelay,
		confirmTimeout:     aidenConfirmTimeout,
	}
}

func (a *AidenAdapter) Name() string { return "aiden:" + a.cmdPath }

func (a *AidenAdapter) resolveBin() (string, error) {
	if filepath.IsAbs(a.cmdPath) {
		return a.cmdPath, nil
	}
	p, err := exec.LookPath(a.cmdPath)
	if err != nil {
		return "", fmt.Errorf("aiden binary %q not found in PATH: %w", a.cmdPath, err)
	}
	return p, nil
}

func (a *AidenAdapter) buildArgs(workingDir string) []string {
	args := []string{
		"x",
		"codex",
		"--dangerously-bypass-approvals-and-sandbox",
		"--no-alt-screen",
	}
	if workingDir != "" {
		args = append(args, "-C", workingDir)
	}
	if a.model != "" {
		args = append(args, "--model", a.model)
	}
	return args
}

const aidenReadyDelay = 6 * time.Second

func (a *AidenAdapter) Start(ctx context.Context, workingDir string) (*CliStartResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, fmt.Errorf("aiden adapter already closed")
	}
	if a.started {
		return nil, fmt.Errorf("aiden adapter already started")
	}

	bin, err := a.resolveBin()
	if err != nil {
		return nil, err
	}
	args := a.buildArgs(workingDir)

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = workingDir
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"LANG=en_US.UTF-8",
		"LC_ALL=en_US.UTF-8",
	)

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 40, Cols: 120})
	if err != nil {
		return nil, fmt.Errorf("pty start aiden: %w", err)
	}

	a.cmd = cmd
	a.ptmx = ptmx
	a.writer = ptmx
	a.started = true

	go func() {
		err := cmd.Wait()
		select {
		case a.errCh <- err:
		default:
		}
	}()

	return &CliStartResult{
		Input:      ptmx,
		Output:     ptmx,
		ErrCh:      a.errCh,
		ReadyDelay: aidenReadyDelay,
	}, nil
}

func (a *AidenAdapter) Send(ctx context.Context, input string) error {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	a.mu.Lock()
	closed := a.closed
	writer := a.writer
	chunkBytes := a.chunkBytes
	chunkDelay := a.chunkDelay
	sendDelay := a.sendDelay
	writeRetryAttempts := a.writeRetryAttempts
	writeRetryDelay := a.writeRetryDelay
	maxEnterAttempts := a.maxEnterAttempts
	enterRetryDelay := a.enterRetryDelay
	confirmTimeout := a.confirmTimeout
	a.mu.Unlock()
	if closed {
		return fmt.Errorf("aiden adapter is closed")
	}
	if writer == nil {
		return fmt.Errorf("aiden adapter not started")
	}

	normalized := normalizeAidenInput(input)
	if strings.TrimSpace(normalized) == "" {
		return nil
	}

	chunks := splitUTF8Chunks(normalized, chunkBytes)
	for i, chunk := range chunks {
		if err := writeAllWithRetry(ctx, writer, []byte(chunk), writeRetryAttempts, writeRetryDelay); err != nil {
			return fmt.Errorf("aiden write text chunk %d/%d: %w", i+1, len(chunks), err)
		}
		if i < len(chunks)-1 {
			if err := waitContext(ctx, chunkDelay); err != nil {
				return err
			}
		}
	}

	if err := waitContext(ctx, sendDelay); err != nil {
		return err
	}

	if maxEnterAttempts <= 0 {
		maxEnterAttempts = 1
	}
	for attempt := 1; attempt <= maxEnterAttempts; attempt++ {
		activitySeq, activityCh := a.outputSubscription()
		if err := writeAllWithRetry(ctx, writer, []byte{'\r'}, writeRetryAttempts, writeRetryDelay); err != nil {
			return fmt.Errorf("aiden write enter attempt %d/%d: %w", attempt, maxEnterAttempts, err)
		}
		confirmed, err := a.waitForOutput(ctx, activitySeq, activityCh, confirmTimeout)
		if err != nil {
			return err
		}
		if confirmed {
			return nil
		}
		if attempt < maxEnterAttempts {
			if err := waitContext(ctx, enterRetryDelay); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("aiden submit not confirmed after %d Enter attempts", maxEnterAttempts)
}

func (a *AidenAdapter) Close() error {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	a.writer = nil
	if a.ptmx != nil {
		_ = a.ptmx.Close()
	}
	if a.cmd != nil && a.cmd.Process != nil {
		_ = a.cmd.Process.Kill()
	}
	return nil
}

func normalizeAidenInput(input string) string {
	input = strings.ReplaceAll(input, "\r\n", "\n")
	input = strings.ReplaceAll(input, "\r", "\n")
	return strings.TrimRight(input, "\n")
}

func splitUTF8Chunks(input string, maxBytes int) []string {
	if input == "" {
		return nil
	}
	if maxBytes <= 0 {
		maxBytes = aidenInputChunkBytes
	}

	chunks := make([]string, 0, (len(input)+maxBytes-1)/maxBytes)
	for start := 0; start < len(input); {
		end := min(start+maxBytes, len(input))
		for end < len(input) && end > start && !utf8.RuneStart(input[end]) {
			end--
		}
		if end == start {
			_, size := utf8.DecodeRuneInString(input[start:])
			end = start + size
		}
		chunks = append(chunks, input[start:end])
		start = end
	}
	return chunks
}

func writeAllWithRetry(
	ctx context.Context,
	writer io.Writer,
	data []byte,
	maxAttempts int,
	retryDelay time.Duration,
) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	failures := 0
	for len(data) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return fmt.Errorf("invalid write count %d for %d bytes", n, len(data))
		}
		if n > 0 {
			data = data[n:]
		}
		if err == nil && n > 0 {
			failures = 0
			continue
		}
		if err == nil {
			err = io.ErrNoProgress
		}
		if len(data) == 0 {
			return err
		}
		failures++
		if failures >= maxAttempts {
			return err
		}
		if waitErr := waitContext(ctx, retryDelay); waitErr != nil {
			return waitErr
		}
	}
	return nil
}

func (a *AidenAdapter) NotifyOutput() {
	a.activityMu.Lock()
	notify := a.activityNotify
	a.activitySeq++
	a.activityNotify = make(chan struct{})
	if notify != nil {
		close(notify)
	}
	a.activityMu.Unlock()
}

func (a *AidenAdapter) outputSubscription() (uint64, <-chan struct{}) {
	a.activityMu.Lock()
	defer a.activityMu.Unlock()
	if a.activityNotify == nil {
		a.activityNotify = make(chan struct{})
	}
	return a.activitySeq, a.activityNotify
}

func (a *AidenAdapter) waitForOutput(
	ctx context.Context,
	baseline uint64,
	notify <-chan struct{},
	timeout time.Duration,
) (bool, error) {
	if timeout <= 0 {
		return true, nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-notify:
		a.activityMu.Lock()
		confirmed := a.activitySeq > baseline
		a.activityMu.Unlock()
		return confirmed, nil
	case <-timer.C:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
