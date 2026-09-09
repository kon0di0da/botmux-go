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

	"github.com/creack/pty/v2"
)

const (
	codexScreenBufferBytes = 16 * 1024
)

func init() {
	RegisterFactory("codex", func(opts AdapterOptions) CliAdapter {
		return NewCodexAdapter(opts)
	})
}

type CodexAdapter struct {
	mu         sync.Mutex
	sendMu     sync.Mutex
	sendTurnMu sync.Mutex
	cmdPath    string
	model      string
	profile    string
	resumeID   string

	cmd        *exec.Cmd
	ptmx       *os.File
	outputR    *io.PipeReader
	outputW    *io.PipeWriter
	errCh      chan error
	ready      chan error
	events     chan AdapterEvent
	activeSend *codexActiveSend

	readyOnce sync.Once
	closed    bool
	started   bool
	screen    string
	deps      codexDependencies
	ctx       context.Context
}

type codexActiveSend struct {
	cancel context.CancelFunc
}

type codexDependencies struct {
	ownedRollouts func(pid int) (map[string]struct{}, error)
}

func NewCodexAdapter(opts AdapterOptions) *CodexAdapter {
	cmdPath := opts.CliPath
	if cmdPath == "" {
		cmdPath = "codex"
	}
	return &CodexAdapter{
		cmdPath:  cmdPath,
		model:    opts.Model,
		profile:  opts.Profile,
		resumeID: opts.ResumeSessionID,
		errCh:    make(chan error, 1),
		ready:    make(chan error, 1),
		events:   make(chan AdapterEvent, 8),
		deps: codexDependencies{
			ownedRollouts: codexRolloutsOwnedByPID,
		},
	}
}

func (a *CodexAdapter) Name() string {
	return "codex:" + a.cmdPath
}

func (a *CodexAdapter) resolveBin() (string, error) {
	if filepath.IsAbs(a.cmdPath) {
		return a.cmdPath, nil
	}
	bin, err := exec.LookPath(a.cmdPath)
	if err != nil {
		return "", fmt.Errorf("codex binary %q not found in PATH: %w", a.cmdPath, err)
	}
	return bin, nil
}

func (a *CodexAdapter) buildArgs(workingDir string) []string {
	base := []string{
		"--dangerously-bypass-approvals-and-sandbox",
		"--dangerously-bypass-hook-trust",
		"--no-alt-screen",
		"-c", "check_for_update_on_startup=false",
	}
	if a.resumeID != "" {
		if a.profile != "" {
			base = append(base, "--profile", a.profile)
		}
		return append([]string{"resume"}, append(base, a.resumeID)...)
	}
	if a.profile != "" {
		base = append(base, "--profile", a.profile)
	}
	if a.model != "" {
		base = append(base, "--model", a.model)
	}
	if workingDir != "" {
		base = append(base, "-C", workingDir)
	}
	return base
}

func (a *CodexAdapter) Start(ctx context.Context, workingDir string) (*CliStartResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, fmt.Errorf("codex adapter already closed")
	}
	if a.started {
		return nil, fmt.Errorf("codex adapter already started")
	}

	bin, err := a.resolveBin()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, bin, a.buildArgs(workingDir)...)
	cmd.Dir = workingDir
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"LANG=en_US.UTF-8",
		"LC_ALL=en_US.UTF-8",
	)
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 40, Cols: 120})
	if err != nil {
		return nil, fmt.Errorf("pty start codex: %w", err)
	}
	outputR, outputW := io.Pipe()
	a.cmd = cmd
	a.ptmx = ptmx
	a.outputR = outputR
	a.outputW = outputW
	a.ctx = ctx
	a.started = true

	go a.pumpOutput()
	go func() {
		err := cmd.Wait()
		a.publishReady(err)
		select {
		case a.errCh <- err:
		default:
		}
	}()

	return &CliStartResult{
		Input:            ptmx,
		Output:           outputR,
		ErrCh:            a.errCh,
		ReadyResult:      a.ready,
		Events:           a.events,
		StructuredOutput: true,
	}, nil
}

func (a *CodexAdapter) Send(ctx context.Context, input string) (SendResult, error) {
	a.sendTurnMu.Lock()
	defer a.sendTurnMu.Unlock()

	normalized := normalizeCodexInput(input)
	a.mu.Lock()
	closed := a.closed
	if closed {
		a.mu.Unlock()
		return SendResult{}, fmt.Errorf("codex adapter is closed")
	}
	if strings.TrimSpace(normalized) == "" {
		a.mu.Unlock()
		return SendResult{}, nil
	}
	resumeID := a.resumeID
	baseCtx := a.ctx
	started := a.started
	sendCtx, cancelSend := context.WithCancel(ctx)
	activeSend := &codexActiveSend{cancel: cancelSend}
	a.activeSend = activeSend
	a.mu.Unlock()
	defer func() {
		cancelSend()
		a.mu.Lock()
		if a.activeSend == activeSend {
			a.activeSend = nil
		}
		a.mu.Unlock()
	}()

	if baseCtx == nil {
		baseCtx = ctx
	}
	rolloutOffset := int64(0)
	if resumeID != "" {
		if rolloutPath, ok := findCodexRolloutBySessionID(resumeID); ok {
			rolloutOffset = currentFileSize(rolloutPath)
		}
	}
	historyPath := codexHistoryPath()
	baseline := currentFileSize(historyPath)
	body := "\x1b[200~" + normalized + "\x1b[201~"
	if err := a.writeCodexInput(sendCtx, []byte(body)); err != nil {
		return SendResult{}, fmt.Errorf("codex paste input: %w", err)
	}
	if err := waitContext(sendCtx, 200*time.Millisecond); err != nil {
		return SendResult{}, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		if err := sendCtx.Err(); err != nil {
			return SendResult{}, err
		}
		if err := a.writeCodexInput(sendCtx, []byte{'\r'}); err != nil {
			return SendResult{}, fmt.Errorf("codex submit input: %w", err)
		}
		sessionID, err := waitForCodexHistory(
			sendCtx,
			historyPath,
			baseline,
			normalized,
			a.ownsSession,
		)
		if err == nil {
			if started {
				go a.watchTranscript(baseCtx, sessionID, normalized, rolloutOffset)
			}
			return SendResult{CliSessionID: sessionID}, nil
		}
		if err := sendCtx.Err(); err != nil {
			return SendResult{}, err
		}
	}
	return SendResult{}, fmt.Errorf("codex submit not confirmed after 3 Enter attempts")
}

func (a *CodexAdapter) Interrupt(ctx context.Context) error {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	a.mu.Lock()
	closed := a.closed
	ptmx := a.ptmx
	a.mu.Unlock()
	if closed {
		return fmt.Errorf("codex adapter is closed")
	}
	if ptmx == nil {
		return fmt.Errorf("codex adapter not started")
	}
	if err := writeAll(ctx, ptmx, []byte{0x1b}); err != nil {
		return fmt.Errorf("codex interrupt: %w", err)
	}
	a.mu.Lock()
	activeSend := a.activeSend
	a.mu.Unlock()
	if activeSend != nil {
		activeSend.cancel()
	}
	return nil
}

func (a *CodexAdapter) Close() error {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	activeSend := a.activeSend
	ptmx := a.ptmx
	outputW := a.outputW
	outputR := a.outputR
	cmd := a.cmd
	a.mu.Unlock()

	if activeSend != nil {
		activeSend.cancel()
	}
	if ptmx != nil {
		_ = ptmx.Close()
	}
	if outputW != nil {
		_ = outputW.Close()
	}
	if outputR != nil {
		_ = outputR.Close()
	}
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	return nil
}

func (a *CodexAdapter) writeCodexInput(ctx context.Context, data []byte) error {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	a.mu.Lock()
	closed := a.closed
	ptmx := a.ptmx
	a.mu.Unlock()
	if closed {
		return fmt.Errorf("codex adapter is closed")
	}
	if ptmx == nil {
		return fmt.Errorf("codex adapter not started")
	}
	return writeAll(ctx, ptmx, data)
}

func (a *CodexAdapter) pumpOutput() {
	defer func() {
		a.mu.Lock()
		outputW := a.outputW
		a.mu.Unlock()
		if outputW != nil {
			_ = outputW.Close()
		}
	}()

	buf := make([]byte, 4096)
	for {
		n, err := a.ptmx.Read(buf)
		if n > 0 {
			chunk := string(buf[:n])
			a.observeReady(chunk)
			a.mu.Lock()
			outputW := a.outputW
			a.mu.Unlock()
			if outputW != nil {
				if _, writeErr := outputW.Write(buf[:n]); writeErr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (a *CodexAdapter) observeReady(chunk string) {
	a.mu.Lock()
	a.screen += chunk
	if len(a.screen) > codexScreenBufferBytes {
		a.screen = a.screen[len(a.screen)-codexScreenBufferBytes:]
	}
	screen := stripCodexANSI(a.screen)
	a.mu.Unlock()
	if codexComposerReady(screen) {
		a.publishReady(nil)
	}
}

func (a *CodexAdapter) publishReady(err error) {
	a.readyOnce.Do(func() {
		a.ready <- err
	})
}

func codexComposerReady(screen string) bool {
	if strings.Contains(screen, "Resuming session") ||
		strings.Contains(screen, "model: loading") ||
		strings.Contains(screen, "Welcome to Codex") ||
		strings.Contains(screen, "Sign in with") {
		return false
	}
	if strings.Contains(screen, "% left") {
		return true
	}
	for rest := screen; ; {
		i := strings.Index(rest, "›")
		if i < 0 {
			return false
		}
		after := strings.TrimLeft(rest[i+len("›"):], " \t")
		if len(after) < 2 || after[0] < '0' || after[0] > '9' ||
			!strings.Contains(after[:min(len(after), 8)], ".") {
			return true
		}
		rest = after
	}
}

func stripCodexANSI(input string) string {
	var out strings.Builder
	for i := 0; i < len(input); {
		if input[i] != 0x1b {
			out.WriteByte(input[i])
			i++
			continue
		}
		if i+1 >= len(input) {
			break
		}
		switch input[i+1] {
		case '[':
			i += 2
			for i < len(input) {
				b := input[i]
				i++
				if b >= 0x40 && b <= 0x7e {
					break
				}
			}
		case ']':
			i += 2
			for i < len(input) {
				if input[i] == 0x07 {
					i++
					break
				}
				if input[i] == 0x1b && i+1 < len(input) && input[i+1] == '\\' {
					i += 2
					break
				}
				i++
			}
		default:
			i += 2
		}
	}
	return out.String()
}

func normalizeCodexInput(input string) string {
	input = strings.ReplaceAll(input, "\r\n", "\n")
	input = strings.ReplaceAll(input, "\r", "\n")
	return strings.TrimRight(input, "\n")
}

func writeAll(ctx context.Context, writer io.Writer, data []byte) error {
	for len(data) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func (a *CodexAdapter) ownsSession(sessionID string) bool {
	a.mu.Lock()
	cmd := a.cmd
	deps := a.deps
	a.mu.Unlock()
	if cmd == nil || cmd.Process == nil || deps.ownedRollouts == nil {
		return false
	}
	owned, err := deps.ownedRollouts(cmd.Process.Pid)
	if err != nil {
		return false
	}
	_, ok := owned[strings.ToLower(sessionID)]
	return ok
}

func (a *CodexAdapter) watchTranscript(ctx context.Context, sessionID, input string, offset int64) {
	const discoveryTimeout = 10 * time.Second
	const pollInterval = 100 * time.Millisecond

	deadline := time.NewTimer(discoveryTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var cursor *codexTranscriptCursor
	for cursor == nil {
		if path, ok := findCodexRolloutBySessionID(sessionID); ok {
			cursor = &codexTranscriptCursor{Path: path, Offset: offset, Input: input}
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			a.publishAdapterEvent(ctx, AdapterEvent{
				Kind:        AdapterTurnTerminal,
				Status:      TurnFailed,
				ErrorCode:   "codex_rollout_missing",
				ErrorDetail: "Codex rollout was not created after submission",
			})
			return
		case <-ticker.C:
		}
	}

	for {
		events, err := cursor.ReadNew()
		if err != nil {
			a.publishAdapterEvent(ctx, AdapterEvent{
				Kind:        AdapterTurnTerminal,
				Status:      TurnFailed,
				ErrorCode:   "codex_rollout_read_error",
				ErrorDetail: safeCodexErrorDetail(err.Error()),
			})
			return
		}
		for _, event := range events {
			a.publishAdapterEvent(ctx, event)
			if event.Kind == AdapterTurnTerminal {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *CodexAdapter) publishAdapterEvent(ctx context.Context, event AdapterEvent) {
	select {
	case a.events <- event:
	case <-ctx.Done():
	}
}
