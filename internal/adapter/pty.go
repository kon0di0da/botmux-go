package adapter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/creack/pty/v2"
)

func init() {
	RegisterFactory("pty", func(kind, cliPath string) CliAdapter {
		return NewPtyAdapter(cliPath)
	})
}

type PtyAdapter struct {
	mu      sync.Mutex
	name    string
	cmdPath string
	cmd     *exec.Cmd
	ptmx    *os.File
	errCh   chan error
	closed  bool
}

func NewPtyAdapter(cmdPath string) *PtyAdapter {
	if cmdPath == "" {
		cmdPath = "/bin/bash"
	}
	return &PtyAdapter{
		name:    "pty:" + cmdPath,
		cmdPath: cmdPath,
		errCh:   make(chan error, 8),
	}
}

func (p *PtyAdapter) Name() string { return p.name }

func (p *PtyAdapter) Start(ctx context.Context, workingDir string) (*CliStartResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("pty adapter already closed")
	}

	cmd := exec.CommandContext(ctx, p.cmdPath)
	cmd.Dir = workingDir
	cmd.Env = append(os.Environ(), "TERM=xterm-256color", "LANG=en_US.UTF-8")

	ptmx, err := pty.Start(cmd)
	if err != nil {
		return nil, fmt.Errorf("pty start %s: %w", p.cmdPath, err)
	}
	p.cmd = cmd
	p.ptmx = ptmx

	go func() {
		err := cmd.Wait()
		select {
		case p.errCh <- err:
		default:
		}
	}()

	return &CliStartResult{Input: ptmx, Output: ptmx, ErrCh: p.errCh}, nil
}

func (p *PtyAdapter) Send(ctx context.Context, input string) error {
	p.mu.Lock()
	closed := p.closed
	ptmx := p.ptmx
	p.mu.Unlock()
	if closed {
		return fmt.Errorf("pty adapter is closed")
	}
	if ptmx == nil {
		return fmt.Errorf("pty adapter not started")
	}
	data := input
	if len(data) > 0 && data[len(data)-1] != '\r' {
		data = data + "\r"
	}
	_, err := ptmx.Write([]byte(data))
	return err
}

func (p *PtyAdapter) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.ptmx != nil {
		_ = p.ptmx.Close()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	close(p.errCh)
	return nil
}
