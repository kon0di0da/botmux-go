package adapter

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

func init() {
	RegisterFactory("tmux", func(opts AdapterOptions) CliAdapter {
		return &TmuxAdapter{
			sessionName: "botmux-go",
			cliCmd:      opts.CliPath,
		}
	})
}

type TmuxAdapter struct {
	sessionName string
	cliCmd      string
	workingDir  string

	input  io.WriteCloser
	output io.ReadCloser
	errCh  chan error

	cancel context.CancelFunc

	reader *bufio.Reader
}

func (t *TmuxAdapter) Name() string { return "tmux" }

func (t *TmuxAdapter) Start(ctx context.Context, workingDir string) (*CliStartResult, error) {
	t.workingDir = workingDir

	if err := t.ensureTmux(); err != nil {
		return nil, err
	}

	t.errCh = make(chan error, 16)
	ctx, t.cancel = context.WithCancel(ctx)

	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("os.Pipe: %w", err)
	}
	t.input = pw
	t.output = pr
	t.reader = bufio.NewReader(pr)

	go t.readTmuxOutput(ctx)

	return &CliStartResult{
		Input:  t.input,
		Output: t.output,
		ErrCh:  t.errCh,
	}, nil
}

func (t *TmuxAdapter) Send(ctx context.Context, input string) error {
	if err := t.ensureTmux(); err != nil {
		return err
	}
	escaped := escapeTmuxKeys(input)
	cmd := exec.Command("tmux", "send-keys", "-t", t.sessionName, escaped, "Enter")
	cmd.Dir = t.workingDir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("tmux send-keys: %s: %w", string(out), err)
	}
	return nil
}

func (t *TmuxAdapter) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	cmd := exec.Command("tmux", "kill-session", "-t", t.sessionName)
	_ = cmd.Run()
	if t.input != nil {
		_ = t.input.Close()
	}
	if t.output != nil {
		_ = t.output.Close()
	}
	return nil
}

func (t *TmuxAdapter) ensureTmux() error {
	cmd := exec.Command("tmux", "has-session", "-t", t.sessionName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("tmux session %s not found: %w", t.sessionName, err)
	}
	return nil
}

func (t *TmuxAdapter) readTmuxOutput(ctx context.Context) {
	defer t.output.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		cmd := exec.Command("tmux", "capture-pane", "-t", t.sessionName, "-p")
		out, err := cmd.Output()
		_ = out
		if err != nil {
			t.errCh <- err
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func escapeTmuxKeys(s string) string {
	result := ""
	for _, r := range s {
		result += string(r)
	}
	return result
}
