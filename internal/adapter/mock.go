package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type MockAdapter struct {
	mu          sync.Mutex
	name        string
	inputR      *io.PipeReader
	inputW      *io.PipeWriter
	outputR     *io.PipeReader
	outputW     *io.PipeWriter
	errCh       chan error
	closed      bool
	echoDelay   time.Duration
	history     []string
}

func init() {
	RegisterFactory("mock", func(kind, cliPath string) CliAdapter {
		return NewMockAdapter(kind)
	})
}

func NewMockAdapter(name string) *MockAdapter {
	if name == "" {
		name = "mock"
	}
	return &MockAdapter{
		name:      name,
		echoDelay: 100 * time.Millisecond,
		errCh:     make(chan error, 8),
	}
}

func (m *MockAdapter) Name() string {
	return m.name
}

func (m *MockAdapter) Start(ctx context.Context, workingDir string) (*CliStartResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("mock adapter already closed")
	}
	m.inputR, m.inputW = io.Pipe()
	m.outputR, m.outputW = io.Pipe()

	go m.echoLoop(ctx)

	return &CliStartResult{
		Input:  m.inputW,
		Output: m.outputR,
		ErrCh:  m.errCh,
	}, nil
}

func (m *MockAdapter) echoLoop(ctx context.Context) {
	buf := make([]byte, 4096)
	var lineBuf bytes.Buffer
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := m.inputR.Read(buf)
		if err != nil {
			if err != io.EOF {
				select {
				case m.errCh <- fmt.Errorf("mock read error: %w", err):
				default:
				}
			}
			return
		}
		for i := 0; i < n; i++ {
			b := buf[i]
			if b == '\n' {
				line := lineBuf.String()
				lineBuf.Reset()
				m.mu.Lock()
				m.history = append(m.history, line)
				m.mu.Unlock()
				time.Sleep(m.echoDelay)
				resp := fmt.Sprintf("[mock-echo] %s\n", line)
				if _, werr := m.outputW.Write([]byte(resp)); werr != nil {
					return
				}
			} else {
				lineBuf.WriteByte(b)
			}
		}
	}
}

func (m *MockAdapter) Send(ctx context.Context, input string) error {
	m.mu.Lock()
	closed := m.closed
	writer := m.inputW
	m.mu.Unlock()
	if closed {
		return fmt.Errorf("mock adapter is closed")
	}
	if writer == nil {
		return fmt.Errorf("mock adapter not started")
	}
	data := input
	if len(data) == 0 || data[len(data)-1] != '\n' {
		data = data + "\n"
	}
	_, err := writer.Write([]byte(data))
	return err
}

func (m *MockAdapter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.inputW != nil {
		m.inputW.Close()
	}
	if m.outputW != nil {
		m.outputW.Close()
	}
	if m.inputR != nil {
		m.inputR.Close()
	}
	if m.outputR != nil {
		m.outputR.Close()
	}
	close(m.errCh)
	return nil
}

func (m *MockAdapter) History() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.history))
	copy(out, m.history)
	return out
}
