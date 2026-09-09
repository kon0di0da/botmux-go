package adapter

import (
	"context"
	"io"
	"sync"
	"time"
)

type TurnStatus string

const (
	TurnCompleted TurnStatus = "completed"
	TurnAborted   TurnStatus = "aborted"
	TurnFailed    TurnStatus = "failed"
)

type AdapterEventKind string

const (
	AdapterOutput       AdapterEventKind = "output"
	AdapterTurnTerminal AdapterEventKind = "turn_terminal"
)

type AdapterEvent struct {
	Kind        AdapterEventKind
	Output      string
	Status      TurnStatus
	ErrorCode   string
	ErrorDetail string
}

type SendResult struct {
	CliSessionID string
}

type CliStartResult struct {
	Input            io.WriteCloser
	Output           io.ReadCloser
	ErrCh            <-chan error
	ReadyResult      <-chan error
	Events           <-chan AdapterEvent
	StructuredOutput bool
	ReadyDelay       time.Duration
}

type CliAdapter interface {
	Start(ctx context.Context, workingDir string) (*CliStartResult, error)
	Send(ctx context.Context, input string) (SendResult, error)
	Close() error
	Name() string
}

type CliTurnInterrupter interface {
	Interrupt(ctx context.Context) error
}

type CliOutputObserver interface {
	NotifyOutput()
}

type AdapterOptions struct {
	CliType         string
	CliPath         string
	Model           string
	Profile         string
	ResumeSessionID string
}

type AdapterFactory func(opts AdapterOptions) CliAdapter

var (
	factoryMu        sync.RWMutex
	adapterFactories = make(map[string]AdapterFactory)
)

func RegisterFactory(kind string, factory AdapterFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	adapterFactories[kind] = factory
}

func Create(opts AdapterOptions) CliAdapter {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	kind := opts.CliType
	if f, ok := adapterFactories[kind]; ok {
		return f(opts)
	}
	return nil
}

func ListKinds() []string {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	result := make([]string, 0, len(adapterFactories))
	for k := range adapterFactories {
		result = append(result, k)
	}
	return result
}
