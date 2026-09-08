package adapter

import (
	"context"
	"io"
	"sync"
	"time"
)

type CliStartResult struct {
	Input      io.WriteCloser
	Output     io.ReadCloser
	ErrCh      <-chan error
	ReadyDelay time.Duration
}

type CliAdapter interface {
	Start(ctx context.Context, workingDir string) (*CliStartResult, error)
	Send(ctx context.Context, input string) error
	Close() error
	Name() string
}

type CliOutputObserver interface {
	NotifyOutput()
}

type AdapterOptions struct {
	CliType string
	CliPath string
	Model   string
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
