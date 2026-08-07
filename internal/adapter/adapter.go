package adapter

import (
	"context"
	"io"
	"sync"
)

type CliStartResult struct {
	Input  io.WriteCloser
	Output io.ReadCloser
	ErrCh  <-chan error
}

type CliAdapter interface {
	Start(ctx context.Context, workingDir string) (*CliStartResult, error)
	Send(ctx context.Context, input string) error
	Close() error
	Name() string
}

type AdapterFactory func(configType string, cliPath string) CliAdapter

var (
	factoryMu       sync.RWMutex
	adapterFactories = make(map[string]AdapterFactory)
)

func RegisterFactory(kind string, factory AdapterFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	adapterFactories[kind] = factory
}

func Create(kind string, cliPath string) CliAdapter {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	if f, ok := adapterFactories[kind]; ok {
		return f(kind, cliPath)
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
