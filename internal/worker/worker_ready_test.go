package worker

import (
	"testing"

	"botmux-go/internal/adapter"
)

func TestWaitForCLIReadyUsesAuthoritativeResult(t *testing.T) {
	ready := make(chan error, 1)
	ready <- nil
	w := New(Options{CliType: "mock"})
	defer w.Cancel()
	w.startResult = &adapter.CliStartResult{
		ReadyResult: ready,
	}
	if err := w.waitForCLIReady(); err != nil {
		t.Fatalf("waitForCLIReady: %v", err)
	}
}
