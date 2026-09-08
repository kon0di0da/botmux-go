package adapter

import (
	"context"
	"io"
	"testing"
)

func TestLegacyAdapterReturnsEmptySendResult(t *testing.T) {
	a := NewMockAdapter("mock")
	start, err := a.Start(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	go io.Copy(io.Discard, start.Output)
	result, err := a.Send(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if result.CliSessionID != "" {
		t.Fatalf("CliSessionID = %q, want empty", result.CliSessionID)
	}
}
