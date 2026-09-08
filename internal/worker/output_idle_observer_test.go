package worker

import (
	"testing"
	"time"
)

func TestCLIOutputIdleObserverClassifiesAndThrottlesSilence(t *testing.T) {
	observer := newCLIOutputIdleObserver(
		10*time.Second,
		40*time.Second,
		10*time.Second,
		2*time.Minute,
	)
	startedAt := time.Unix(100, 0)

	if _, ok := observer.Diagnostic(startedAt); ok {
		t.Fatal("inactive observer produced a diagnostic")
	}

	observer.BeginInput(startedAt)
	if _, ok := observer.Diagnostic(startedAt.Add(9 * time.Second)); ok {
		t.Fatal("diagnostic emitted before idle threshold")
	}

	diag, ok := observer.Diagnostic(startedAt.Add(10 * time.Second))
	if !ok {
		t.Fatal("missing waiting diagnostic")
	}
	if diag.State != cliOutputWaiting {
		t.Fatalf("state = %q, want %q", diag.State, cliOutputWaiting)
	}
	if diag.OutputSeen {
		t.Fatal("output marked as seen before PTY activity")
	}

	if _, ok := observer.Diagnostic(startedAt.Add(15 * time.Second)); ok {
		t.Fatal("diagnostic was not rate limited")
	}

	observer.MarkOutput(startedAt.Add(20 * time.Second))
	diag, ok = observer.Diagnostic(startedAt.Add(30 * time.Second))
	if !ok {
		t.Fatal("missing output quiet diagnostic")
	}
	if diag.State != cliOutputQuiet {
		t.Fatalf("state = %q, want %q", diag.State, cliOutputQuiet)
	}
	if !diag.OutputSeen {
		t.Fatal("PTY activity was not recorded")
	}

	diag, ok = observer.Diagnostic(startedAt.Add(60 * time.Second))
	if !ok {
		t.Fatal("missing possible stall diagnostic")
	}
	if diag.State != cliOutputPossibleStall {
		t.Fatalf("state = %q, want %q", diag.State, cliOutputPossibleStall)
	}
}

func TestCLIOutputIdleObserverStopsAfterWindowOrCancellation(t *testing.T) {
	observer := newCLIOutputIdleObserver(
		time.Second,
		5*time.Second,
		time.Second,
		10*time.Second,
	)
	startedAt := time.Unix(200, 0)

	observer.BeginInput(startedAt)
	if _, ok := observer.Diagnostic(startedAt.Add(11 * time.Second)); ok {
		t.Fatal("diagnostic emitted after observation window")
	}

	observer.BeginInput(startedAt.Add(20 * time.Second))
	observer.CancelInput()
	if _, ok := observer.Diagnostic(startedAt.Add(30 * time.Second)); ok {
		t.Fatal("cancelled observer produced a diagnostic")
	}
}
