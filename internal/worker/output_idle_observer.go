package worker

import (
	"sync"
	"time"
)

type cliOutputIdleState string

const (
	cliOutputWaiting       cliOutputIdleState = "waiting_for_model_output"
	cliOutputQuiet         cliOutputIdleState = "output_quiet"
	cliOutputPossibleStall cliOutputIdleState = "possible_pty_stall"
)

type cliOutputIdleDiagnostic struct {
	State      cliOutputIdleState
	IdleFor    time.Duration
	SinceInput time.Duration
	OutputSeen bool
}

type cliOutputIdleObserver struct {
	mu sync.Mutex

	idleAfter  time.Duration
	stallAfter time.Duration
	logEvery   time.Duration
	observeFor time.Duration

	active         bool
	inputAt        time.Time
	lastActivityAt time.Time
	lastLoggedAt   time.Time
	outputSeen     bool
}

func newCLIOutputIdleObserver(idleAfter, stallAfter, logEvery, observeFor time.Duration) *cliOutputIdleObserver {
	return &cliOutputIdleObserver{
		idleAfter:  idleAfter,
		stallAfter: stallAfter,
		logEvery:   logEvery,
		observeFor: observeFor,
	}
}

func (o *cliOutputIdleObserver) BeginInput(now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.active = true
	o.inputAt = now
	o.lastActivityAt = now
	o.lastLoggedAt = time.Time{}
	o.outputSeen = false
}

func (o *cliOutputIdleObserver) CancelInput() {
	o.mu.Lock()
	o.active = false
	o.mu.Unlock()
}

func (o *cliOutputIdleObserver) MarkOutput(now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.active {
		return
	}
	o.lastActivityAt = now
	o.outputSeen = true
}

func (o *cliOutputIdleObserver) Diagnostic(now time.Time) (cliOutputIdleDiagnostic, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.active {
		return cliOutputIdleDiagnostic{}, false
	}
	sinceInput := now.Sub(o.inputAt)
	if sinceInput < 0 || sinceInput > o.observeFor {
		o.active = false
		return cliOutputIdleDiagnostic{}, false
	}
	idleFor := now.Sub(o.lastActivityAt)
	if idleFor < o.idleAfter {
		return cliOutputIdleDiagnostic{}, false
	}
	if !o.lastLoggedAt.IsZero() && now.Sub(o.lastLoggedAt) < o.logEvery {
		return cliOutputIdleDiagnostic{}, false
	}

	state := cliOutputWaiting
	if o.outputSeen {
		state = cliOutputQuiet
	}
	if idleFor >= o.stallAfter {
		state = cliOutputPossibleStall
	}
	o.lastLoggedAt = now
	return cliOutputIdleDiagnostic{
		State:      state,
		IdleFor:    idleFor,
		SinceInput: sinceInput,
		OutputSeen: o.outputSeen,
	}, true
}
