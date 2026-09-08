package daemon

import (
	"context"
	"os/exec"
	"testing"

	"botmux-go/internal/protocol"
)

func TestWaitWorkerExitCountsOnlyCurrentPreReadyFailure(t *testing.T) {
	tests := []struct {
		name      string
		markReady bool
		wantCount int
	}{
		{name: "pre-ready exit", wantCount: 1},
		{name: "post-ready exit", markReady: true, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := NewSessionMeta("session-exit-test", "bot-test")
			handle := NewWorkerHandle(meta.SessionID)
			if tt.markReady {
				handle.markReady()
			}
			handle.Cmd = exec.Command("/bin/sh", "-c", "exit 1")
			if err := handle.Cmd.Start(); err != nil {
				t.Fatalf("start worker fixture: %v", err)
			}
			handle.Pid = handle.Cmd.Process.Pid

			d := &Daemon{
				sessions:      map[string]*SessionMeta{meta.SessionID: meta},
				workers:       map[string]*WorkerHandle{meta.SessionID: handle},
				spawnFailures: make(map[string]*spawnFailure),
			}
			d.waitWorkerExit(handle)

			d.monitorMu.Lock()
			got := 0
			if failure := d.spawnFailures[meta.SessionID]; failure != nil {
				got = failure.count
			}
			d.monitorMu.Unlock()
			if got != tt.wantCount {
				t.Fatalf("spawn failure count = %d, want %d", got, tt.wantCount)
			}
		})
	}
}

func TestMonitorReadyIgnoresStaleWorkerGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meta := NewSessionMeta("session-generation-test", "bot-test")
	meta.Status = StatusSpawning
	stale := NewWorkerHandle(meta.SessionID)
	stale.markReady()
	current := NewWorkerHandle(meta.SessionID)
	d := &Daemon{
		ctx:      ctx,
		sessions: map[string]*SessionMeta{meta.SessionID: meta},
		workers:  map[string]*WorkerHandle{meta.SessionID: current},
	}

	d.monitorReady(stale, meta)

	if meta.Status != StatusSpawning {
		t.Fatalf("stale monitor changed status to %s", meta.Status)
	}
	if current.IsReady() {
		t.Fatal("stale monitor marked current worker ready")
	}
}

func TestExplicitReadyClearsSpawnFailureBudget(t *testing.T) {
	d, handle := newReadyTestDaemon(t)
	d.spawnFailures = map[string]*spawnFailure{
		handle.SessionID: {count: maxSpawnRetries},
	}
	meta := d.sessions[handle.SessionID]

	d.routeMessage(protocol.NewMessage(protocol.MsgReady, handle.SessionID, "ready"), meta, handle)

	d.monitorMu.Lock()
	_, exists := d.spawnFailures[handle.SessionID]
	d.monitorMu.Unlock()
	if exists {
		t.Fatal("spawn failure budget was not cleared after MsgReady")
	}
}
