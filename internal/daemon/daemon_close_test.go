package daemon

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestStopWaitsForWorkerExitBeforeCancelingDaemonContext(t *testing.T) {
	meta := NewSessionMeta("daemon-stop-waits-for-worker", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 0.15")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		ctx:           ctx,
		cancel:        cancel,
		sessions:      map[string]*SessionMeta{meta.SessionID: meta},
		workers:       map[string]*WorkerHandle{meta.SessionID: handle},
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}
	go d.waitWorkerExit(handle)

	contextCanceledAfterWorkerExit := make(chan bool, 1)
	go func() {
		<-ctx.Done()
		select {
		case <-handle.ExitDone:
			contextCanceledAfterWorkerExit <- true
		default:
			contextCanceledAfterWorkerExit <- false
		}
	}()

	started := time.Now()
	if err := d.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("Stop returned before the worker exited: %v", elapsed)
	}
	if canceledAfterExit := <-contextCanceledAfterWorkerExit; !canceledAfterExit {
		t.Fatal("daemon context was canceled before the worker exit was reaped")
	}
}

func TestStopKillsWorkerThatMissesGracePeriod(t *testing.T) {
	meta := NewSessionMeta("daemon-stop-kills-stuck-worker", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		ctx:           ctx,
		cancel:        cancel,
		sessions:      map[string]*SessionMeta{meta.SessionID: meta},
		workers:       map[string]*WorkerHandle{meta.SessionID: handle},
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}
	go d.waitWorkerExit(handle)

	started := time.Now()
	if err := d.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("Stop exceeded grace period: %v", elapsed)
	}
	select {
	case <-handle.ExitDone:
	default:
		t.Fatal("stuck worker was not reaped after shutdown grace period")
	}
}

func TestCloseSessionPersistsBeforeWorkerExit(t *testing.T) {
	meta := NewSessionMeta("session-close-persisted-first", "bot-test")
	handle := NewWorkerHandle(meta.SessionID)
	handle.Cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := handle.Cmd.Start(); err != nil {
		t.Fatalf("start worker fixture: %v", err)
	}
	handle.Pid = handle.Cmd.Process.Pid

	d := &Daemon{
		sessions:      map[string]*SessionMeta{meta.SessionID: meta},
		workers:       map[string]*WorkerHandle{meta.SessionID: handle},
		store:         NewSessionStore(t.TempDir()),
		spawnFailures: make(map[string]*spawnFailure),
	}
	if err := d.store.save(meta.ToPersisted()); err != nil {
		t.Fatalf("persist session fixture: %v", err)
	}

	workerExited := make(chan struct{})
	go func() {
		d.waitWorkerExit(handle)
		close(workerExited)
	}()
	t.Cleanup(func() {
		if handle.Cmd.Process != nil {
			_ = handle.Cmd.Process.Kill()
		}
		select {
		case <-workerExited:
		case <-time.After(2 * time.Second):
			t.Error("worker fixture was not reaped")
		}
	})

	closed := make(chan struct{})
	go func() {
		d.CloseSession(meta.SessionID, "test")
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("CloseSession blocked waiting for the worker to exit")
	}

	persisted, err := d.store.load(meta.SessionID)
	if err != nil {
		t.Fatalf("load closed session: %v", err)
	}
	if !persisted.Closed {
		t.Fatal("closed session was not persisted before worker exit")
	}

	select {
	case <-workerExited:
		t.Fatal("worker exited before the test could verify asynchronous cleanup")
	default:
	}
}
