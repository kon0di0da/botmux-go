package daemon

import (
	"os/exec"
	"testing"
	"time"
)

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
