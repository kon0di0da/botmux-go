package daemon

import (
	"errors"
	"net"
	"os/exec"
	"sync"
	"time"

	"botmux-go/internal/protocol"
)

type WorkerHandle struct {
	SessionID  string
	InstanceID string
	Cmd        *exec.Cmd
	Conn       net.Conn
	Ready      chan struct{}
	ExitDone   chan struct{}
	Pid        int

	mu                     sync.Mutex
	hbSeen                 time.Time
	isReady                bool
	closed                 bool
	startupFailureRecorded bool
}

func NewWorkerHandle(sessionID string) *WorkerHandle {
	return &WorkerHandle{
		SessionID: sessionID,
		Ready:     make(chan struct{}),
		ExitDone:  make(chan struct{}),
		hbSeen:    time.Now(),
	}
}

func (h *WorkerHandle) IsAlive() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	return h.Conn != nil && time.Since(h.hbSeen) < 30*time.Second
}

func (h *WorkerHandle) TouchHb() {
	h.mu.Lock()
	h.hbSeen = time.Now()
	h.mu.Unlock()
}

func (h *WorkerHandle) LastHb() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hbSeen
}

func (h *WorkerHandle) Send(msg *protocol.Message) error {
	h.mu.Lock()
	conn := h.Conn
	h.mu.Unlock()
	if conn == nil {
		return errors.New("worker connection nil")
	}
	_, err := msg.WriteTo(conn)
	return err
}

func (h *WorkerHandle) SetConn(conn net.Conn) net.Conn {
	h.mu.Lock()
	old := h.Conn
	h.Conn = conn
	h.hbSeen = time.Now()
	h.mu.Unlock()
	return old
}

func (h *WorkerHandle) markReady() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.isReady {
		h.isReady = true
		close(h.Ready)
	}
}

func (h *WorkerHandle) IsReady() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.isReady
}

func (h *WorkerHandle) markStartupFailure() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.isReady || h.startupFailureRecorded {
		return false
	}
	h.startupFailureRecorded = true
	return true
}

func (h *WorkerHandle) CloseConn() {
	h.mu.Lock()
	conn := h.Conn
	h.closed = true
	h.Conn = nil
	h.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}
