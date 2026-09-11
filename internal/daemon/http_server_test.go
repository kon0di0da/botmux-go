package daemon

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"botmux-go/internal/config"
	"botmux-go/internal/protocol"
)

func TestSessionDetailIncludesTurnState(t *testing.T) {
	fixture := newHTTPServerTestFixture(t, true)
	fixture.meta.PublishTerminal(protocol.TurnTerminal{Status: protocol.TurnAborted})
	if !fixture.meta.BeginTurn() {
		t.Fatal("begin turn")
	}
	if _, ok := fixture.meta.BeginTurnCancel(); !ok {
		t.Fatal("begin turn cancellation")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/"+fixture.meta.SessionID, nil)
	rec := httptest.NewRecorder()
	fixture.daemon.handleSessionsSubrouter(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got struct {
		TurnActive     bool                   `json:"turn_active"`
		TurnCancelling bool                   `json:"turn_cancelling"`
		LatestTerminal *protocol.TurnTerminal `json:"latest_terminal"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !got.TurnActive {
		t.Fatal("turn_active = false, want true")
	}
	if !got.TurnCancelling {
		t.Fatal("turn_cancelling = false, want true")
	}
	if got.LatestTerminal == nil || got.LatestTerminal.Status != protocol.TurnAborted {
		t.Fatalf("latest_terminal = %#v, want aborted terminal", got.LatestTerminal)
	}
}

func TestCancelEndpointAcceptsActiveCodexTurn(t *testing.T) {
	fixture := newHTTPServerTestFixture(t, true)
	if !fixture.meta.BeginTurn() {
		t.Fatal("begin turn")
	}

	type readResult struct {
		message *protocol.Message
		err     error
	}
	read := make(chan readResult, 1)
	go func() {
		message, err := protocol.DecodeMessage(fixture.workerConn)
		read <- readResult{message: message, err: err}
	}()

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+fixture.meta.SessionID+"/cancel", nil)
	rec := httptest.NewRecorder()
	fixture.daemon.handleSessionsSubrouter(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	var got struct {
		OK         bool   `json:"ok"`
		SessionID  string `json:"session_id"`
		Cancelling bool   `json:"cancelling"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !got.OK || got.SessionID != fixture.meta.SessionID || !got.Cancelling {
		t.Fatalf("response = %#v, want accepted cancellation for %q", got, fixture.meta.SessionID)
	}

	select {
	case result := <-read:
		if result.err != nil {
			t.Fatalf("read cancel IPC: %v", result.err)
		}
		if result.message.Type != protocol.MsgCancelTurn {
			t.Fatalf("IPC message type = %s, want %s", result.message.Type, protocol.MsgCancelTurn)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel endpoint did not send worker IPC")
	}

	snapshot := fixture.meta.TurnSnapshot()
	if !snapshot.Active || !snapshot.Cancelling {
		t.Fatalf("turn snapshot = %#v, want active cancelling turn", snapshot)
	}
}

func TestCancelEndpointRejectsIneligibleSessionWithoutIPC(t *testing.T) {
	tests := []struct {
		name  string
		ready bool
		setup func(*httpServerTestFixture)
	}{
		{
			name:  "closed",
			ready: true,
			setup: func(fixture *httpServerTestFixture) {
				fixture.meta.Closed = true
				fixture.meta.Status = StatusClosed
			},
		},
		{
			name:  "non-Codex",
			ready: true,
			setup: func(fixture *httpServerTestFixture) {
				fixture.meta.CliType = string(config.CliMock)
				if !fixture.meta.BeginTurn() {
					t.Fatal("begin turn")
				}
			},
		},
		{
			name:  "idle",
			ready: true,
			setup: func(*httpServerTestFixture) {},
		},
		{
			name:  "duplicate cancellation",
			ready: true,
			setup: func(fixture *httpServerTestFixture) {
				if !fixture.meta.BeginTurn() {
					t.Fatal("begin turn")
				}
				if _, ok := fixture.meta.BeginTurnCancel(); !ok {
					t.Fatal("begin turn cancellation")
				}
			},
		},
		{
			name:  "no ready worker",
			ready: false,
			setup: func(fixture *httpServerTestFixture) {
				if !fixture.meta.BeginTurn() {
					t.Fatal("begin turn")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newHTTPServerTestFixture(t, tt.ready)
			tt.setup(fixture)

			req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+fixture.meta.SessionID+"/cancel", nil)
			rec := httptest.NewRecorder()
			fixture.daemon.handleSessionsSubrouter(rec, req)

			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
			}
			assertNoHTTPTestIPC(t, fixture.workerConn)
		})
	}
}

func TestCancelEndpointReturnsNotFoundForUnknown(t *testing.T) {
	fixture := newHTTPServerTestFixture(t, true)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/unknown/cancel", nil)
	rec := httptest.NewRecorder()
	fixture.daemon.handleSessionsSubrouter(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

type httpServerTestFixture struct {
	daemon     *Daemon
	meta       *SessionMeta
	handle     *WorkerHandle
	workerConn net.Conn
}

func newHTTPServerTestFixture(t *testing.T, ready bool) *httpServerTestFixture {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.DefaultConfig()
	cfg.SessionsDir = t.TempDir()
	meta := NewSessionMeta("http-server-test", "bot-codex")
	meta.CliType = string(config.CliCodex)
	meta.Status = StatusReady

	handle := NewWorkerHandle(meta.SessionID)
	if ready {
		handle.markReady()
	}
	daemonConn, workerConn := net.Pipe()
	handle.SetConn(daemonConn)

	d := &Daemon{
		cfg:                cfg,
		store:              NewSessionStore(cfg.SessionsDir),
		ctx:                ctx,
		cancel:             cancel,
		sessions:           map[string]*SessionMeta{meta.SessionID: meta},
		workers:            map[string]*WorkerHandle{meta.SessionID: handle},
		workerOwners:       map[*WorkerHandle]*SessionMeta{handle: meta},
		pendingRestart:     make(map[*SessionMeta]*WorkerHandle),
		spawnFailures:      make(map[string]*spawnFailure),
		turnCancelTimeout:  time.Minute,
		restartWorkerGrace: time.Minute,
	}
	t.Cleanup(func() {
		cancel()
		_ = daemonConn.Close()
		_ = workerConn.Close()
	})
	return &httpServerTestFixture{
		daemon:     d,
		meta:       meta,
		handle:     handle,
		workerConn: workerConn,
	}
}

func assertNoHTTPTestIPC(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer conn.SetReadDeadline(time.Time{})

	if message, err := protocol.DecodeMessage(conn); err == nil {
		t.Fatalf("unexpected worker IPC: %#v", message)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read worker IPC: %v", err)
	}
}
