package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/protocol"
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

func TestWorkerReadyHandshakeTimeoutDefaultsWhenUnset(t *testing.T) {
	w := New(Options{CliType: "mock"})
	t.Cleanup(w.Cancel)

	if got := w.readyHandshakeTimeout; got != defaultReadyHandshakeTimeout {
		t.Fatalf("ready handshake timeout = %v, want %v", got, defaultReadyHandshakeTimeout)
	}
	for _, timeout := range []time.Duration{0, -time.Second} {
		w.readyHandshakeTimeout = timeout
		if got := w.readyHandshakeTimeoutOrDefault(); got != defaultReadyHandshakeTimeout {
			t.Fatalf("ready handshake timeout for %v = %v, want %v", timeout, got, defaultReadyHandshakeTimeout)
		}
	}
}

func TestWorkerStartFailureCleansUpAdapter(t *testing.T) {
	startErr := errors.New("adapter start failed")
	daemonConn, workerConn := net.Pipe()
	t.Cleanup(func() { _ = daemonConn.Close() })

	cli := newCleanupTrackingTestAdapter(startErr)
	w := New(Options{
		SessionID:  "worker-start-failure",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	w.cliAdapter = cli
	w.dial = func(string, string) (net.Conn, error) {
		return workerConn, nil
	}

	err := w.Run()
	if !errors.Is(err, startErr) {
		t.Fatalf("Run error = %v, want %v", err, startErr)
	}
	if w.startResult != nil {
		t.Fatal("worker retained a start result after Start returned an error")
	}
	waitForSignal(t, cli.closed, "adapter Close after startup failure")
	if got := cli.closeCalls.Load(); got != 1 {
		t.Fatalf("adapter Close calls = %d, want 1", got)
	}
	if w.ctx.Err() == nil {
		t.Fatal("worker context was not canceled after startup failure")
	}
}

func TestWorkerInitialHandshakeRejectionCleansUpAdapter(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	cli := newInitialHandshakeRejectTestAdapter()
	w := New(Options{
		SessionID:        "worker-initial-rejected",
		WorkerInstanceID: "stale-nonce-123",
		DaemonAddr:       listener.Addr().String(),
		CliType:          "mock",
	})
	w.cliAdapter = cli

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- fmt.Errorf("accept worker: %w", err)
			return
		}
		defer conn.Close()

		msg, err := protocol.NewMessageReader(conn).Read()
		if err != nil {
			serverDone <- fmt.Errorf("read ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			serverDone <- fmt.Errorf("first message = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		if msg.WorkerInstanceID != w.workerInstanceID {
			serverDone <- fmt.Errorf("worker instance ID = %q, want %q", msg.WorkerInstanceID, w.workerInstanceID)
			return
		}
		if _, err := protocol.NewMessage(protocol.MsgError, w.sessionID, "worker instance mismatch").WriteTo(conn); err != nil {
			serverDone <- fmt.Errorf("reject ready: %w", err)
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		var data [1]byte
		if _, err := conn.Read(data[:]); err != io.EOF {
			serverDone <- fmt.Errorf("read after rejection = %v, want EOF", err)
			return
		}
		serverDone <- nil
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run succeeded after ready rejection")
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ready rejection")
	}

	select {
	case <-cli.closed:
	case <-time.After(time.Second):
		t.Fatal("adapter Close was not called")
	}
	if got := cli.closeCalls.Load(); got != 1 {
		t.Fatalf("adapter Close calls = %d, want 1", got)
	}
	if w.ctx.Err() == nil {
		t.Fatal("worker context was not canceled")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not close rejected connection")
	}
}

func TestWorkerHandshakeRejectsMalformedAcknowledgement(t *testing.T) {
	const sessionID = "worker-malformed-ack"

	tests := []struct {
		name       string
		response   func() *protocol.Message
		wantDetail string
	}{
		{
			name: "ack with wrong session",
			response: func() *protocol.Message {
				return protocol.NewMessage(protocol.MsgAck, "other-session", workerReadyAckPayload)
			},
			wantDetail: `session="other-session"`,
		},
		{
			name: "ack with wrong payload",
			response: func() *protocol.Message {
				return protocol.NewMessage(protocol.MsgAck, sessionID, "not_worker_ready")
			},
			wantDetail: `payload="not_worker_ready"`,
		},
		{
			name: "heartbeat",
			response: func() *protocol.Message {
				return protocol.NewMessage(protocol.MsgHeartbeat, sessionID, "unexpected")
			},
			wantDetail: "type=heartbeat",
		},
		{
			name: "error",
			response: func() *protocol.Message {
				return protocol.NewMessage(protocol.MsgError, sessionID, "daemon rejected worker")
			},
			wantDetail: "daemon rejected worker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			daemonConn, workerConn := net.Pipe()
			handshakeWorker := New(Options{
				SessionID:  sessionID,
				DaemonAddr: "daemon-test",
				CliType:    "mock",
			})
			handshakePeerDone := make(chan error, 1)
			go func() {
				defer daemonConn.Close()
				msg, err := protocol.NewMessageReader(daemonConn).Read()
				if err != nil {
					handshakePeerDone <- fmt.Errorf("read ready: %w", err)
					return
				}
				if msg.Type != protocol.MsgReady {
					handshakePeerDone <- fmt.Errorf("message type = %s, want %s", msg.Type, protocol.MsgReady)
					return
				}
				_, err = tt.response().WriteTo(daemonConn)
				handshakePeerDone <- err
			}()

			_, err := handshakeWorker.readyHandshake(workerConn)
			_ = workerConn.Close()
			var rejected *workerHandshakeRejectedError
			if !errors.As(err, &rejected) {
				t.Errorf("readyHandshake error = %T (%v), want workerHandshakeRejectedError", err, err)
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantDetail) {
				t.Errorf("readyHandshake error = %v, want detail %q", err, tt.wantDetail)
			}
			select {
			case err := <-handshakePeerDone:
				if err != nil {
					t.Fatalf("send malformed acknowledgement: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("handshake peer did not finish")
			}

			oldDaemonConn, oldWorkerConn := net.Pipe()
			newDaemonConn, newWorkerConn := net.Pipe()
			reconnectWorker := New(Options{
				SessionID:  sessionID,
				DaemonAddr: "daemon-test",
				CliType:    "mock",
			})
			reconnectWorker.conn = oldWorkerConn
			reconnectWorker.msgReader = protocol.NewMessageReader(oldWorkerConn)
			reconnectWorker.reconnectSleep = func(time.Duration) {}

			var dialCalls atomic.Int32
			secondDial := make(chan struct{}, 1)
			reconnectWorker.dial = func(string, string) (net.Conn, error) {
				if dialCalls.Add(1) == 1 {
					return newWorkerConn, nil
				}
				select {
				case secondDial <- struct{}{}:
				default:
				}
				<-reconnectWorker.ctx.Done()
				return nil, reconnectWorker.ctx.Err()
			}
			t.Cleanup(func() {
				reconnectWorker.Cancel()
				_ = oldDaemonConn.Close()
				_ = newDaemonConn.Close()
			})

			reconnectPeerDone := make(chan error, 1)
			go func() {
				defer newDaemonConn.Close()
				msg, err := protocol.NewMessageReader(newDaemonConn).Read()
				if err != nil {
					reconnectPeerDone <- fmt.Errorf("read reconnect ready: %w", err)
					return
				}
				if msg.Type != protocol.MsgReady {
					reconnectPeerDone <- fmt.Errorf("reconnect message type = %s, want %s", msg.Type, protocol.MsgReady)
					return
				}
				_, err = tt.response().WriteTo(newDaemonConn)
				reconnectPeerDone <- err
			}()

			reconnectDone := make(chan struct{})
			go func() {
				reconnectWorker.reconnectToDaemon()
				close(reconnectDone)
			}()

			select {
			case <-reconnectDone:
			case <-secondDial:
				t.Fatal("reconnect retried after malformed acknowledgement")
			case <-time.After(time.Second):
				t.Fatal("reconnect did not stop after malformed acknowledgement")
			}
			if got := dialCalls.Load(); got != 1 {
				t.Errorf("dial calls = %d, want 1", got)
			}
			if !reconnectWorker.isClosed() {
				t.Error("worker was not cleaned up after malformed acknowledgement")
			}
			reconnectWorker.connMu.Lock()
			publishedConn := reconnectWorker.conn
			reconnectWorker.connMu.Unlock()
			if publishedConn != nil {
				t.Error("worker retained a connection after malformed acknowledgement")
			}
			select {
			case err := <-reconnectPeerDone:
				if err != nil {
					t.Fatalf("send reconnect malformed acknowledgement: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("reconnect peer did not finish")
			}
		})
	}
}

func TestWorkerHandshakeRejectsInvalidJSONAcknowledgement(t *testing.T) {
	const sessionID = "worker-invalid-json-ack"

	tests := []struct {
		name string
		raw  string
	}{
		{name: "plain text", raw: "not-json\n"},
		{name: "malformed object", raw: "{bad}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			daemonConn, workerConn := net.Pipe()
			handshakeWorker := New(Options{
				SessionID:  sessionID,
				DaemonAddr: "daemon-test",
				CliType:    "mock",
			})
			handshakePeerDone := make(chan error, 1)
			go func() {
				defer daemonConn.Close()
				msg, err := protocol.NewMessageReader(daemonConn).Read()
				if err != nil {
					handshakePeerDone <- fmt.Errorf("read ready: %w", err)
					return
				}
				if msg.Type != protocol.MsgReady {
					handshakePeerDone <- fmt.Errorf("message type = %s, want %s", msg.Type, protocol.MsgReady)
					return
				}
				_, err = io.WriteString(daemonConn, tt.raw)
				handshakePeerDone <- err
			}()

			_, err := handshakeWorker.readyHandshake(workerConn)
			_ = workerConn.Close()
			var rejected *workerHandshakeRejectedError
			if !errors.As(err, &rejected) {
				t.Errorf("readyHandshake error = %T (%v), want workerHandshakeRejectedError", err, err)
			}
			select {
			case err := <-handshakePeerDone:
				if err != nil {
					t.Fatalf("send invalid JSON acknowledgement: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("handshake peer did not finish")
			}

			oldDaemonConn, oldWorkerConn := net.Pipe()
			newDaemonConn, newWorkerConn := net.Pipe()
			reconnectWorker := New(Options{
				SessionID:  sessionID,
				DaemonAddr: "daemon-test",
				CliType:    "mock",
			})
			reconnectWorker.conn = oldWorkerConn
			reconnectWorker.msgReader = protocol.NewMessageReader(oldWorkerConn)
			reconnectWorker.reconnectSleep = func(time.Duration) {}

			var dialCalls atomic.Int32
			secondDial := make(chan struct{}, 1)
			reconnectWorker.dial = func(string, string) (net.Conn, error) {
				if dialCalls.Add(1) == 1 {
					return newWorkerConn, nil
				}
				select {
				case secondDial <- struct{}{}:
				default:
				}
				<-reconnectWorker.ctx.Done()
				return nil, reconnectWorker.ctx.Err()
			}
			t.Cleanup(func() {
				reconnectWorker.Cancel()
				_ = oldDaemonConn.Close()
				_ = newDaemonConn.Close()
			})

			reconnectPeerDone := make(chan error, 1)
			go func() {
				defer newDaemonConn.Close()
				msg, err := protocol.NewMessageReader(newDaemonConn).Read()
				if err != nil {
					reconnectPeerDone <- fmt.Errorf("read reconnect ready: %w", err)
					return
				}
				if msg.Type != protocol.MsgReady {
					reconnectPeerDone <- fmt.Errorf("reconnect message type = %s, want %s", msg.Type, protocol.MsgReady)
					return
				}
				_, err = io.WriteString(newDaemonConn, tt.raw)
				reconnectPeerDone <- err
			}()

			reconnectDone := make(chan struct{})
			go func() {
				reconnectWorker.reconnectToDaemon()
				close(reconnectDone)
			}()

			select {
			case <-reconnectDone:
			case <-secondDial:
				t.Fatal("reconnect retried after invalid JSON acknowledgement")
			case <-time.After(time.Second):
				t.Fatal("reconnect did not stop after invalid JSON acknowledgement")
			}
			if got := dialCalls.Load(); got != 1 {
				t.Errorf("dial calls = %d, want 1", got)
			}
			if !reconnectWorker.isClosed() {
				t.Error("worker was not cleaned up after invalid JSON acknowledgement")
			}
			reconnectWorker.connMu.Lock()
			publishedConn := reconnectWorker.conn
			reconnectWorker.connMu.Unlock()
			if publishedConn != nil {
				t.Error("worker retained a connection after invalid JSON acknowledgement")
			}
			select {
			case err := <-reconnectPeerDone:
				if err != nil {
					t.Fatalf("send reconnect invalid JSON acknowledgement: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("reconnect peer did not finish")
			}
		})
	}
}

func TestWorkerHandshakeKeepsConnectionCloseTemporary(t *testing.T) {
	daemonConn, workerConn := net.Pipe()
	t.Cleanup(func() { _ = workerConn.Close() })

	w := New(Options{
		SessionID:  "worker-closed-ack",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	peerDone := make(chan error, 1)
	go func() {
		defer daemonConn.Close()
		msg, err := protocol.NewMessageReader(daemonConn).Read()
		if err != nil {
			peerDone <- fmt.Errorf("read ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			peerDone <- fmt.Errorf("message type = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		peerDone <- nil
	}()

	_, err := w.readyHandshake(workerConn)
	var rejected *workerHandshakeRejectedError
	if errors.As(err, &rejected) {
		t.Fatalf("readyHandshake error = %T (%v), want temporary connection error", err, err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readyHandshake error = %v, want EOF", err)
	}
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake peer did not finish")
	}
}

func TestWorkerReadyHandshakeTimesOut(t *testing.T) {
	daemonConn, rawWorkerConn := net.Pipe()
	workerConn := newDeadlineRecordingConn(rawWorkerConn)
	w := New(Options{
		SessionID:  "worker-ready-timeout",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	w.readyHandshakeTimeout = 20 * time.Millisecond

	peerObservedReady := make(chan struct{})
	peerRelease := make(chan struct{})
	var releasePeerOnce sync.Once
	releasePeer := func() {
		releasePeerOnce.Do(func() { close(peerRelease) })
	}
	peerDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(daemonConn).Read()
		if err != nil {
			peerDone <- fmt.Errorf("read ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			peerDone <- fmt.Errorf("message type = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		close(peerObservedReady)
		<-peerRelease
		peerDone <- nil
	}()
	t.Cleanup(func() {
		releasePeer()
		w.Cancel()
		_ = workerConn.Close()
		_ = daemonConn.Close()
	})

	handshakeDone := make(chan error, 1)
	go func() {
		_, err := w.readyHandshake(workerConn)
		handshakeDone <- err
	}()

	waitForSignal(t, peerObservedReady, "ready handshake write")
	select {
	case err := <-handshakeDone:
		var rejected *workerHandshakeRejectedError
		if errors.As(err, &rejected) {
			t.Fatalf("readyHandshake error = %T (%v), want temporary timeout", err, err)
		}
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("readyHandshake error = %T (%v), want timeout", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("ready handshake did not time out")
	}

	deadlines := workerConn.deadlines()
	if len(deadlines) != 1 || deadlines[0].IsZero() {
		t.Fatalf("handshake deadlines = %v, want one non-zero deadline", deadlines)
	}
	w.Cancel()
	select {
	case <-workerConn.closed:
		t.Fatal("timed-out handshake cancellation watcher did not exit")
	case <-time.After(100 * time.Millisecond):
	}
	releasePeer()
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout peer did not exit")
	}
}

func TestWorkerReadyHandshakeClearsDeadlineAfterAcknowledgment(t *testing.T) {
	daemonConn, rawWorkerConn := net.Pipe()
	workerConn := newDeadlineRecordingConn(rawWorkerConn)
	w := New(Options{
		SessionID:  "worker-ready-deadline-clear",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	t.Cleanup(func() {
		w.Cancel()
		_ = workerConn.Close()
		_ = daemonConn.Close()
	})

	peerDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(daemonConn).Read()
		if err != nil {
			peerDone <- fmt.Errorf("read ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			peerDone <- fmt.Errorf("message type = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		_, err = protocol.NewMessage(protocol.MsgAck, w.sessionID, workerReadyAckPayload).WriteTo(daemonConn)
		peerDone <- err
	}()

	if _, err := w.readyHandshake(workerConn); err != nil {
		t.Fatalf("readyHandshake: %v", err)
	}
	deadlines := workerConn.deadlines()
	if len(deadlines) != 2 || deadlines[0].IsZero() || !deadlines[1].IsZero() {
		t.Fatalf("handshake deadlines = %v, want non-zero deadline followed by clear", deadlines)
	}
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("acknowledgment peer did not finish")
	}
}

func TestWorkerReadyHandshakeCancelsPrivateConnection(t *testing.T) {
	daemonConn, workerConn := net.Pipe()
	w := New(Options{
		SessionID:  "worker-ready-cancel",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	w.readyHandshakeTimeout = time.Second
	t.Cleanup(func() {
		w.Cancel()
		_ = workerConn.Close()
		_ = daemonConn.Close()
	})

	readySent := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(daemonConn).Read()
		if err != nil {
			peerDone <- fmt.Errorf("read ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			peerDone <- fmt.Errorf("message type = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		close(readySent)
		var data [1]byte
		_, err = daemonConn.Read(data[:])
		peerDone <- err
	}()

	handshakeDone := make(chan error, 1)
	go func() {
		_, err := w.readyHandshake(workerConn)
		handshakeDone <- err
	}()

	waitForSignal(t, readySent, "ready handshake write before cancellation")
	w.Cancel()
	select {
	case err := <-handshakeDone:
		var rejected *workerHandshakeRejectedError
		if errors.As(err, &rejected) {
			t.Fatalf("readyHandshake error = %T (%v), want temporary connection error", err, err)
		}
		if err == nil {
			t.Fatal("readyHandshake succeeded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("ready handshake did not return after cancellation")
	}
	select {
	case err := <-peerDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("private connection read after cancellation = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("private connection was not closed by cancellation")
	}
	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != nil {
		t.Fatal("ready handshake published a connection before acknowledgment")
	}
}

func TestWorkerReconnectRetriesAfterReadyHandshakeTimeout(t *testing.T) {
	oldDaemonConn, oldWorkerConn := net.Pipe()
	firstDaemonConn, firstWorkerConn := net.Pipe()
	secondDaemonConn, secondWorkerConn := net.Pipe()
	w := New(Options{
		SessionID:        "worker-reconnect-timeout",
		WorkerInstanceID: "timeout-nonce",
		DaemonAddr:       "daemon-test",
		CliType:          "mock",
	})
	w.readyHandshakeTimeout = 20 * time.Millisecond
	w.conn = oldWorkerConn
	w.msgReader = protocol.NewMessageReader(oldWorkerConn)
	w.reconnectSleep = func(time.Duration) {}

	var dialCalls atomic.Int32
	w.dial = func(string, string) (net.Conn, error) {
		switch dialCalls.Add(1) {
		case 1:
			return firstWorkerConn, nil
		case 2:
			return secondWorkerConn, nil
		default:
			return nil, errors.New("unexpected reconnect dial")
		}
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = oldDaemonConn.Close()
		_ = firstDaemonConn.Close()
		_ = firstWorkerConn.Close()
		_ = secondDaemonConn.Close()
		_ = secondWorkerConn.Close()
	})

	firstReady := make(chan struct{})
	firstPeerDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(firstDaemonConn).Read()
		if err != nil {
			firstPeerDone <- fmt.Errorf("read first ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			firstPeerDone <- fmt.Errorf("first message type = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		close(firstReady)
		var data [1]byte
		_, err = firstDaemonConn.Read(data[:])
		firstPeerDone <- err
	}()

	secondPeerDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(secondDaemonConn).Read()
		if err != nil {
			secondPeerDone <- fmt.Errorf("read second ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			secondPeerDone <- fmt.Errorf("second message type = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		_, err = protocol.NewMessage(protocol.MsgAck, w.sessionID, workerReadyAckPayload).WriteTo(secondDaemonConn)
		secondPeerDone <- err
	}()

	reconnectDone := make(chan struct{})
	go func() {
		w.reconnectToDaemon()
		close(reconnectDone)
	}()

	waitForSignal(t, firstReady, "first reconnect ready")
	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != oldWorkerConn {
		t.Fatal("reconnect published the timed-out connection")
	}

	select {
	case <-reconnectDone:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not retry after ready handshake timeout")
	}
	if got := dialCalls.Load(); got != 2 {
		t.Fatalf("reconnect dial calls = %d, want 2", got)
	}
	w.connMu.Lock()
	publishedConn = w.conn
	publishedReader := w.msgReader
	w.connMu.Unlock()
	if publishedConn != secondWorkerConn {
		t.Fatal("reconnect did not publish the acknowledged connection")
	}
	if publishedReader == nil {
		t.Fatal("reconnect did not publish a message reader")
	}
	if !w.isConnected() {
		t.Fatal("reconnect did not mark worker connected after retry")
	}
	select {
	case err := <-firstPeerDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("first connection read after timeout = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed-out reconnect connection was not closed")
	}
	select {
	case err := <-secondPeerDone:
		if err != nil {
			t.Fatalf("acknowledge second reconnect ready: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second reconnect peer did not finish")
	}
}

func TestWorkerReadyIncludesInstanceID(t *testing.T) {
	daemonConn, workerConn := net.Pipe()
	defer daemonConn.Close()
	defer workerConn.Close()

	w := New(Options{
		SessionID:        "worker-ready-nonce",
		CliType:          "mock",
		WorkerInstanceID: "nonce-123",
	})
	defer w.Cancel()
	w.conn = workerConn

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- w.sendReady()
	}()

	msg, err := protocol.DecodeMessage(daemonConn)
	if err != nil {
		t.Fatalf("decode ready message: %v", err)
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("send ready message: %v", err)
	}
	if msg.Type != protocol.MsgReady {
		t.Errorf("type = %q, want %q", msg.Type, protocol.MsgReady)
	}
	if msg.SessionID != "worker-ready-nonce" {
		t.Errorf("session ID = %q, want %q", msg.SessionID, "worker-ready-nonce")
	}
	if msg.WorkerInstanceID != "nonce-123" {
		t.Errorf("worker instance ID = %q, want %q", msg.WorkerInstanceID, "nonce-123")
	}
}

func TestWorkerSendsReadyBeforeHeartbeats(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	ready := make(chan error, 1)
	ticks := make(chan time.Time, 1)
	cli := newDelayedReadyTestAdapter(ready)
	w := New(Options{
		SessionID:        "worker-ready-order",
		WorkerInstanceID: "nonce-heartbeat-123",
		DaemonAddr:       listener.Addr().String(),
		CliType:          "mock",
	})
	w.cliAdapter = cli
	w.heartbeatTicks = ticks

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()
	runExited := false

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept worker: %v", err)
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = cli.Close()
		_ = conn.Close()
		if !runExited {
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Error("worker did not exit during cleanup")
			}
		}
	})

	reader := protocol.NewMessageReader(conn)
	ticks <- time.Now()
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set pre-ready read deadline: %v", err)
	}
	if msg, err := reader.Read(); err == nil {
		t.Fatalf("worker sent %s before ready: %#v", msg.Type, msg)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before ready: %v, want timeout", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("queued heartbeat tick was consumed before ready")
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear pre-ready read deadline: %v", err)
	}

	ready <- nil
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set post-ready read deadline: %v", err)
	}
	first, err := reader.Read()
	if err != nil {
		t.Fatalf("read ready message: %v", err)
	}
	if first.Type != protocol.MsgReady {
		t.Fatalf("first worker message = %s, want %s", first.Type, protocol.MsgReady)
	}
	if first.WorkerInstanceID != "nonce-heartbeat-123" {
		t.Fatalf("ready worker instance ID = %q, want %q", first.WorkerInstanceID, "nonce-heartbeat-123")
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, w.sessionID, "worker_ready").WriteTo(conn); err != nil {
		t.Fatalf("acknowledge ready: %v", err)
	}
	heartbeat, err := reader.Read()
	if err != nil {
		t.Fatalf("read queued heartbeat: %v", err)
	}
	if heartbeat.Type != protocol.MsgHeartbeat {
		t.Fatalf("post-ready message = %s, want %s", heartbeat.Type, protocol.MsgHeartbeat)
	}
	if heartbeat.WorkerInstanceID != "nonce-heartbeat-123" {
		t.Fatalf("heartbeat worker instance ID = %q, want %q", heartbeat.WorkerInstanceID, "nonce-heartbeat-123")
	}

	if _, err := protocol.NewMessage(protocol.MsgClose, w.sessionID, "").WriteTo(conn); err != nil {
		t.Fatalf("send close: %v", err)
	}
	select {
	case err := <-runDone:
		runExited = true
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgClose")
	}
}

func TestWorkerSendsReadyBeforeOutputEvents(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	ready := make(chan error, 1)
	cli := newReadyOutputTestAdapter(ready)
	w := New(Options{
		SessionID:        "worker-ready-output-order",
		WorkerInstanceID: "nonce-output-123",
		DaemonAddr:       listener.Addr().String(),
		CliType:          "mock",
	})
	w.cliAdapter = cli

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()
	runExited := false

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept worker: %v", err)
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = cli.Close()
		_ = conn.Close()
		if !runExited {
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Error("worker did not exit during cleanup")
			}
		}
	})

	reader := protocol.NewMessageReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set pre-ready read deadline: %v", err)
	}
	if msg, err := reader.Read(); err == nil {
		t.Fatalf("worker sent %s before ready: %#v", msg.Type, msg)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before ready: %v, want timeout", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}

	ready <- nil

	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set ready read deadline: %v", err)
	}
	first, err := reader.Read()
	if err != nil {
		t.Fatalf("read ready message: %v", err)
	}
	if first.Type != protocol.MsgReady {
		t.Fatalf("first worker message = %s, want %s", first.Type, protocol.MsgReady)
	}
	if first.WorkerInstanceID != "nonce-output-123" {
		t.Fatalf("ready worker instance ID = %q, want %q", first.WorkerInstanceID, "nonce-output-123")
	}

	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set pre-ack read deadline: %v", err)
	}
	if msg, err := reader.Read(); err == nil {
		t.Fatalf("worker sent %s before ready acknowledgment: %#v", msg.Type, msg)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before ready acknowledgment: %v, want timeout", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear pre-ack read deadline: %v", err)
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, w.sessionID, "worker_ready").WriteTo(conn); err != nil {
		t.Fatalf("acknowledge ready: %v", err)
	}
	select {
	case <-cli.eventSent:
	case <-time.After(time.Second):
		t.Fatal("adapter output event was not consumed after ready acknowledgment")
	}
	output, err := reader.Read()
	if err != nil {
		t.Fatalf("read output message: %v", err)
	}
	if output.Type != protocol.MsgOutput || output.Payload != "ready-gated output" {
		t.Fatalf("output message = %#v, want ready-gated output", output)
	}

	if _, err := protocol.NewMessage(protocol.MsgClose, w.sessionID, "").WriteTo(conn); err != nil {
		t.Fatalf("send close: %v", err)
	}
	select {
	case err := <-runDone:
		runExited = true
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgClose")
	}
}

func TestWorkerDrainsStartupOutputBeforeReady(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	cli := newStartupOutputReadyTestAdapter()
	w := New(Options{
		SessionID:        "worker-drain-startup-output",
		WorkerInstanceID: "nonce-drain-output-123",
		DaemonAddr:       listener.Addr().String(),
		CliType:          "mock",
	})
	w.cliAdapter = cli

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()
	runExited := false

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept worker: %v", err)
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = cli.Close()
		_ = conn.Close()
		if !runExited {
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Error("worker did not exit during cleanup")
			}
		}
	})

	waitForSignal(t, cli.startupWritesComplete, "adapter startup output writes to complete")

	reader := protocol.NewMessageReader(conn)
	first, err := reader.Read()
	if err != nil {
		t.Fatalf("read ready message: %v", err)
	}
	if first.Type != protocol.MsgReady {
		t.Fatalf("first worker message = %s, want %s", first.Type, protocol.MsgReady)
	}
	if first.WorkerInstanceID != "nonce-drain-output-123" {
		t.Fatalf("ready worker instance ID = %q, want %q", first.WorkerInstanceID, "nonce-drain-output-123")
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, w.sessionID, workerReadyAckPayload).WriteTo(conn); err != nil {
		t.Fatalf("acknowledge ready: %v", err)
	}

	if _, err := protocol.NewMessage(protocol.MsgClose, w.sessionID, "").WriteTo(conn); err != nil {
		t.Fatalf("send close: %v", err)
	}
	select {
	case err := <-runDone:
		runExited = true
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgClose")
	}
}

func TestWorkerDoesNotProcessInputBeforeReadyAcknowledgment(t *testing.T) {
	daemonConn, workerConn := net.Pipe()
	blockingConn := newBlockingWriteConn(workerConn)
	cli := newReadyBarrierTestAdapter()
	w := New(Options{
		SessionID:  "worker-ready-input-barrier",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	w.cliAdapter = cli
	w.dial = func(string, string) (net.Conn, error) {
		return blockingConn, nil
	}

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run() }()
	runExited := false
	t.Cleanup(func() {
		blockingConn.releaseWrite()
		w.Cancel()
		_ = cli.Close()
		_ = daemonConn.Close()
		if !runExited {
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Error("worker did not exit during cleanup")
			}
		}
	})

	waitForSignal(t, blockingConn.writeStarted, "ready write to start")

	select {
	case <-w.readyCh:
		t.Fatal("ready barrier opened before ready send completed")
	default:
	}
	select {
	case <-cli.sendStarted:
		t.Fatal("adapter Send started before ready send completed")
	default:
	}

	readyMsg := make(chan *protocol.Message, 1)
	readyErr := make(chan error, 1)
	go func() {
		msg, err := protocol.DecodeMessage(daemonConn)
		if err != nil {
			readyErr <- err
			return
		}
		readyMsg <- msg
	}()
	blockingConn.releaseWrite()

	select {
	case err := <-readyErr:
		t.Fatalf("read ready message: %v", err)
	case msg := <-readyMsg:
		if msg.Type != protocol.MsgReady {
			t.Fatalf("first worker message = %s, want %s", msg.Type, protocol.MsgReady)
		}
	case <-time.After(time.Second):
		t.Fatal("ready send did not complete")
	}
	select {
	case <-w.readyCh:
		t.Fatal("ready barrier opened before ready acknowledgment")
	default:
	}
	select {
	case <-cli.sendStarted:
		t.Fatal("adapter Send started before ready acknowledgment")
	default:
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, w.sessionID, "worker_ready").WriteTo(daemonConn); err != nil {
		t.Fatalf("acknowledge ready: %v", err)
	}
	inputWrite := make(chan error, 1)
	go func() {
		_, err := protocol.NewMessage(protocol.MsgUserInput, w.sessionID, "hello").WriteTo(daemonConn)
		inputWrite <- err
	}()
	select {
	case err := <-inputWrite:
		if err != nil {
			t.Fatalf("send user input: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("user input was not read after ready acknowledgment")
	}
	waitForSignal(t, cli.sendStarted, "adapter Send after ready acknowledgment")

	if _, err := protocol.NewMessage(protocol.MsgClose, w.sessionID, "").WriteTo(daemonConn); err != nil {
		t.Fatalf("send close: %v", err)
	}
	select {
	case err := <-runDone:
		runExited = true
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after MsgClose")
	}
}

func TestWorkerReconnectPublishesOnlyAfterReadyAck(t *testing.T) {
	oldDaemonConn, oldWorkerConn := net.Pipe()
	newDaemonConn, newWorkerConn := net.Pipe()
	blockingConn := newBlockingWriteConn(newWorkerConn)
	w := New(Options{
		SessionID:        "worker-reconnect-ready",
		WorkerInstanceID: "nonce-reconnect-123",
		DaemonAddr:       "daemon-test",
		CliType:          "mock",
	})
	w.conn = oldWorkerConn
	w.msgReader = protocol.NewMessageReader(oldWorkerConn)
	oldReader := w.msgReader
	w.dial = func(string, string) (net.Conn, error) {
		return blockingConn, nil
	}
	w.reconnectSleep = func(time.Duration) {}
	t.Cleanup(func() {
		blockingConn.releaseWrite()
		w.Cancel()
		_ = oldDaemonConn.Close()
		_ = newDaemonConn.Close()
	})

	oldConnClosed := make(chan struct{})
	go func() {
		var buf [1]byte
		_, _ = oldDaemonConn.Read(buf[:])
		close(oldConnClosed)
	}()

	reconnectDone := make(chan struct{})
	go func() {
		w.reconnectToDaemon()
		close(reconnectDone)
	}()
	waitForSignal(t, blockingConn.writeStarted, "reconnect ready write to start")

	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != oldWorkerConn {
		t.Fatal("reconnect published the new connection before READY completed")
	}
	if w.isConnected() {
		t.Fatal("reconnect marked worker connected before READY completed")
	}

	readyMsg := make(chan *protocol.Message, 1)
	readyErr := make(chan error, 1)
	go func() {
		msg, err := protocol.DecodeMessage(newDaemonConn)
		if err != nil {
			readyErr <- err
			return
		}
		readyMsg <- msg
	}()
	blockingConn.releaseWrite()

	select {
	case err := <-readyErr:
		t.Fatalf("read reconnect ready message: %v", err)
	case msg := <-readyMsg:
		if msg.Type != protocol.MsgReady {
			t.Fatalf("first reconnect message = %s, want %s", msg.Type, protocol.MsgReady)
		}
		if msg.WorkerInstanceID != "nonce-reconnect-123" {
			t.Fatalf("reconnect worker instance ID = %q, want %q", msg.WorkerInstanceID, "nonce-reconnect-123")
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect ready send did not complete")
	}

	w.connMu.Lock()
	publishedConn = w.conn
	publishedReader := w.msgReader
	w.connMu.Unlock()
	if publishedConn != oldWorkerConn {
		t.Fatal("reconnect published the new connection before ready acknowledgment")
	}
	if publishedReader != oldReader {
		t.Fatal("reconnect published a new message reader before ready acknowledgment")
	}
	if w.isConnected() {
		t.Fatal("reconnect marked worker connected before ready acknowledgment")
	}

	if _, err := protocol.NewMessage(protocol.MsgAck, w.sessionID, "worker_ready").WriteTo(newDaemonConn); err != nil {
		t.Fatalf("acknowledge reconnect ready: %v", err)
	}
	waitForSignal(t, reconnectDone, "reconnect after ready acknowledgment")
	waitForSignal(t, oldConnClosed, "old connection to close after reconnect acknowledgment")
	w.connMu.Lock()
	publishedConn = w.conn
	publishedReader = w.msgReader
	w.connMu.Unlock()
	if publishedConn != blockingConn {
		t.Fatal("reconnect did not publish the acknowledged connection")
	}
	if publishedReader == nil || publishedReader == oldReader {
		t.Fatal("reconnect did not publish the acknowledged message reader")
	}
	if !w.isConnected() {
		t.Fatal("reconnect did not mark worker connected after ready acknowledgment")
	}

	writes := blockingConn.writes()
	if len(writes) == 0 {
		t.Fatal("reconnect did not write to the new connection")
	}
	first, err := protocol.DecodeMessage(bytes.NewReader(writes[0]))
	if err != nil {
		t.Fatalf("decode first reconnect write: %v", err)
	}
	if first.Type != protocol.MsgReady {
		t.Fatalf("first reconnect write = %s, want %s", first.Type, protocol.MsgReady)
	}
}

func TestWorkerReconnectDoesNotPublishRejectedReady(t *testing.T) {
	oldDaemonConn, oldWorkerConn := net.Pipe()
	newDaemonConn, newWorkerConn := net.Pipe()
	w := New(Options{
		SessionID:        "worker-reconnect-rejected",
		WorkerInstanceID: "nonce-rejected-123",
		DaemonAddr:       "daemon-test",
		CliType:          "mock",
	})
	w.conn = oldWorkerConn
	w.msgReader = protocol.NewMessageReader(oldWorkerConn)
	w.reconnectSleep = func(time.Duration) {}

	var dialCalls atomic.Int32
	w.dial = func(string, string) (net.Conn, error) {
		if dialCalls.Add(1) != 1 {
			t.Fatal("reconnect dialed again after permanent ready rejection")
		}
		return newWorkerConn, nil
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = oldDaemonConn.Close()
		_ = newDaemonConn.Close()
	})

	readyRead := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(newDaemonConn).Read()
		if err != nil {
			close(readyRead)
			serverDone <- err
			return
		}
		close(readyRead)
		if msg.Type != protocol.MsgReady {
			serverDone <- fmt.Errorf("first message = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		_, err = protocol.NewMessage(protocol.MsgError, w.sessionID, "worker instance mismatch").WriteTo(newDaemonConn)
		serverDone <- err
	}()

	reconnectDone := make(chan struct{})
	go func() {
		w.reconnectToDaemon()
		close(reconnectDone)
	}()
	waitForSignal(t, readyRead, "rejected reconnect ready")
	waitForSignal(t, reconnectDone, "reconnect exit after ready rejection")

	w.connMu.Lock()
	publishedConn := w.conn
	publishedReader := w.msgReader
	w.connMu.Unlock()
	if publishedConn == newWorkerConn {
		t.Fatal("reconnect published the rejected connection")
	}
	if publishedReader != nil {
		t.Fatal("reconnect retained a message reader after permanent ready rejection")
	}
	if w.isConnected() {
		t.Fatal("reconnect marked worker connected after permanent ready rejection")
	}
	if !w.isClosed() {
		t.Fatal("worker did not cancel after permanent ready rejection")
	}
	if got := dialCalls.Load(); got != 1 {
		t.Fatalf("dial calls = %d, want 1", got)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("send ready rejection: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish sending ready rejection")
	}
}

func TestWorkerReconnectRejectionCleansUpAdapter(t *testing.T) {
	oldDaemonConn, oldWorkerConn := net.Pipe()
	newDaemonConn, newWorkerConn := net.Pipe()
	t.Cleanup(func() {
		_ = oldDaemonConn.Close()
		_ = newDaemonConn.Close()
	})

	storeDir := t.TempDir()
	sessionID := "worker-reconnect-cleanup"
	sessionPath := filepath.Join(storeDir, sessionID+".json")
	if err := os.WriteFile(sessionPath, []byte(`{"session_id":"worker-reconnect-cleanup","closed":false}`), 0o644); err != nil {
		t.Fatalf("write persisted session: %v", err)
	}

	cli := newCleanupTrackingTestAdapter(nil)
	input := &closeTrackingWriteCloser{}
	output := &closeTrackingReadCloser{}
	w := New(Options{
		SessionID:        sessionID,
		WorkerInstanceID: "nonce-reconnect-cleanup",
		DaemonAddr:       "daemon-test",
		StoreDir:         storeDir,
		CliType:          "mock",
	})
	w.cliAdapter = cli
	w.startResult = &adapter.CliStartResult{
		Input:  input,
		Output: output,
		ErrCh:  make(chan error),
	}
	w.conn = oldWorkerConn
	w.msgReader = protocol.NewMessageReader(oldWorkerConn)
	w.reconnectSleep = func(time.Duration) {}

	var dialCalls atomic.Int32
	w.dial = func(string, string) (net.Conn, error) {
		if dialCalls.Add(1) != 1 {
			return nil, errors.New("unexpected reconnect dial")
		}
		return newWorkerConn, nil
	}

	serverDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(newDaemonConn).Read()
		if err != nil {
			serverDone <- fmt.Errorf("read ready: %w", err)
			return
		}
		if msg.Type != protocol.MsgReady {
			serverDone <- fmt.Errorf("first message = %s, want %s", msg.Type, protocol.MsgReady)
			return
		}
		_, err = protocol.NewMessage(protocol.MsgError, w.sessionID, "worker instance mismatch").WriteTo(newDaemonConn)
		serverDone <- err
	}()

	reconnectDone := make(chan struct{})
	go func() {
		w.reconnectToDaemon()
		close(reconnectDone)
	}()
	waitForSignal(t, reconnectDone, "reconnect cleanup after ready rejection")
	waitForSignal(t, cli.closed, "adapter Close after reconnect rejection")

	if got := cli.closeCalls.Load(); got != 1 {
		t.Fatalf("adapter Close calls = %d, want 1", got)
	}
	if got := input.closeCalls.Load(); got != 1 {
		t.Fatalf("started input Close calls = %d, want 1", got)
	}
	if got := output.closeCalls.Load(); got != 1 {
		t.Fatalf("started output Close calls = %d, want 1", got)
	}
	if w.ctx.Err() == nil {
		t.Fatal("worker context was not canceled after reconnect rejection")
	}
	if !w.isClosed() {
		t.Fatal("worker was not closed after reconnect rejection")
	}
	if got := dialCalls.Load(); got != 1 {
		t.Fatalf("dial calls = %d, want 1", got)
	}
	data, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatalf("read persisted session: %v", err)
	}
	if !bytes.Contains(data, []byte(`"closed":false`)) {
		t.Fatalf("reconnect rejection marked persisted session closed: %s", data)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("send ready rejection: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish sending ready rejection")
	}
}

func TestWorkerReconnectIsSingleFlight(t *testing.T) {
	daemonConn, workerConn := net.Pipe()
	firstDialEntered := make(chan struct{})
	releaseFirstDial := make(chan struct{})
	duplicateDialEntered := make(chan struct{}, 4)
	duplicateReturned := make(chan struct{}, 4)
	ownerDone := make(chan struct{})
	var dialCalls atomic.Int32
	var reconnectWG sync.WaitGroup
	var releaseOnce sync.Once
	releaseDial := func() {
		releaseOnce.Do(func() { close(releaseFirstDial) })
	}

	w := New(Options{
		SessionID:        "worker-reconnect-single-flight",
		WorkerInstanceID: "nonce-single-flight-123",
		DaemonAddr:       "daemon-test",
		CliType:          "mock",
	})
	w.reconnectSleep = func(time.Duration) {}
	w.dial = func(string, string) (net.Conn, error) {
		if dialCalls.Add(1) == 1 {
			close(firstDialEntered)
			<-releaseFirstDial
			return workerConn, nil
		}
		duplicateDialEntered <- struct{}{}
		<-w.ctx.Done()
		return nil, w.ctx.Err()
	}
	t.Cleanup(func() {
		w.Cancel()
		_ = daemonConn.Close()
		releaseDial()
		_ = workerConn.Close()

		reconnectsDone := make(chan struct{})
		go func() {
			reconnectWG.Wait()
			close(reconnectsDone)
		}()
		select {
		case <-reconnectsDone:
		case <-time.After(time.Second):
			t.Error("reconnect goroutines did not exit during cleanup")
		}
	})

	reconnectWG.Add(1)
	go func() {
		defer reconnectWG.Done()
		defer close(ownerDone)
		w.reconnectToDaemon()
	}()
	waitForSignal(t, firstDialEntered, "first reconnect dial to start")

	for range 4 {
		reconnectWG.Add(1)
		go func() {
			defer reconnectWG.Done()
			w.reconnectToDaemon()
			duplicateReturned <- struct{}{}
		}()
	}
	for range 4 {
		select {
		case <-duplicateReturned:
		case <-duplicateDialEntered:
			t.Fatal("duplicate reconnect attempted to dial while the first reconnect was in progress")
		case <-time.After(time.Second):
			t.Fatal("duplicate reconnect did not return while the first reconnect was in progress")
		}
	}
	if got := dialCalls.Load(); got != 1 {
		t.Fatalf("dial calls while first reconnect was blocked = %d, want 1", got)
	}

	readyMessage := make(chan *protocol.Message, 1)
	readyErr := make(chan error, 1)
	go func() {
		msg, err := protocol.DecodeMessage(daemonConn)
		if err != nil {
			readyErr <- err
			return
		}
		readyMessage <- msg
	}()
	releaseDial()

	select {
	case err := <-readyErr:
		t.Fatalf("read reconnect ready message: %v", err)
	case msg := <-readyMessage:
		if msg.Type != protocol.MsgReady {
			t.Fatalf("reconnect message type = %s, want %s", msg.Type, protocol.MsgReady)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect did not send READY")
	}
	if _, err := protocol.NewMessage(protocol.MsgAck, w.sessionID, "worker_ready").WriteTo(daemonConn); err != nil {
		t.Fatalf("acknowledge reconnect ready: %v", err)
	}
	waitForSignal(t, ownerDone, "first reconnect to finish")

	if got := dialCalls.Load(); got != 1 {
		t.Fatalf("dial calls = %d, want 1", got)
	}
	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != workerConn {
		t.Fatal("reconnect did not publish the sole ready connection")
	}
}

func TestWorkerReconnectPublishesConnectionDespiteBlockedOldWrite(t *testing.T) {
	oldDaemonConn, oldWorkerConn := net.Pipe()
	blockedOldConn := newCloseBlockingWriteConn(oldWorkerConn)
	newDaemonConn, newWorkerConn := net.Pipe()
	w := New(Options{
		SessionID:        "worker-reconnect-blocked-write",
		WorkerInstanceID: "nonce-blocked-write-123",
		DaemonAddr:       "daemon-test",
		CliType:          "mock",
	})
	w.conn = blockedOldConn
	w.msgReader = protocol.NewMessageReader(blockedOldConn)
	w.dial = func(string, string) (net.Conn, error) {
		return newWorkerConn, nil
	}
	w.reconnectSleep = func(time.Duration) {}

	sendDone := make(chan error, 1)
	senderExited := make(chan struct{})
	reconnectDone := make(chan struct{})
	t.Cleanup(func() {
		_ = blockedOldConn.Close()
		w.Cancel()
		_ = oldDaemonConn.Close()
		_ = newDaemonConn.Close()
		select {
		case <-senderExited:
		case <-time.After(time.Second):
			t.Error("blocked send did not exit during cleanup")
		}
	})

	go func() {
		defer close(senderExited)
		sendDone <- w.sendMessage(protocol.MsgOutput, "blocked")
	}()
	waitForSignal(t, blockedOldConn.writeStarted, "old write to start")

	readyMessage := make(chan *protocol.Message, 1)
	serverDone := make(chan error, 1)
	go func() {
		msg, err := protocol.NewMessageReader(newDaemonConn).Read()
		if err != nil {
			serverDone <- err
			return
		}
		readyMessage <- msg
		_, err = protocol.NewMessage(protocol.MsgAck, w.sessionID, "worker_ready").WriteTo(newDaemonConn)
		serverDone <- err
	}()

	go func() {
		w.reconnectToDaemon()
		close(reconnectDone)
	}()

	select {
	case msg := <-readyMessage:
		if msg.Type != protocol.MsgReady {
			t.Fatalf("reconnect message type = %s, want %s", msg.Type, protocol.MsgReady)
		}
		if msg.WorkerInstanceID != "nonce-blocked-write-123" {
			t.Fatalf("reconnect worker instance ID = %q, want %q", msg.WorkerInstanceID, "nonce-blocked-write-123")
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect did not send READY")
	}
	waitForSignal(t, reconnectDone, "reconnect to publish an acknowledged connection")
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("acknowledge reconnect ready: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon did not acknowledge reconnect ready")
	}

	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != newWorkerConn {
		t.Fatal("reconnect did not publish the replacement connection")
	}

	select {
	case err := <-sendDone:
		if err == nil {
			t.Fatal("old write unexpectedly succeeded after the stale connection closed")
		}
	case <-time.After(time.Second):
		t.Fatal("closing the stale connection did not unblock the old write")
	}
}

func TestWorkerHeartbeatDoesNotReconnectAfterStaleWriteFailure(t *testing.T) {
	oldDaemonConn, oldWorkerConn := net.Pipe()
	staleConn := newCloseBlockingWriteConn(oldWorkerConn)
	newConn := &recordingConn{}
	ticks := make(chan time.Time, 1)
	w := New(Options{
		SessionID:  "worker-stale-heartbeat",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	w.conn = staleConn
	w.msgReader = protocol.NewMessageReader(staleConn)
	w.heartbeatTicks = ticks

	var reconnectCalls atomic.Int32
	reconnectStarted := make(chan struct{}, 1)
	w.reconnectSleep = func(time.Duration) {
		reconnectCalls.Add(1)
		select {
		case reconnectStarted <- struct{}{}:
		default:
		}
		<-w.ctx.Done()
	}
	var dialCalls atomic.Int32
	w.dial = func(string, string) (net.Conn, error) {
		dialCalls.Add(1)
		return nil, errors.New("unexpected reconnect dial")
	}

	heartbeatsDone := make(chan struct{})
	t.Cleanup(func() {
		w.Cancel()
		_ = staleConn.Close()
		_ = oldDaemonConn.Close()
		select {
		case <-heartbeatsDone:
		case <-time.After(time.Second):
			t.Error("heartbeat goroutine did not exit")
		}
	})

	w.wg.Add(1)
	go func() {
		w.sendHeartbeats()
		close(heartbeatsDone)
	}()

	ticks <- time.Now()
	waitForSignal(t, staleConn.writeStarted, "stale heartbeat write to start")

	oldConn := w.publishConnection(newConn, protocol.NewMessageReader(newConn))
	if oldConn != staleConn {
		t.Fatal("heartbeat did not write to the stale connection")
	}
	if err := oldConn.Close(); err != nil {
		t.Fatalf("close stale connection: %v", err)
	}

	select {
	case <-reconnectStarted:
		t.Fatal("heartbeat started reconnect after stale write failure")
	case <-time.After(100 * time.Millisecond):
	}
	if got := reconnectCalls.Load(); got != 0 {
		t.Fatalf("reconnect calls = %d, want 0", got)
	}
	if got := dialCalls.Load(); got != 0 {
		t.Fatalf("dial calls = %d, want 0", got)
	}
	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != newConn {
		t.Fatal("stale heartbeat failure replaced the newly published connection")
	}
	if !w.isConnected() {
		t.Fatal("stale heartbeat failure marked the newly connected worker disconnected")
	}
}

func TestWorkerIgnoresStaleDaemonReaderFailureAfterReconnect(t *testing.T) {
	oldConn := newBlockingReadConn(errors.New("old reader failed"))
	newConn := newBlockingReadConn(net.ErrClosed)
	w := New(Options{
		SessionID:  "worker-stale-daemon-reader",
		DaemonAddr: "daemon-test",
		CliType:    "mock",
	})
	w.conn = oldConn
	w.msgReader = protocol.NewMessageReader(oldConn)
	w.setConnected(true)

	var reconnectCalls atomic.Int32
	reconnectStarted := make(chan struct{}, 1)
	w.reconnectSleep = func(time.Duration) {
		reconnectCalls.Add(1)
		reconnectStarted <- struct{}{}
		<-w.ctx.Done()
	}
	var dialCalls atomic.Int32
	dialStarted := make(chan struct{}, 1)
	w.dial = func(string, string) (net.Conn, error) {
		dialCalls.Add(1)
		dialStarted <- struct{}{}
		return nil, errors.New("unexpected reconnect dial")
	}

	daemonReadDone := make(chan struct{})
	t.Cleanup(func() {
		w.Cancel()
		select {
		case <-daemonReadDone:
		case <-time.After(time.Second):
			t.Error("daemon reader did not exit during cleanup")
		}
	})

	w.wg.Add(1)
	go func() {
		w.readDaemonMessages()
		close(daemonReadDone)
	}()
	waitForSignal(t, oldConn.readStarted, "old daemon reader to start")

	oldPublishedConn := w.publishConnection(newConn, protocol.NewMessageReader(newConn))
	if oldPublishedConn != oldConn {
		t.Fatal("replacement connection did not replace the old connection")
	}
	oldConn.releaseRead()

	select {
	case <-newConn.readStarted:
	case <-reconnectStarted:
		t.Fatal("stale daemon reader error started reconnect")
	case <-dialStarted:
		t.Fatal("stale daemon reader error dialed reconnect")
	}
	if got := reconnectCalls.Load(); got != 0 {
		t.Fatalf("reconnect calls = %d, want 0", got)
	}
	if got := dialCalls.Load(); got != 0 {
		t.Fatalf("dial calls = %d, want 0", got)
	}
	if !w.isConnected() {
		t.Fatal("stale daemon reader error marked the replacement connection disconnected")
	}
	w.connMu.Lock()
	publishedConn := w.conn
	w.connMu.Unlock()
	if publishedConn != newConn {
		t.Fatal("stale daemon reader error replaced the newly published connection")
	}
}

type delayedReadyTestAdapter struct {
	ready   <-chan error
	outputR *io.PipeReader
	outputW *io.PipeWriter
}

type initialHandshakeRejectTestAdapter struct {
	ready      <-chan error
	outputR    *io.PipeReader
	outputW    *io.PipeWriter
	closed     chan struct{}
	closeCalls atomic.Int32
	closeOnce  sync.Once
}

type cleanupTrackingTestAdapter struct {
	startErr   error
	closed     chan struct{}
	closeCalls atomic.Int32
	closeOnce  sync.Once
}

func newCleanupTrackingTestAdapter(startErr error) *cleanupTrackingTestAdapter {
	return &cleanupTrackingTestAdapter{
		startErr: startErr,
		closed:   make(chan struct{}),
	}
}

func (a *cleanupTrackingTestAdapter) Name() string {
	return "cleanup-tracking-test"
}

func (a *cleanupTrackingTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	if a.startErr != nil {
		return nil, a.startErr
	}
	return nil, errors.New("Start must not be called")
}

func (a *cleanupTrackingTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *cleanupTrackingTestAdapter) Close() error {
	a.closeCalls.Add(1)
	a.closeOnce.Do(func() { close(a.closed) })
	return nil
}

type closeTrackingReadCloser struct {
	closeCalls atomic.Int32
}

func (r *closeTrackingReadCloser) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (r *closeTrackingReadCloser) Close() error {
	r.closeCalls.Add(1)
	return nil
}

type closeTrackingWriteCloser struct {
	closeCalls atomic.Int32
}

func (w *closeTrackingWriteCloser) Write(data []byte) (int, error) {
	return len(data), nil
}

func (w *closeTrackingWriteCloser) Close() error {
	w.closeCalls.Add(1)
	return nil
}

func newInitialHandshakeRejectTestAdapter() *initialHandshakeRejectTestAdapter {
	ready := make(chan error, 1)
	ready <- nil
	outputR, outputW := io.Pipe()
	return &initialHandshakeRejectTestAdapter{
		ready:   ready,
		outputR: outputR,
		outputW: outputW,
		closed:  make(chan struct{}),
	}
}

func (a *initialHandshakeRejectTestAdapter) Name() string {
	return "initial-handshake-reject-test"
}

func (a *initialHandshakeRejectTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	return &adapter.CliStartResult{
		Output:      a.outputR,
		ErrCh:       make(chan error),
		ReadyResult: a.ready,
	}, nil
}

func (a *initialHandshakeRejectTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *initialHandshakeRejectTestAdapter) Close() error {
	a.closeCalls.Add(1)
	a.closeOnce.Do(func() {
		close(a.closed)
		_ = a.outputR.Close()
		_ = a.outputW.Close()
	})
	return nil
}

func newDelayedReadyTestAdapter(ready <-chan error) *delayedReadyTestAdapter {
	outputR, outputW := io.Pipe()
	return &delayedReadyTestAdapter{
		ready:   ready,
		outputR: outputR,
		outputW: outputW,
	}
}

func (a *delayedReadyTestAdapter) Name() string {
	return "delayed-ready-test"
}

func (a *delayedReadyTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	return &adapter.CliStartResult{
		Output:      a.outputR,
		ErrCh:       make(chan error),
		ReadyResult: a.ready,
	}, nil
}

func (a *delayedReadyTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *delayedReadyTestAdapter) Close() error {
	_ = a.outputR.Close()
	return a.outputW.Close()
}

type readyOutputTestAdapter struct {
	ready     <-chan error
	events    chan adapter.AdapterEvent
	eventSent chan struct{}
	stop      chan struct{}
	outputR   *io.PipeReader
	outputW   *io.PipeWriter
}

func newReadyOutputTestAdapter(ready <-chan error) *readyOutputTestAdapter {
	outputR, outputW := io.Pipe()
	return &readyOutputTestAdapter{
		ready:     ready,
		events:    make(chan adapter.AdapterEvent),
		eventSent: make(chan struct{}),
		stop:      make(chan struct{}),
		outputR:   outputR,
		outputW:   outputW,
	}
}

func (a *readyOutputTestAdapter) Name() string {
	return "ready-output-test"
}

func (a *readyOutputTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	go func() {
		select {
		case a.events <- adapter.AdapterEvent{Kind: adapter.AdapterOutput, Output: "ready-gated output"}:
			close(a.eventSent)
		case <-a.stop:
		}
	}()
	return &adapter.CliStartResult{
		Output:           a.outputR,
		ErrCh:            make(chan error),
		ReadyResult:      a.ready,
		Events:           a.events,
		StructuredOutput: true,
	}, nil
}

func (a *readyOutputTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *readyOutputTestAdapter) Close() error {
	select {
	case <-a.stop:
	default:
		close(a.stop)
	}
	_ = a.outputR.Close()
	return a.outputW.Close()
}

type startupOutputReadyTestAdapter struct {
	ready                 chan error
	startupWritesComplete chan struct{}
	closed                chan struct{}
	outputR               *io.PipeReader
	outputW               *io.PipeWriter
	closeOnce             sync.Once
}

func newStartupOutputReadyTestAdapter() *startupOutputReadyTestAdapter {
	outputR, outputW := io.Pipe()
	return &startupOutputReadyTestAdapter{
		ready:                 make(chan error),
		startupWritesComplete: make(chan struct{}),
		closed:                make(chan struct{}),
		outputR:               outputR,
		outputW:               outputW,
	}
}

func (a *startupOutputReadyTestAdapter) Name() string {
	return "startup-output-ready-test"
}

func (a *startupOutputReadyTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	go func() {
		for _, output := range []string{"startup output\n", "composer-ready signal\n"} {
			if _, err := io.WriteString(a.outputW, output); err != nil {
				return
			}
		}
		close(a.startupWritesComplete)
		select {
		case a.ready <- nil:
		case <-a.closed:
		}
	}()
	return &adapter.CliStartResult{
		Output:      a.outputR,
		ErrCh:       make(chan error),
		ReadyResult: a.ready,
	}, nil
}

func (a *startupOutputReadyTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	return adapter.SendResult{}, nil
}

func (a *startupOutputReadyTestAdapter) Close() error {
	a.closeOnce.Do(func() {
		close(a.closed)
		_ = a.outputR.Close()
		_ = a.outputW.Close()
	})
	return nil
}

type readyBarrierTestAdapter struct {
	sendStarted chan struct{}
	outputR     *io.PipeReader
	outputW     *io.PipeWriter
}

func newReadyBarrierTestAdapter() *readyBarrierTestAdapter {
	outputR, outputW := io.Pipe()
	return &readyBarrierTestAdapter{
		sendStarted: make(chan struct{}),
		outputR:     outputR,
		outputW:     outputW,
	}
}

func (a *readyBarrierTestAdapter) Name() string {
	return "ready-barrier-test"
}

func (a *readyBarrierTestAdapter) Start(context.Context, string) (*adapter.CliStartResult, error) {
	return &adapter.CliStartResult{
		Output: a.outputR,
		ErrCh:  make(chan error),
	}, nil
}

func (a *readyBarrierTestAdapter) Send(context.Context, string) (adapter.SendResult, error) {
	close(a.sendStarted)
	return adapter.SendResult{}, nil
}

func (a *readyBarrierTestAdapter) Close() error {
	_ = a.outputR.Close()
	return a.outputW.Close()
}

type blockingWriteConn struct {
	net.Conn

	writeStarted chan struct{}
	release      chan struct{}

	startOnce    sync.Once
	releaseOnce  sync.Once
	writesMu     sync.Mutex
	writeRecords [][]byte
}

func newBlockingWriteConn(conn net.Conn) *blockingWriteConn {
	return &blockingWriteConn{
		Conn:         conn,
		writeStarted: make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (c *blockingWriteConn) Write(data []byte) (int, error) {
	copied := append([]byte(nil), data...)
	c.writesMu.Lock()
	c.writeRecords = append(c.writeRecords, copied)
	c.writesMu.Unlock()
	c.startOnce.Do(func() { close(c.writeStarted) })
	<-c.release
	return c.Conn.Write(data)
}

func (c *blockingWriteConn) releaseWrite() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *blockingWriteConn) writes() [][]byte {
	c.writesMu.Lock()
	defer c.writesMu.Unlock()

	writes := make([][]byte, len(c.writeRecords))
	for i, data := range c.writeRecords {
		writes[i] = append([]byte(nil), data...)
	}
	return writes
}

type closeBlockingWriteConn struct {
	net.Conn

	writeStarted chan struct{}
	closed       chan struct{}
	startOnce    sync.Once
	closeOnce    sync.Once
}

func newCloseBlockingWriteConn(conn net.Conn) *closeBlockingWriteConn {
	return &closeBlockingWriteConn{
		Conn:         conn,
		writeStarted: make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

func (c *closeBlockingWriteConn) Write([]byte) (int, error) {
	c.startOnce.Do(func() { close(c.writeStarted) })
	<-c.closed
	return 0, net.ErrClosed
}

func (c *closeBlockingWriteConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type blockingReadConn struct {
	readStarted chan struct{}
	release     chan struct{}
	closed      chan struct{}
	err         error
	readOnce    sync.Once
	releaseOnce sync.Once
	closeOnce   sync.Once
}

func newBlockingReadConn(err error) *blockingReadConn {
	return &blockingReadConn{
		readStarted: make(chan struct{}),
		release:     make(chan struct{}),
		closed:      make(chan struct{}),
		err:         err,
	}
}

func (c *blockingReadConn) Read([]byte) (int, error) {
	c.readOnce.Do(func() { close(c.readStarted) })
	select {
	case <-c.release:
		return 0, c.err
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *blockingReadConn) Write(data []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
		return len(data), nil
	}
}

func (c *blockingReadConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *blockingReadConn) LocalAddr() net.Addr {
	return testAddr("local")
}

func (c *blockingReadConn) RemoteAddr() net.Addr {
	return testAddr("remote")
}

func (c *blockingReadConn) SetDeadline(time.Time) error {
	return nil
}

func (c *blockingReadConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *blockingReadConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *blockingReadConn) releaseRead() {
	c.releaseOnce.Do(func() { close(c.release) })
}

type recordingConn struct {
	writesMu sync.Mutex
	writesV  [][]byte
	closed   bool
}

func (c *recordingConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *recordingConn) Write(data []byte) (int, error) {
	c.writesMu.Lock()
	defer c.writesMu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	c.writesV = append(c.writesV, append([]byte(nil), data...))
	return len(data), nil
}

func (c *recordingConn) Close() error {
	c.writesMu.Lock()
	c.closed = true
	c.writesMu.Unlock()
	return nil
}

func (c *recordingConn) LocalAddr() net.Addr {
	return testAddr("local")
}

func (c *recordingConn) RemoteAddr() net.Addr {
	return testAddr("remote")
}

func (c *recordingConn) SetDeadline(time.Time) error {
	return nil
}

func (c *recordingConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *recordingConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *recordingConn) writes() [][]byte {
	c.writesMu.Lock()
	defer c.writesMu.Unlock()

	writes := make([][]byte, len(c.writesV))
	for i, data := range c.writesV {
		writes[i] = append([]byte(nil), data...)
	}
	return writes
}

type deadlineRecordingConn struct {
	net.Conn

	deadlinesMu sync.Mutex
	deadlinesV  []time.Time
	closed      chan struct{}
	closeOnce   sync.Once
}

func newDeadlineRecordingConn(conn net.Conn) *deadlineRecordingConn {
	return &deadlineRecordingConn{
		Conn:   conn,
		closed: make(chan struct{}),
	}
}

func (c *deadlineRecordingConn) SetDeadline(deadline time.Time) error {
	if err := c.Conn.SetDeadline(deadline); err != nil {
		return err
	}
	c.deadlinesMu.Lock()
	c.deadlinesV = append(c.deadlinesV, deadline)
	c.deadlinesMu.Unlock()
	return nil
}

func (c *deadlineRecordingConn) deadlines() []time.Time {
	c.deadlinesMu.Lock()
	defer c.deadlinesMu.Unlock()
	return append([]time.Time(nil), c.deadlinesV...)
}

func (c *deadlineRecordingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type testAddr string

func (a testAddr) Network() string {
	return "test"
}

func (a testAddr) String() string {
	return string(a)
}
