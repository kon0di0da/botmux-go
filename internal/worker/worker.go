package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/daemon"
	"botmux-go/internal/protocol"
)

const (
	cliOutputIdleAfter           = 15 * time.Second
	cliOutputStallAfter          = 60 * time.Second
	cliOutputIdleLogEvery        = 15 * time.Second
	cliOutputObserveFor          = 130 * time.Second
	cliOutputCheckInterval       = 5 * time.Second
	workerReadyAckPayload        = "worker_ready"
	defaultReadyHandshakeTimeout = 10 * time.Second
)

type workerHandshakeRejectedError struct {
	payload string
}

func (e *workerHandshakeRejectedError) Error() string {
	return "worker ready rejected: " + e.payload
}

type Worker struct {
	sessionID        string
	workerInstanceID string
	daemonAddr       string
	storeDir         string
	cliAdapter       adapter.CliAdapter
	workingDir       string
	cliType          string

	conn      net.Conn
	connMu    sync.Mutex
	sendMu    sync.Mutex
	msgReader *protocol.MessageReader

	reconnectMu  sync.Mutex
	reconnecting bool

	startResult *adapter.CliStartResult

	ctx                   context.Context
	cancel                context.CancelFunc
	heartbeatInterval     time.Duration
	heartbeatTicks        <-chan time.Time
	dial                  func(network, address string) (net.Conn, error)
	reconnectSleep        func(time.Duration)
	readyHandshakeTimeout time.Duration

	wg      sync.WaitGroup
	readyCh chan struct{}
	closed  bool
	closeMu sync.Mutex

	connected bool

	daemonDisconnectedCh chan struct{}

	deduper              *LineDeduper
	outputIdleObserver   *cliOutputIdleObserver
	turnMu               sync.Mutex
	turnInFlight         bool
	turnID               uint64
	turnCtx              context.Context
	turnCancel           context.CancelFunc
	turnCancelRequested  bool
	turnInterruptPending bool
}

type Options struct {
	SessionID        string
	WorkerInstanceID string
	DaemonAddr       string
	CliType          string
	CliPath          string
	Model            string
	CodexProfile     string
	ResumeSessionID  string
	WorkingDir       string
	StoreDir         string
}

func New(opts Options) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		sessionID:        opts.SessionID,
		workerInstanceID: opts.WorkerInstanceID,
		daemonAddr:       opts.DaemonAddr,
		storeDir:         opts.StoreDir,
		cliAdapter: adapter.Create(adapter.AdapterOptions{
			CliType: opts.CliType, CliPath: opts.CliPath, Model: opts.Model, Profile: opts.CodexProfile,
			ResumeSessionID: opts.ResumeSessionID,
		}),
		workingDir:            opts.WorkingDir,
		cliType:               opts.CliType,
		ctx:                   ctx,
		cancel:                cancel,
		heartbeatInterval:     5 * time.Second,
		dial:                  net.Dial,
		reconnectSleep:        time.Sleep,
		readyHandshakeTimeout: defaultReadyHandshakeTimeout,
		readyCh:               make(chan struct{}),
		daemonDisconnectedCh:  make(chan struct{}, 1),
		deduper:               NewLineDeduper(400),
		outputIdleObserver: newCLIOutputIdleObserver(
			cliOutputIdleAfter,
			cliOutputStallAfter,
			cliOutputIdleLogEvery,
			cliOutputObserveFor,
		),
	}
}

func (w *Worker) Run() error {
	if w.cliAdapter == nil {
		return fmt.Errorf("no adapter registered for cli_type")
	}
	conn, err := w.connectToDaemon()
	if err != nil {
		return err
	}

	if err := w.startCli(); err != nil {
		_ = conn.Close()
		w.cleanup(false)
		return fmt.Errorf("start cli: %w", err)
	}
	w.wg.Add(1)
	go w.readCliOutput()
	cleanupInitialFailure := true
	defer func() {
		if cleanupInitialFailure {
			_ = conn.Close()
			w.cleanup(false)
		}
	}()

	if err := w.waitForCLIReady(); err != nil {
		return err
	}

	reader, err := w.readyHandshake(conn)
	if err != nil {
		return err
	}
	oldConn, published := w.publishConnection(conn, reader)
	if !published {
		_ = conn.Close()
		if err := w.ctx.Err(); err != nil {
			return err
		}
		return errors.New("worker closed before publishing daemon connection")
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}
	log.Printf("[worker:%s] connected to daemon %s", safeShortID(w.sessionID), w.daemonAddr)
	close(w.readyCh)
	cleanupInitialFailure = false

	w.wg.Add(1)
	go w.readDaemonMessages()

	goroutines := 1
	if w.startResult.Events != nil {
		goroutines++
	}
	w.wg.Add(goroutines)
	if w.startResult.Events != nil {
		go w.readAdapterEvents()
	}
	go w.sendHeartbeats()

	w.wg.Wait()
	return nil
}

func (w *Worker) waitForCLIReady() error {
	if w.startResult == nil {
		return errors.New("cli start result missing")
	}
	if w.startResult.ReadyResult != nil {
		select {
		case err := <-w.startResult.ReadyResult:
			if err != nil {
				return fmt.Errorf("cli readiness failed: %w", err)
			}
			return nil
		case <-time.After(45 * time.Second):
			return errors.New("cli ready timeout")
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
	if w.startResult.ReadyDelay <= 0 {
		return nil
	}
	select {
	case <-time.After(w.startResult.ReadyDelay):
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	case err, ok := <-w.startResult.ErrCh:
		if ok && err != nil {
			return fmt.Errorf("cli exited during ready wait: %w", err)
		}
		return errors.New("cli exited during ready wait")
	}
}

func (w *Worker) connectToDaemon() (net.Conn, error) {
	var conn net.Conn
	var err error
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 20; attempt++ {
		conn, err = w.dial("tcp", w.daemonAddr)
		if err == nil {
			break
		}
		time.Sleep(backoff)
		backoff *= 2
		if backoff > 3*time.Second {
			backoff = 3 * time.Second
		}
	}
	if err != nil {
		return nil, fmt.Errorf("connect daemon %s: %w", w.daemonAddr, err)
	}
	return conn, nil
}

func (w *Worker) reconnectToDaemon() {
	w.reconnectMu.Lock()
	if w.reconnecting {
		w.reconnectMu.Unlock()
		return
	}
	w.reconnecting = true
	w.reconnectMu.Unlock()
	defer func() {
		w.reconnectMu.Lock()
		w.reconnecting = false
		w.reconnectMu.Unlock()
	}()

	log.Printf("[worker:%s] daemon disconnected, starting reconnection...", safeShortID(w.sessionID))
	for attempt := 0; attempt < 100; attempt++ {
		if w.isClosed() {
			return
		}
		backoff := time.Duration(min(1<<uint(attempt), 30)) * time.Second
		if attempt > 20 {
			backoff = 30 * time.Second
		} else {
			backoff = time.Duration(1<<uint(attempt)) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
		log.Printf("[worker:%s] reconnect attempt %d (backoff=%v)...", safeShortID(w.sessionID), attempt+1, backoff)
		w.reconnectSleep(backoff)

		conn, err := w.dial("tcp", w.daemonAddr)
		if err != nil {
			continue
		}
		reader, err := w.readyHandshake(conn)
		if err != nil {
			_ = conn.Close()
			var rejected *workerHandshakeRejectedError
			if errors.As(err, &rejected) {
				log.Printf("[worker:%s] reconnect rejected: %s", safeShortID(w.sessionID), rejected.payload)
				w.cleanup(false)
				return
			}
			log.Printf("[worker:%s] reconnect ready: %v", safeShortID(w.sessionID), err)
			continue
		}
		oldConn, published := w.publishConnection(conn, reader)
		if !published {
			_ = conn.Close()
			return
		}
		if oldConn != nil {
			_ = oldConn.Close()
		}

		log.Printf("[worker:%s] reconnected to daemon (attempt %d)", safeShortID(w.sessionID), attempt+1)
		return
	}
	log.Printf("[worker:%s] failed to reconnect after 100 attempts, giving up", safeShortID(w.sessionID))
}

func (w *Worker) publishConnection(conn net.Conn, reader *protocol.MessageReader) (net.Conn, bool) {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	if w.closed || w.ctx.Err() != nil {
		return nil, false
	}
	w.connMu.Lock()
	defer w.connMu.Unlock()
	oldConn := w.conn
	w.conn = conn
	w.msgReader = reader
	w.connected = true
	return oldConn, true
}

func (w *Worker) setConnected(v bool) {
	w.connMu.Lock()
	w.connected = v
	w.connMu.Unlock()
}

func (w *Worker) isConnected() bool {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	return w.connected
}

func (w *Worker) startCli() error {
	result, err := w.cliAdapter.Start(w.ctx, w.workingDir)
	if err != nil {
		return err
	}
	w.startResult = result
	log.Printf("[worker:%s] cli adapter started (adapter=%s, dir=%s)",
		safeShortID(w.sessionID), w.cliAdapter.Name(), w.workingDir)
	return nil
}

func (w *Worker) sendReady() error {
	w.connMu.Lock()
	conn := w.conn
	w.connMu.Unlock()
	return w.sendReadyTo(conn)
}

func (w *Worker) sendReadyTo(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection closed")
	}
	m := protocol.NewMessage(protocol.MsgReady, w.sessionID, "ready")
	m.WorkerInstanceID = w.workerInstanceID
	_, err := m.WriteTo(conn)
	return err
}

func (w *Worker) readyHandshakeTimeoutOrDefault() time.Duration {
	if w.readyHandshakeTimeout <= 0 {
		return defaultReadyHandshakeTimeout
	}
	return w.readyHandshakeTimeout
}

func isTransientReadyHandshakeError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func (w *Worker) readyHandshake(conn net.Conn) (*protocol.MessageReader, error) {
	if conn == nil {
		return nil, errors.New("connection closed")
	}

	done := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-w.ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer func() {
		close(done)
		<-watcherDone
	}()

	handshakeSucceeded := false
	defer func() {
		if handshakeSucceeded {
			_ = conn.SetDeadline(time.Time{})
		}
	}()

	if err := conn.SetDeadline(time.Now().Add(w.readyHandshakeTimeoutOrDefault())); err != nil {
		return nil, fmt.Errorf("set ready handshake deadline: %w", err)
	}
	if err := w.sendReadyTo(conn); err != nil {
		return nil, fmt.Errorf("write ready message: %w", err)
	}
	reader := protocol.NewMessageReader(conn)
	msg, err := reader.Read()
	if err != nil {
		if isTransientReadyHandshakeError(err) {
			return nil, fmt.Errorf("read ready acknowledgment: %w", err)
		}
		return nil, &workerHandshakeRejectedError{
			payload: fmt.Sprintf("invalid ready acknowledgment: %v", err),
		}
	}
	if msg.Type == protocol.MsgError {
		return nil, &workerHandshakeRejectedError{payload: msg.Payload}
	}
	if msg.Type != protocol.MsgAck || msg.SessionID != w.sessionID || msg.Payload != workerReadyAckPayload {
		return nil, &workerHandshakeRejectedError{payload: fmt.Sprintf(
			"unexpected ready acknowledgment: type=%s session=%q payload=%q",
			msg.Type, msg.SessionID, msg.Payload,
		)}
	}
	handshakeSucceeded = true
	return reader, nil
}

func (w *Worker) sendError(msg string) {
	_ = w.sendMessage(protocol.MsgError, msg)
}

func (w *Worker) sendMessage(typ protocol.MessageType, payload string) error {
	_, err := w.sendMessageWithConn(typ, payload)
	return err
}

func (w *Worker) sendMessageWithConn(typ protocol.MessageType, payload string) (net.Conn, error) {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	w.connMu.Lock()
	conn := w.conn
	w.connMu.Unlock()
	if conn == nil {
		return nil, errors.New("connection closed")
	}
	m := protocol.NewMessage(typ, w.sessionID, payload)
	m.WorkerInstanceID = w.workerInstanceID
	_, err := m.WriteTo(conn)
	return conn, err
}

func (w *Worker) markDisconnectedIfCurrent(conn net.Conn, reader *protocol.MessageReader) bool {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	if conn == nil || w.conn != conn || (reader != nil && w.msgReader != reader) {
		return false
	}
	w.connected = false
	return true
}

func (w *Worker) readDaemonMessages() {
	defer w.wg.Done()
	defer w.Cancel()
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}
		w.connMu.Lock()
		conn := w.conn
		reader := w.msgReader
		w.connMu.Unlock()
		if reader == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		msg, err := reader.Read()
		if err != nil {
			if w.ctx.Err() != nil {
				return
			}
			if !w.markDisconnectedIfCurrent(conn, reader) {
				continue
			}
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("[worker:%s] read daemon error: %v", safeShortID(w.sessionID), err)
			}
			go w.reconnectToDaemon()
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		switch msg.Type {
		case protocol.MsgUserInput, protocol.MsgNewSession:
			select {
			case <-w.readyCh:
			case <-w.ctx.Done():
				return
			}
			w.deduper.Reset()
			turnID, ok := w.beginTurnWithID()
			if !ok {
				w.sendError("codex_turn_in_progress")
				continue
			}
			w.outputIdleObserver.BeginInput(time.Now())
			w.wg.Add(1)
			go w.handleInput(turnID, msg.Payload)
		case protocol.MsgCancelTurn:
			if turnID, ok := w.requestTurnInterrupt(); ok {
				w.wg.Add(1)
				go w.interruptTurn(turnID)
			}
		case protocol.MsgRestartWorker:
			log.Printf("[worker:%s] restart requested by daemon", safeShortID(w.sessionID))
			w.cleanup(false)
			return
		case protocol.MsgClose:
			log.Printf("[worker:%s] close requested by daemon", safeShortID(w.sessionID))
			w.cleanup(true)
			return
		case protocol.MsgHeartbeat:
		case protocol.MsgAck:
		default:
			log.Printf("[worker:%s] unknown msg type: %s", safeShortID(w.sessionID), msg.Type)
		}
	}
}

func (w *Worker) handleInput(turnID uint64, input string) {
	defer w.wg.Done()

	turnCtx, ok := w.turnContextForID(turnID)
	if !ok {
		return
	}
	log.Printf("[worker:%s] sending to cli: %q", safeShortID(w.sessionID), input)
	result, err := w.cliAdapter.Send(turnCtx, input)
	if err != nil {
		if w.ctx.Err() != nil || turnCtx.Err() != nil || w.isTurnCancellationRequested(turnID) {
			return
		}
		log.Printf("[worker:%s] send to cli: %v", safeShortID(w.sessionID), err)
		w.failCurrentTurn(turnID, "codex_submit_failed", err)
		return
	}
	if w.ctx.Err() != nil || turnCtx.Err() != nil || result.CliSessionID == "" {
		return
	}
	if err := w.sendMessage(protocol.MsgCliSessionBound, result.CliSessionID); err != nil {
		log.Printf("[worker:%s] persist CLI session ID: %v", safeShortID(w.sessionID), err)
	}
}

func (w *Worker) readCliOutput() {
	defer w.wg.Done()
	defer w.Cancel()
	reader := bufio.NewReaderSize(w.startResult.Output, 64*1024)
	var rawCount, emitCount int
	lastLog := time.Now()
	idleTicker := time.NewTicker(cliOutputCheckInterval)
	defer idleTicker.Stop()

	readCh := make(chan readResult, 1)
	go func() {
		line, err := reader.ReadString('\n')
		readCh <- readResult{data: line, err: err}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case err, ok := <-w.startResult.ErrCh:
			if ok && err != nil {
				log.Printf("[worker:%s] cli error: %v (raw=%d emitted=%d)", safeShortID(w.sessionID), err, rawCount, emitCount)
			} else {
				log.Printf("[worker:%s] cli exited (raw=%d emitted=%d)", safeShortID(w.sessionID), rawCount, emitCount)
			}
			return
		case now := <-idleTicker.C:
			if diag, ok := w.outputIdleObserver.Diagnostic(now); ok {
				log.Printf(
					"[worker:%s] cli output idle: state=%s idle=%s since_input=%s output_seen=%t raw=%d emitted=%d pty_read_pending=true",
					safeShortID(w.sessionID),
					diag.State,
					diag.IdleFor.Round(time.Second),
					diag.SinceInput.Round(time.Second),
					diag.OutputSeen,
					rawCount,
					emitCount,
				)
			}
		case r := <-readCh:
			if r.err != nil {
				if !errors.Is(r.err, io.EOF) {
					log.Printf("[worker:%s] cli read: %v (raw=%d emitted=%d)", safeShortID(w.sessionID), r.err, rawCount, emitCount)
				} else {
					log.Printf("[worker:%s] cli EOF (raw=%d emitted=%d)", safeShortID(w.sessionID), rawCount, emitCount)
				}
				return
			}
			if observer, ok := w.cliAdapter.(adapter.CliOutputObserver); ok {
				observer.NotifyOutput()
			}
			w.outputIdleObserver.MarkOutput(time.Now())
			rawCount++
			rawLine := strings.TrimRight(r.data, "\r\n")
			clean := rawLine
			if strings.ContainsRune(clean, '\r') {
				if idx := strings.LastIndexByte(clean, '\r'); idx >= 0 {
					clean = clean[idx+1:]
				}
			}
			clean = strings.TrimRight(clean, "\r")
			clean = stripAnsi(clean)
			clean = strings.TrimSpace(clean)
			if clean != "" && !w.startResult.StructuredOutput {
				select {
				case <-w.readyCh:
					if out := w.deduper.Check(clean); out != "" {
						emitCount++
						if err := w.sendMessage(protocol.MsgOutput, out); err != nil {
							log.Printf("[worker:%s] send output: %v", safeShortID(w.sessionID), err)
							time.Sleep(500 * time.Millisecond)
						} else if time.Since(lastLog) > 5*time.Second || emitCount <= 5 {
							preview := out
							if len(preview) > 120 {
								preview = preview[:120] + "..."
							}
							log.Printf("[worker:%s] >> %s", safeShortID(w.sessionID), preview)
							lastLog = time.Now()
						}
					}
				default:
				}
			}
			readCh = make(chan readResult, 1)
			go func() {
				line, err := reader.ReadString('\n')
				readCh <- readResult{data: line, err: err}
			}()
		}
	}
}

func (w *Worker) readAdapterEvents() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case event, ok := <-w.startResult.Events:
			if !ok {
				return
			}
			if w.cliType == "codex" {
				if event.TurnID == 0 {
					continue
				}
				if _, ok := w.turnContextForID(event.TurnID); !ok {
					continue
				}
			}
			switch event.Kind {
			case adapter.AdapterOutput:
				if event.Output == "" {
					continue
				}
				w.outputIdleObserver.MarkOutput(time.Now())
				if err := w.sendMessage(protocol.MsgOutput, event.Output); err != nil {
					log.Printf("[worker:%s] send structured output: %v", safeShortID(w.sessionID), err)
				}
			case adapter.AdapterTurnTerminal:
				turnID := uint64(0)
				if w.cliType == "codex" {
					turnID = event.TurnID
				}
				w.completeTurn(turnID, protocol.TurnTerminal{
					Status:      protocol.TurnStatus(event.Status),
					ErrorCode:   event.ErrorCode,
					ErrorDetail: event.ErrorDetail,
				})
			}
		}
	}
}

func (w *Worker) sendTurnTerminal(terminal protocol.TurnTerminal) {
	payload, err := json.Marshal(terminal)
	if err != nil {
		w.sendError("encode Codex terminal: " + err.Error())
		return
	}
	if err := w.sendMessage(protocol.MsgTurnCompleted, string(payload)); err != nil {
		log.Printf("[worker:%s] send turn terminal: %v", safeShortID(w.sessionID), err)
	}
}

func (w *Worker) beginTurn() bool {
	_, ok := w.beginTurnWithID()
	return ok
}

func (w *Worker) beginTurnWithID() (uint64, bool) {
	if w.cliType != "codex" {
		return 0, true
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	if w.turnInFlight || w.turnInterruptPending {
		return 0, false
	}
	w.turnInFlight = true
	w.turnID++
	w.turnCtx, w.turnCancel = context.WithCancel(adapter.WithTurnID(w.ctx, w.turnID))
	w.turnCancelRequested = false
	return w.turnID, true
}

func (w *Worker) turnContextForID(turnID uint64) (context.Context, bool) {
	if w.cliType != "codex" {
		return w.ctx, true
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	if !w.turnInFlight || w.turnID != turnID || w.turnCtx == nil {
		return nil, false
	}
	return w.turnCtx, true
}

func (w *Worker) requestTurnInterrupt() (uint64, bool) {
	if w.cliType != "codex" {
		return 0, false
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	if !w.turnInFlight || w.turnCancelRequested {
		return 0, false
	}
	w.turnCancelRequested = true
	if w.turnCancel != nil {
		w.turnCancel()
	}
	w.turnInterruptPending = true
	return w.turnID, true
}

func (w *Worker) isTurnCancellationRequested(turnID uint64) bool {
	if w.cliType != "codex" {
		return false
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	return w.turnInFlight && w.turnID == turnID && w.turnCancelRequested
}

func (w *Worker) shouldInterruptTurn(turnID uint64) bool {
	if w.cliType != "codex" {
		return false
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	return w.turnInFlight &&
		w.turnID == turnID &&
		w.turnCancelRequested &&
		w.turnInterruptPending
}

func (w *Worker) finishTurnInterrupt(turnID uint64) {
	if w.cliType != "codex" {
		return
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	if w.turnID == turnID {
		w.turnInterruptPending = false
	}
}

func (w *Worker) finishTurn() {
	w.finishTurnWithID(0)
}

func (w *Worker) finishTurnWithID(turnID uint64) bool {
	if w.cliType != "codex" {
		return true
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	if !w.turnInFlight || (turnID != 0 && turnID != w.turnID) {
		return false
	}
	w.turnInFlight = false
	w.cancelTurnLocked()
	w.turnCancelRequested = false
	return true
}

func (w *Worker) cancelTurnLocked() {
	if w.turnCancel != nil {
		w.turnCancel()
		w.turnCancel = nil
	}
	w.turnCtx = nil
}

func (w *Worker) cancelActiveTurn() {
	if w.cliType != "codex" {
		return
	}
	w.turnMu.Lock()
	w.cancelTurnLocked()
	w.turnMu.Unlock()
}

func (w *Worker) interruptTurn(turnID uint64) {
	defer w.wg.Done()
	defer w.finishTurnInterrupt(turnID)

	if !w.shouldInterruptTurn(turnID) {
		return
	}

	interrupter, ok := w.cliAdapter.(adapter.CliTurnInterrupter)
	if !ok {
		w.failCurrentTurn(turnID, "codex_cancel_failed", errors.New("adapter does not support turn interruption"))
		return
	}
	if err := interrupter.Interrupt(w.ctx); err != nil {
		if w.ctx.Err() != nil {
			return
		}
		log.Printf("[worker:%s] interrupt turn: %v", safeShortID(w.sessionID), err)
		w.failCurrentTurn(turnID, "codex_cancel_failed", err)
	}
}

func (w *Worker) completeTurn(turnID uint64, terminal protocol.TurnTerminal) bool {
	if !w.finishTurnWithID(turnID) {
		return false
	}
	w.outputIdleObserver.CancelInput()
	w.sendTurnTerminal(terminal)
	return true
}

func (w *Worker) failCurrentTurn(turnID uint64, code string, err error) {
	w.completeTurn(turnID, protocol.TurnTerminal{
		Status:      protocol.TurnFailed,
		ErrorCode:   code,
		ErrorDetail: err.Error(),
	})
}

type readResult struct {
	data string
	err  error
}

func (w *Worker) sendHeartbeats() {
	defer w.wg.Done()
	ticks := w.heartbeatTicks
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(w.heartbeatInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticks:
			conn, err := w.sendMessageWithConn(protocol.MsgHeartbeat, "")
			if err != nil && w.markDisconnectedIfCurrent(conn, nil) {
				go w.reconnectToDaemon()
			}
		}
	}
}

func (w *Worker) Cancel() {
	w.closeMu.Lock()
	if w.closed {
		w.closeMu.Unlock()
		return
	}
	w.closed = true
	w.closeMu.Unlock()
	w.cancelActiveTurn()
	w.cancel()
	w.connMu.Lock()
	conn := w.conn
	w.conn = nil
	w.msgReader = nil
	w.connected = false
	w.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (w *Worker) isClosed() bool {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	return w.closed
}

func (w *Worker) cleanup(markClosed bool) {
	w.closeMu.Lock()
	alreadyClosed := w.closed
	w.closed = true
	w.closeMu.Unlock()
	w.cancelActiveTurn()
	if !alreadyClosed {
		w.cancel()
	}
	if w.startResult != nil {
		if w.startResult.Input != nil {
			w.startResult.Input.Close()
		}
		if w.startResult.Output != nil {
			w.startResult.Output.Close()
		}
	}
	if w.cliAdapter != nil {
		_ = w.cliAdapter.Close()
	}
	w.connMu.Lock()
	conn := w.conn
	w.conn = nil
	w.msgReader = nil
	w.connected = false
	w.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if markClosed && w.storeDir != "" {
		store := daemon.NewSessionStore(w.storeDir)
		_ = store.MarkClosed(w.sessionID)
	}
	log.Printf("[worker:%s] worker exited", safeShortID(w.sessionID))
}

func GetEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func safeShortID(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}
