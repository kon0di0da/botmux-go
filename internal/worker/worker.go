package worker

import (
	"bufio"
	"context"
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
	cliOutputIdleAfter     = 15 * time.Second
	cliOutputStallAfter    = 60 * time.Second
	cliOutputIdleLogEvery  = 15 * time.Second
	cliOutputObserveFor    = 130 * time.Second
	cliOutputCheckInterval = 5 * time.Second
)

type Worker struct {
	sessionID  string
	daemonAddr string
	storeDir   string
	cliAdapter adapter.CliAdapter
	workingDir string
	cliType    string

	conn      net.Conn
	connMu    sync.Mutex
	msgReader *protocol.MessageReader

	startResult *adapter.CliStartResult

	ctx    context.Context
	cancel context.CancelFunc

	wg      sync.WaitGroup
	readyCh chan struct{}
	closed  bool
	closeMu sync.Mutex

	connectedMu sync.Mutex
	connected   bool

	daemonDisconnectedCh chan struct{}

	deduper            *LineDeduper
	outputIdleObserver *cliOutputIdleObserver
	turnMu             sync.Mutex
	turnInFlight       bool
}

type Options struct {
	SessionID       string
	DaemonAddr      string
	CliType         string
	CliPath         string
	Model           string
	CodexProfile    string
	ResumeSessionID string
	WorkingDir      string
	StoreDir        string
}

func New(opts Options) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		sessionID:  opts.SessionID,
		daemonAddr: opts.DaemonAddr,
		storeDir:   opts.StoreDir,
		cliAdapter: adapter.Create(adapter.AdapterOptions{
			CliType: opts.CliType, CliPath: opts.CliPath, Model: opts.Model, Profile: opts.CodexProfile,
			ResumeSessionID: opts.ResumeSessionID,
		}),
		workingDir:           opts.WorkingDir,
		cliType:              opts.CliType,
		ctx:                  ctx,
		cancel:               cancel,
		readyCh:              make(chan struct{}),
		daemonDisconnectedCh: make(chan struct{}, 1),
		deduper:              NewLineDeduper(400),
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
	if err := w.connectToDaemon(); err != nil {
		return err
	}

	if err := w.startCli(); err != nil {
		w.sendError("start_cli: " + err.Error())
		return err
	}

	goroutines := 3
	if w.startResult.Events != nil {
		goroutines++
	}
	w.wg.Add(goroutines)
	if w.startResult.Events != nil {
		go w.readAdapterEvents()
	}
	go w.readDaemonMessages()
	go w.readCliOutput()
	go w.sendHeartbeats()

	if err := w.waitForCLIReady(); err != nil {
		w.sendError("cli_ready: " + err.Error())
		return err
	}

	close(w.readyCh)

	if err := w.sendReady(); err != nil {
		return err
	}

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

func (w *Worker) connectToDaemon() error {
	var conn net.Conn
	var err error
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 20; attempt++ {
		conn, err = net.Dial("tcp", w.daemonAddr)
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
		return fmt.Errorf("connect daemon %s: %w", w.daemonAddr, err)
	}
	w.setConnected(true)
	w.conn = conn
	w.msgReader = protocol.NewMessageReader(conn)
	log.Printf("[worker:%s] connected to daemon %s", safeShortID(w.sessionID), w.daemonAddr)
	return nil
}

func (w *Worker) reconnectToDaemon() {
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
		time.Sleep(backoff)

		conn, err := net.Dial("tcp", w.daemonAddr)
		if err != nil {
			continue
		}
		w.setConnected(true)
		w.connMu.Lock()
		oldConn := w.conn
		w.conn = conn
		w.msgReader = protocol.NewMessageReader(conn)
		w.connMu.Unlock()
		if oldConn != nil {
			_ = oldConn.Close()
		}

		_ = w.sendReady()
		log.Printf("[worker:%s] reconnected to daemon (attempt %d)", safeShortID(w.sessionID), attempt+1)
		return
	}
	log.Printf("[worker:%s] failed to reconnect after 100 attempts, giving up", safeShortID(w.sessionID))
}

func (w *Worker) setConnected(v bool) {
	w.connectedMu.Lock()
	w.connected = v
	w.connectedMu.Unlock()
}

func (w *Worker) isConnected() bool {
	w.connectedMu.Lock()
	defer w.connectedMu.Unlock()
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
	return w.sendMessage(protocol.MsgReady, "ready")
}

func (w *Worker) sendError(msg string) {
	_ = w.sendMessage(protocol.MsgError, msg)
}

func (w *Worker) sendMessage(typ protocol.MessageType, payload string) error {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	if w.conn == nil {
		return errors.New("connection closed")
	}
	m := protocol.NewMessage(typ, w.sessionID, payload)
	_, err := m.WriteTo(w.conn)
	return err
}

func (w *Worker) readDaemonMessages() {
	defer w.wg.Done()
	defer w.Cancel()
	type frame struct {
		msg *protocol.Message
		err error
	}
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}
		w.connMu.Lock()
		reader := w.msgReader
		w.connMu.Unlock()
		if reader == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		ch := make(chan frame, 1)
		go func() {
			m, e := reader.Read()
			ch <- frame{msg: m, err: e}
		}()
		var f frame
		select {
		case <-w.ctx.Done():
			return
		case f = <-ch:
		}
		if f.err != nil {
			if !errors.Is(f.err, io.EOF) && !errors.Is(f.err, net.ErrClosed) {
				log.Printf("[worker:%s] read daemon error: %v", safeShortID(w.sessionID), f.err)
			}
			w.setConnected(false)
			go w.reconnectToDaemon()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		msg := f.msg
		switch msg.Type {
		case protocol.MsgUserInput, protocol.MsgNewSession:
			select {
			case <-w.readyCh:
			case <-w.ctx.Done():
				return
			}
			w.deduper.Reset()
			if !w.beginTurn() {
				w.sendError("codex_turn_in_progress")
				continue
			}
			log.Printf("[worker:%s] sending to cli: %q", safeShortID(w.sessionID), msg.Payload)
			w.outputIdleObserver.BeginInput(time.Now())
			result, err := w.cliAdapter.Send(w.ctx, msg.Payload)
			if err != nil {
				w.finishTurn()
				w.outputIdleObserver.CancelInput()
				log.Printf("[worker:%s] send to cli: %v", safeShortID(w.sessionID), err)
				continue
			}
			if result.CliSessionID != "" {
				if err := w.sendMessage(protocol.MsgSessionUpdate, result.CliSessionID); err != nil {
					log.Printf("[worker:%s] persist CLI session ID: %v", safeShortID(w.sessionID), err)
				}
			}
		case protocol.MsgClose:
			log.Printf("[worker:%s] close requested by daemon", safeShortID(w.sessionID))
			return
		case protocol.MsgHeartbeat:
		case protocol.MsgAck:
		default:
			log.Printf("[worker:%s] unknown msg type: %s", safeShortID(w.sessionID), msg.Type)
		}
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
				w.finishTurn()
				w.outputIdleObserver.CancelInput()
				if event.Status != adapter.TurnCompleted {
					detail := event.ErrorCode
					if event.ErrorDetail != "" {
						detail += ": " + event.ErrorDetail
					}
					w.sendError(detail)
				}
			}
		}
	}
}

func (w *Worker) beginTurn() bool {
	if w.cliType != "codex" {
		return true
	}
	w.turnMu.Lock()
	defer w.turnMu.Unlock()
	if w.turnInFlight {
		return false
	}
	w.turnInFlight = true
	return true
}

func (w *Worker) finishTurn() {
	if w.cliType != "codex" {
		return
	}
	w.turnMu.Lock()
	w.turnInFlight = false
	w.turnMu.Unlock()
}

type readResult struct {
	data string
	err  error
}

func (w *Worker) sendHeartbeats() {
	defer w.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.sendMessage(protocol.MsgHeartbeat, ""); err != nil {
				w.setConnected(false)
				go w.reconnectToDaemon()
			}
		}
	}
}

func (w *Worker) Cancel() {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	w.cancel()
}

func (w *Worker) isClosed() bool {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	return w.closed
}

func (w *Worker) cleanup() {
	w.closeMu.Lock()
	alreadyClosed := w.closed
	w.closed = true
	w.closeMu.Unlock()
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
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
	w.connMu.Unlock()
	if w.storeDir != "" {
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
