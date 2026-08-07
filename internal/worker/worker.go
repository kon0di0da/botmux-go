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
	"sync"
	"time"

	"botmux-go/internal/adapter"
	"botmux-go/internal/daemon"
	"botmux-go/internal/protocol"
)

type Worker struct {
	sessionID  string
	daemonAddr string
	storeDir   string
	cliAdapter adapter.CliAdapter
	workingDir string

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
}

type Options struct {
	SessionID  string
	DaemonAddr string
	CliType    string
	CliPath    string
	WorkingDir string
	StoreDir   string
}

func New(opts Options) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		sessionID:            opts.SessionID,
		daemonAddr:           opts.DaemonAddr,
		storeDir:             opts.StoreDir,
		cliAdapter:           adapter.Create(opts.CliType, opts.CliPath),
		workingDir:           opts.WorkingDir,
		ctx:                  ctx,
		cancel:               cancel,
		readyCh:              make(chan struct{}),
		daemonDisconnectedCh: make(chan struct{}, 1),
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
	close(w.readyCh)

	if err := w.sendReady(); err != nil {
		return err
	}

	w.wg.Add(3)
	go w.readDaemonMessages()
	go w.readCliOutput()
	go w.sendHeartbeats()

	w.wg.Wait()
	return nil
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
			if err := w.cliAdapter.Send(w.ctx, msg.Payload); err != nil {
				log.Printf("[worker:%s] send to cli: %v", safeShortID(w.sessionID), err)
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
	reader := bufio.NewReader(w.startResult.Output)
	var buf []byte
	type lineFrame struct {
		data     []byte
		isPrefix bool
		err      error
	}
	for {
		select {
		case <-w.ctx.Done():
			return
		case err, ok := <-w.startResult.ErrCh:
			if ok && err != nil {
				log.Printf("[worker:%s] cli error: %v", safeShortID(w.sessionID), err)
			}
			return
		default:
		}
		ch := make(chan lineFrame, 1)
		go func() {
			l, p, e := reader.ReadLine()
			cp := make([]byte, len(l))
			copy(cp, l)
			ch <- lineFrame{data: cp, isPrefix: p, err: e}
		}()
		var f lineFrame
		select {
		case <-w.ctx.Done():
			return
		case err, ok := <-w.startResult.ErrCh:
			if ok && err != nil {
				log.Printf("[worker:%s] cli error: %v", safeShortID(w.sessionID), err)
			}
			return
		case f = <-ch:
		}
		if f.err != nil {
			if !errors.Is(f.err, io.EOF) {
				log.Printf("[worker:%s] cli readline: %v", safeShortID(w.sessionID), f.err)
			}
			return
		}
		buf = append(buf, f.data...)
		if f.isPrefix {
			continue
		}
		text := string(buf)
		buf = buf[:0]
		if text == "" {
			continue
		}
		if err := w.sendMessage(protocol.MsgOutput, text); err != nil {
			log.Printf("[worker:%s] send output to daemon: %v", safeShortID(w.sessionID), err)
			time.Sleep(1 * time.Second)
			continue
		}
	}
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
