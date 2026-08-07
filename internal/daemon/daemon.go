package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"botmux-go/internal/config"
	"botmux-go/internal/protocol"
)

type Daemon struct {
	cfg      *config.DaemonConfig
	listener net.Listener
	selfExe  string
	store    *SessionStore

	ctx    context.Context
	cancel context.CancelFunc

	sessionsMu sync.RWMutex
	sessions   map[string]*SessionMeta

	workersMu sync.RWMutex
	workers   map[string]*WorkerHandle

	connMapMu  sync.Mutex
	connToSess map[net.Conn]string

	closed  bool
	closeMu sync.Mutex

	wg sync.WaitGroup

	monitorMu    sync.Mutex
	monitorStop  chan struct{}
	spawnFailures map[string]*spawnFailure
	spawnSem     chan struct{}
}

func New(cfg *config.DaemonConfig) (*Daemon, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get self exe: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Daemon{
		cfg:        cfg,
		selfExe:    exe,
		store:      NewSessionStore(cfg.SessionsDir),
		ctx:        ctx,
		cancel:     cancel,
		sessions:   make(map[string]*SessionMeta),
		workers:    make(map[string]*WorkerHandle),
		connToSess: make(map[net.Conn]string),
	}, nil
}

func (d *Daemon) Start() error {
	if err := d.cfg.EnsureDirs(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", d.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", d.cfg.ListenAddr, err)
	}
	d.listener = ln
	log.Printf("[daemon] listening on %s (self=%s, sessions_dir=%s)", d.cfg.ListenAddr, d.selfExe, d.cfg.SessionsDir)

	d.restoreSessions()
	d.startSessionMonitor()

	d.wg.Add(2)
	go d.acceptLoop()
	go d.periodicGC()
	return nil
}

func (d *Daemon) Wait() {
	d.wg.Wait()
}

func (d *Daemon) Stop() error {
	d.closeMu.Lock()
	if d.closed {
		d.closeMu.Unlock()
		return nil
	}
	d.closed = true
	d.closeMu.Unlock()
	d.cancel()
	d.stopSessionMonitor()
	if d.listener != nil {
		_ = d.listener.Close()
	}
	d.sessionsMu.RLock()
	ids := make([]string, 0, len(d.sessions))
	for id := range d.sessions {
		ids = append(ids, id)
	}
	d.sessionsMu.RUnlock()
	for _, id := range ids {
		d.CloseSession(id, "daemon shutdown")
	}
	return nil
}

type NewSessionOpts struct {
	SessionID  string
	BotID      string
	CliType    string
	CliPath    string
	WorkingDir string
	OnReady    func(meta *SessionMeta)
}

func (d *Daemon) NewSession(opts NewSessionOpts) (*SessionMeta, error) {
	if d.isClosed() {
		return nil, errors.New("daemon closed")
	}
	if opts.SessionID == "" {
		return nil, errors.New("session_id required")
	}
	bot, ok := d.cfg.FindBot(opts.BotID)
	if !ok {
		if len(d.cfg.Bots) == 0 {
			return nil, fmt.Errorf("no bots configured")
		}
		bot = &d.cfg.Bots[0]
	}
	cliType := opts.CliType
	if cliType == "" {
		cliType = string(bot.CliType)
	}
	workingDir := opts.WorkingDir
	if workingDir == "" {
		workingDir = bot.WorkingDir
	}

	now := time.Now()
	meta := &SessionMeta{
		SessionID:  opts.SessionID,
		BotID:      bot.BotID,
		CliType:    cliType,
		CliPath:    opts.CliPath,
		WorkingDir: workingDir,
		LastOutput: []string{},
		CreatedAt:  now,
		Status:     StatusCreated,
		lastActive: now,
	}

	d.sessionsMu.Lock()
	if _, exists := d.sessions[opts.SessionID]; exists {
		d.sessionsMu.Unlock()
		return nil, fmt.Errorf("session %s already exists", opts.SessionID)
	}
	d.sessions[opts.SessionID] = meta
	d.sessionsMu.Unlock()

	ps := meta.ToPersisted()
	if err := d.store.save(ps); err != nil {
		log.Printf("[daemon] warn: persist session %s failed: %v", safeShort(opts.SessionID), err)
	}

	if err := d.spawnWorkerForSession(meta); err != nil {
		return nil, err
	}

	if opts.OnReady != nil {
		d.workersMu.RLock()
		h := d.workers[meta.SessionID]
		d.workersMu.RUnlock()
		if h != nil {
			go func() {
				select {
				case <-h.Ready:
					opts.OnReady(meta)
				case <-time.After(10 * time.Second):
					log.Printf("[daemon] session %s: worker ready timeout", safeShort(opts.SessionID))
				case <-d.ctx.Done():
				}
			}()
		}
	}
	return meta, nil
}

func (d *Daemon) spawnWorkerForSession(meta *SessionMeta) error {
	handle := NewWorkerHandle(meta.SessionID)

	cmd := exec.CommandContext(d.ctx, d.selfExe)
	cmd.Env = append(os.Environ(),
		"BOTMUX_ROLE=worker",
		"BOTMUX_SESSION_ID="+meta.SessionID,
		"BOTMUX_DAEMON_ADDR="+d.cfg.ListenAddr,
		"BOTMUX_CLI_TYPE="+meta.CliType,
		"BOTMUX_CLI_PATH="+meta.CliPath,
		"BOTMUX_WORKING_DIR="+meta.WorkingDir,
		"BOTMUX_STORE_DIR="+d.cfg.SessionsDir,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	handle.Cmd = cmd

	d.sessionsMu.Lock()
	meta.Status = StatusSpawning
	d.sessionsMu.Unlock()

	if err := cmd.Start(); err != nil {
		d.workersMu.Lock()
		delete(d.workers, meta.SessionID)
		d.workersMu.Unlock()
		d.sessionsMu.Lock()
		meta.Status = StatusRecovering
		d.sessionsMu.Unlock()
		return fmt.Errorf("spawn worker: %w", err)
	}
	handle.Pid = cmd.Process.Pid

	d.workersMu.Lock()
	d.workers[meta.SessionID] = handle
	d.workersMu.Unlock()

	_ = d.store.UpdateWorkerPID(meta.SessionID, handle.Pid)

	go d.waitWorkerExit(handle)
	go d.monitorReady(handle, meta)

	log.Printf("[daemon] session %s spawned worker (cli=%s, pid=%d)", safeShort(meta.SessionID), meta.CliType, handle.Pid)
	return nil
}

func (d *Daemon) monitorReady(handle *WorkerHandle, meta *SessionMeta) {
	select {
	case <-handle.Ready:
		d.sessionsMu.Lock()
		if !meta.Closed {
			meta.Status = StatusReady
		}
		d.sessionsMu.Unlock()
		log.Printf("[daemon] session %s -> READY", safeShort(meta.SessionID))
	case <-time.After(10 * time.Second):
		d.sessionsMu.Lock()
		if !meta.Closed && meta.Status != StatusReady {
			meta.Status = StatusRecovering
		}
		d.sessionsMu.Unlock()
		log.Printf("[daemon] session %s: worker ready timeout, status=RECOVERING", safeShort(meta.SessionID))
	case <-d.ctx.Done():
	}
}

func (d *Daemon) restoreSessions() {
	stored, err := d.store.list()
	if err != nil || len(stored) == 0 {
		if err != nil {
			log.Printf("[daemon] restore sessions: %v", err)
		}
		return
	}
	log.Printf("[daemon] restoring %d persisted session(s)", len(stored))
	for _, ps := range stored {
		meta := SessionMetaFromPersisted(ps)
		d.sessionsMu.Lock()
		d.sessions[ps.SessionID] = meta
		d.sessionsMu.Unlock()
		log.Printf("[daemon] restored session %s (bot=%s, status=%s, last_active=%s)",
			safeShort(ps.SessionID), ps.BotID, meta.Status, meta.LastActive().Format("15:04:05"))
	}
}

func (d *Daemon) waitWorkerExit(handle *WorkerHandle) {
	err := handle.Cmd.Wait()
	exitMsg := fmt.Sprintf("worker exit (pid=%d):", handle.Pid)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("[daemon] session %s %s err=%v", safeShort(handle.SessionID), exitMsg, err)
		}
	}

	d.workersMu.Lock()
	h, exists := d.workers[handle.SessionID]
	if exists && h == handle {
		delete(d.workers, handle.SessionID)
	}
	d.workersMu.Unlock()

	d.sessionsMu.Lock()
	meta, ok := d.sessions[handle.SessionID]
	if ok && !meta.Closed {
		meta.Status = StatusRecovering
	}
	d.sessionsMu.Unlock()

	handle.CloseConn()

	if ok {
		for _, fn := range meta.DrainOnClose() {
			func() {
				defer func() { _ = recover() }()
				fn()
			}()
		}
	}
}

func (d *Daemon) removeSession(id string) {
	d.workersMu.Lock()
	h, hasHandle := d.workers[id]
	if hasHandle {
		delete(d.workers, id)
	}
	d.workersMu.Unlock()

	d.sessionsMu.Lock()
	meta, hasMeta := d.sessions[id]
	if hasMeta {
		meta.Closed = true
		meta.Status = StatusClosed
		delete(d.sessions, id)
	}
	d.sessionsMu.Unlock()

	if !hasMeta {
		return
	}

	_ = d.store.MarkClosed(id)

	if hasHandle && h != nil {
		h.CloseConn()
	}

	for _, fn := range meta.DrainOnClose() {
		func() {
			defer func() { _ = recover() }()
			fn()
		}()
	}
}

func (d *Daemon) CloseSession(id, reason string) {
	d.sessionsMu.RLock()
	_, ok := d.sessions[id]
	d.sessionsMu.RUnlock()
	if !ok {
		return
	}

	d.workersMu.RLock()
	h := d.workers[id]
	d.workersMu.RUnlock()

	if h != nil {
		msg := protocol.NewMessage(protocol.MsgClose, id, reason)
		_ = h.Send(msg)
		timeout := time.AfterFunc(2*time.Second, func() {
			if h.Cmd != nil && h.Cmd.Process != nil {
				_ = h.Cmd.Process.Kill()
			}
		})
		defer timeout.Stop()
		if h.Cmd != nil && h.Cmd.Process != nil {
			_ = h.Cmd.Wait()
		}
	}
	d.removeSession(id)
	log.Printf("[daemon] session %s closed (reason=%s)", safeShort(id), reason)
}

func (d *Daemon) SendInput(id, input string) error {
	d.sessionsMu.RLock()
	meta, ok := d.sessions[id]
	d.sessionsMu.RUnlock()
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	if meta.Closed {
		return fmt.Errorf("session %s is closed", id)
	}

	d.workersMu.RLock()
	h := d.workers[id]
	d.workersMu.RUnlock()
	if h == nil {
		return fmt.Errorf("session %s has no worker (status=%s)", id, meta.Status)
	}

	select {
	case <-h.Ready:
	case <-time.After(15 * time.Second):
		return fmt.Errorf("session %s worker not connected (status=%s)", id, meta.Status)
	}

	msg := protocol.NewMessage(protocol.MsgUserInput, id, input)
	meta.touchActive()
	return h.Send(msg)
}

func (d *Daemon) Sessions() map[string]*SessionMeta {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	out := make(map[string]*SessionMeta, len(d.sessions))
	for k, v := range d.sessions {
		out[k] = v
	}
	return out
}

func (d *Daemon) GetSessionMeta(id string) (*SessionMeta, bool) {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	m, ok := d.sessions[id]
	return m, ok
}

func (d *Daemon) GetWorkerHandle(id string) (*WorkerHandle, bool) {
	d.workersMu.RLock()
	defer d.workersMu.RUnlock()
	h, ok := d.workers[id]
	return h, ok
}

func (d *Daemon) isClosed() bool {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	return d.closed
}

func (d *Daemon) acceptLoop() {
	defer d.wg.Done()
	defer d.Stop()
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			if d.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("[daemon] accept: %v", err)
			continue
		}
		d.wg.Add(1)
		go d.handleConn(conn)
	}
}

func (d *Daemon) handleConn(conn net.Conn) {
	defer d.wg.Done()
	defer conn.Close()

	reader := protocol.NewMessageReader(conn)
	first, err := reader.Read()
	if err != nil {
		log.Printf("[daemon] conn read first: %v", err)
		return
	}

	if isClientFirstMessage(first) {
		d.handleClientConn(conn, reader, first)
		return
	}

	sessionID := first.SessionID
	d.sessionsMu.RLock()
	meta, ok := d.sessions[sessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		log.Printf("[daemon] session %s not registered for incoming worker conn", safeShort(sessionID))
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "session not registered").WriteTo(conn)
		return
	}
	if meta.Closed {
		return
	}

	d.workersMu.Lock()
	h, hasH := d.workers[sessionID]
	if !hasH || h == nil {
		h = NewWorkerHandle(sessionID)
		d.workers[sessionID] = h
	}
	oldConn := h.SetConn(conn)
	d.workersMu.Unlock()
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}

	h.markReady()

	d.connMapMu.Lock()
	d.connToSess[conn] = sessionID
	d.connMapMu.Unlock()
	defer func() {
		d.connMapMu.Lock()
		delete(d.connToSess, conn)
		d.connMapMu.Unlock()
	}()

	if first.Type != protocol.MsgNewSession {
		d.routeMessage(first, meta, h)
	}
	for {
		msg, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("[daemon] worker session %s read: %v", safeShort(sessionID), err)
			}
			return
		}
		if msg.SessionID == "" {
			msg.SessionID = sessionID
		}
		d.routeMessage(msg, meta, h)
	}
}

func (d *Daemon) handleClientConn(conn net.Conn, reader *protocol.MessageReader, first *protocol.Message) {
	cliID := fmt.Sprintf("cli-%p", conn)
	defer func() {
		log.Printf("[daemon] client %s disconnected", cliID)
	}()
	if first.Type == protocol.MsgNewSession {
		sid := first.SessionID
		if sid == "" {
			sid = fmt.Sprintf("s-%d", time.Now().UnixNano())
		}
		botID := first.Payload
		log.Printf("[daemon] %s creating session %s (bot=%s)", cliID, safeShort(sid), botID)
		var readyCh <-chan struct{} = nil
		_, err := d.NewSession(NewSessionOpts{
			SessionID: sid,
			BotID:     botID,
		})
		if err == nil {
			d.workersMu.RLock()
			h := d.workers[sid]
			d.workersMu.RUnlock()
			if h != nil {
				readyCh = h.Ready
			}
		}
		ackPayload := "ok"
		if err != nil {
			ackPayload = "err:" + err.Error()
		}
		_, _ = protocol.NewMessage(protocol.MsgAck, sid, ackPayload).WriteTo(conn)
		if err != nil {
			log.Printf("[daemon] %s new session failed: %v", cliID, err)
			return
		}
		select {
		case <-readyCh:
			_, _ = protocol.NewMessage(protocol.MsgReady, sid, "").WriteTo(conn)
		case <-time.After(15 * time.Second):
			log.Printf("[daemon] %s session %s ready timeout", cliID, safeShort(sid))
			return
		case <-d.ctx.Done():
			return
		}
	} else if first != nil {
		sid := first.SessionID
		if sid != "" {
			d.forwardClientMessage(conn, first)
		}
	}
	for {
		msg, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("[daemon] client %s read: %v", cliID, err)
			}
			return
		}
		d.forwardClientMessage(conn, msg)
	}
}

func (d *Daemon) forwardClientMessage(clientConn net.Conn, msg *protocol.Message) {
	switch msg.Type {
	case protocol.MsgClose:
		d.CloseSession(msg.SessionID, "client close")
		return
	case protocol.MsgUserInput, protocol.MsgNewSession:
	default:
	}
	if msg.SessionID == "" {
		_, _ = protocol.NewMessage(protocol.MsgError, "", "missing session_id").WriteTo(clientConn)
		return
	}

	d.sessionsMu.RLock()
	meta, ok := d.sessions[msg.SessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "session not found").WriteTo(clientConn)
		return
	}
	if meta.Closed {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "session is closed").WriteTo(clientConn)
		return
	}

	d.workersMu.RLock()
	h := d.workers[msg.SessionID]
	d.workersMu.RUnlock()
	if h == nil {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "worker not available").WriteTo(clientConn)
		return
	}

	select {
	case <-h.Ready:
	case <-time.After(15 * time.Second):
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "worker not connected").WriteTo(clientConn)
		return
	}

	if msg.Type == protocol.MsgUserInput {
		inCopy := *msg
		if err := h.Send(&inCopy); err != nil {
			_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "forward: "+err.Error()).WriteTo(clientConn)
			return
		}
		meta.touchActive()
	}

	connMu := sync.Mutex{}
	stop := make(chan struct{})
	meta.AddOnClose(func() { closeOnce(stop) })

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		seen := 0
		deadline := time.After(8 * time.Second)
		for {
			select {
			case <-stop:
				return
			case <-deadline:
				return
			case <-ticker.C:
			}
			outs := meta.SnapshotOutput()
			if len(outs) <= seen {
				continue
			}
			for i := seen; i < len(outs); i++ {
				m := protocol.NewMessage(protocol.MsgOutput, meta.SessionID, outs[i])
				connMu.Lock()
				_, _ = m.WriteTo(clientConn)
				connMu.Unlock()
			}
			seen = len(outs)
		}
	}()
}

func isClientFirstMessage(m *protocol.Message) bool {
	if m == nil {
		return false
	}
	switch m.Type {
	case protocol.MsgNewSession,
		protocol.MsgUserInput,
		protocol.MsgClose,
		protocol.MsgAck:
		return true
	}
	return m.SessionID == ""
}

func safeShort(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

func closeOnce(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}

func isClosedChan(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (d *Daemon) routeMessage(msg *protocol.Message, meta *SessionMeta, h *WorkerHandle) {
	switch msg.Type {
	case protocol.MsgReady:
		log.Printf("[daemon] session %s -> READY", safeShort(meta.SessionID))
		meta.touchActive()
		_ = d.store.UpdateLastActive(meta.SessionID)
	case protocol.MsgHeartbeat:
		h.TouchHb()
	case protocol.MsgOutput:
		meta.AddOutput(msg.Payload)
		meta.touchActive()
		_ = d.store.UpdateOutput(meta.SessionID, msg.Payload)
		fmt.Printf("[session=%s] %s\n", safeShort(meta.SessionID), msg.Payload)
	case protocol.MsgError:
		log.Printf("[daemon] session %s error: %s", safeShort(meta.SessionID), msg.Payload)
	case protocol.MsgClose:
		log.Printf("[daemon] session %s closed by worker", safeShort(meta.SessionID))
		d.removeSession(meta.SessionID)
	default:
		log.Printf("[daemon] session %s unknown msg type=%s", safeShort(meta.SessionID), msg.Type)
	}
}

func (d *Daemon) periodicGC() {
	defer d.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.runGC()
		}
	}
}

func (d *Daemon) runGC() {
	now := time.Now()
	d.sessionsMu.RLock()
	metas := make([]*SessionMeta, 0, len(d.sessions))
	for _, m := range d.sessions {
		metas = append(metas, m)
	}
	d.sessionsMu.RUnlock()
	idleLimit := 15 * time.Minute
	hbLimit := 20 * time.Second
	for _, meta := range metas {
		if meta.Closed {
			continue
		}
		lastActive := meta.LastActive()
		d.workersMu.RLock()
		h, hasH := d.workers[meta.SessionID]
		var lastHb time.Time
		if hasH {
			lastHb = h.LastHb()
		}
		d.workersMu.RUnlock()

		if hasH && now.Sub(lastHb) > hbLimit && h.IsReady() {
			log.Printf("[daemon] session %s heartbeat stale (>%s), closing", safeShort(meta.SessionID), hbLimit)
			d.CloseSession(meta.SessionID, "heartbeat stale")
			continue
		}
		if now.Sub(lastActive) > idleLimit {
			log.Printf("[daemon] session %s idle (>%s), closing", safeShort(meta.SessionID), idleLimit)
			d.CloseSession(meta.SessionID, "idle")
		}
	}
}
