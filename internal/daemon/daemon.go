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

type WorkerSession struct {
	SessionID  string
	BotID      string
	CliType    string
	CliPath    string
	WorkingDir string
	Cmd        *exec.Cmd
	Conn       net.Conn
	Connected  chan struct{}
	LastOutput []string
	mu         sync.Mutex
	closed     bool
	lastActive time.Time
	hbSeen     time.Time
	onClose    []func()
}

func (ws *WorkerSession) touchActive() {
	ws.mu.Lock()
	ws.lastActive = time.Now()
	ws.mu.Unlock()
}

func (ws *WorkerSession) AddOutput(line string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.LastOutput = append(ws.LastOutput, line)
	if len(ws.LastOutput) > 50 {
		ws.LastOutput = ws.LastOutput[len(ws.LastOutput)-50:]
	}
}

func (ws *WorkerSession) SnapshotOutput() []string {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	out := make([]string, len(ws.LastOutput))
	copy(out, ws.LastOutput)
	return out
}

type Daemon struct {
	cfg       *config.DaemonConfig
	listener  net.Listener
	selfExe   string
	store     *SessionStore

	ctx    context.Context
	cancel context.CancelFunc

	sessionsMu sync.RWMutex
	sessions   map[string]*WorkerSession

	connMapMu  sync.Mutex
	connToSess map[net.Conn]string

	closed  bool
	closeMu sync.Mutex

	wg sync.WaitGroup
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
		sessions:   make(map[string]*WorkerSession),
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
	if d.listener != nil {
		_ = d.listener.Close()
	}
	d.sessionsMu.Lock()
	ids := make([]string, 0, len(d.sessions))
	for id := range d.sessions {
		ids = append(ids, id)
	}
	d.sessionsMu.Unlock()
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
	OnReady    func(ws *WorkerSession)
}

func (d *Daemon) NewSession(opts NewSessionOpts) (*WorkerSession, error) {
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

	ws := &WorkerSession{
		SessionID:  opts.SessionID,
		BotID:      bot.BotID,
		CliType:    cliType,
		CliPath:    opts.CliPath,
		WorkingDir: workingDir,
		Connected:  make(chan struct{}),
		lastActive: time.Now(),
		hbSeen:     time.Now(),
	}
	d.sessionsMu.Lock()
	if _, exists := d.sessions[opts.SessionID]; exists {
		d.sessionsMu.Unlock()
		return nil, fmt.Errorf("session %s already exists", opts.SessionID)
	}
	d.sessions[opts.SessionID] = ws
	d.sessionsMu.Unlock()

	ps := &PersistedSession{
		SessionID:  opts.SessionID,
		BotID:      bot.BotID,
		CliType:    cliType,
		CliPath:    opts.CliPath,
		WorkingDir: workingDir,
		LastOutput: []string{},
		LastActive: time.Now(),
		CreatedAt:  time.Now(),
	}
	if err := d.store.save(ps); err != nil {
		log.Printf("[daemon] warn: persist session %s failed: %v", safeShort(opts.SessionID), err)
	}

	cmd := exec.CommandContext(d.ctx, d.selfExe)
	env := append(os.Environ(),
		"BOTMUX_ROLE=worker",
		"BOTMUX_SESSION_ID="+opts.SessionID,
		"BOTMUX_DAEMON_ADDR="+d.cfg.ListenAddr,
		"BOTMUX_CLI_TYPE="+cliType,
		"BOTMUX_CLI_PATH="+opts.CliPath,
		"BOTMUX_WORKING_DIR="+workingDir,
		"BOTMUX_STORE_DIR="+d.cfg.SessionsDir,
	)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	ws.Cmd = cmd

	ws.mu.Lock()
	ws.onClose = append(ws.onClose, func() {
		if opts.OnReady != nil {
		}
	})
	ws.mu.Unlock()

	if err := cmd.Start(); err != nil {
		d.removeSession(opts.SessionID)
		return nil, fmt.Errorf("spawn worker: %w", err)
	}

	d.store.UpdateWorkerPID(opts.SessionID, cmd.Process.Pid)
	go d.waitWorkerExit(ws)

	if opts.OnReady != nil {
		go func() {
			select {
			case <-ws.Connected:
				opts.OnReady(ws)
			case <-time.After(10 * time.Second):
				log.Printf("[daemon] session %s: worker ready timeout", opts.SessionID[:8])
			case <-d.ctx.Done():
			}
		}()
	}

	log.Printf("[daemon] session %s spawned (cli=%s, pid=%d)", opts.SessionID[:8], cliType, cmd.Process.Pid)
	return ws, nil
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
		ws := &WorkerSession{
			SessionID:  ps.SessionID,
			BotID:      ps.BotID,
			CliType:    ps.CliType,
			CliPath:    ps.CliPath,
			WorkingDir: ps.WorkingDir,
			LastOutput: ps.LastOutput,
			Connected:  make(chan struct{}),
			lastActive: time.Now(),
			hbSeen:     time.Now(),
		}
		d.sessionsMu.Lock()
		d.sessions[ps.SessionID] = ws
		d.sessionsMu.Unlock()
		log.Printf("[daemon] restored session %s (bot=%s, last_active=%s)",
			safeShort(ps.SessionID), ps.BotID, ps.LastActive.Format("15:04:05"))
	}
}

func (d *Daemon) waitWorkerExit(ws *WorkerSession) {
	err := ws.Cmd.Wait()
	exitMsg := fmt.Sprintf("worker exit (pid=%d):", ws.Cmd.Process.Pid)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("[daemon] session %s %s err=%v", ws.SessionID[:8], exitMsg, err)
		}
	}
	d.removeSession(ws.SessionID)
}

func (d *Daemon) removeSession(id string) {
	d.sessionsMu.Lock()
	ws, ok := d.sessions[id]
	if ok {
		delete(d.sessions, id)
	}
	d.sessionsMu.Unlock()
	if !ok || ws == nil {
		return
	}
	d.store.MarkClosed(id)
	ws.mu.Lock()
	closed := ws.closed
	ws.closed = true
	callbacks := ws.onClose
	ws.onClose = nil
	ws.mu.Unlock()
	for _, fn := range callbacks {
		func() {
			defer func() { _ = recover() }()
			fn()
		}()
	}
	if !closed && ws.Conn != nil {
		_ = ws.Conn.Close()
	}
}

func (d *Daemon) CloseSession(id, reason string) {
	d.sessionsMu.RLock()
	ws, ok := d.sessions[id]
	d.sessionsMu.RUnlock()
	if !ok {
		return
	}
	msg := protocol.NewMessage(protocol.MsgClose, id, reason)
	if ws.Conn != nil {
		_, _ = msg.WriteTo(ws.Conn)
	}
	timeout := time.AfterFunc(2*time.Second, func() {
		if ws.Cmd != nil && ws.Cmd.Process != nil {
			_ = ws.Cmd.Process.Kill()
		}
	})
	defer timeout.Stop()
	if ws.Cmd != nil && ws.Cmd.Process != nil {
		_ = ws.Cmd.Wait()
	}
	d.removeSession(id)
	log.Printf("[daemon] session %s closed (reason=%s)", id[:8], reason)
}

func (d *Daemon) SendInput(id, input string) error {
	d.sessionsMu.RLock()
	ws, ok := d.sessions[id]
	d.sessionsMu.RUnlock()
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	select {
	case <-ws.Connected:
	case <-time.After(15 * time.Second):
		return fmt.Errorf("session %s worker not connected", id)
	}
	if ws.Conn == nil {
		return fmt.Errorf("session %s has no connection", id)
	}
	msg := protocol.NewMessage(protocol.MsgUserInput, id, input)
	ws.touchActive()
	_, err := msg.WriteTo(ws.Conn)
	return err
}

func (d *Daemon) Sessions() map[string]*WorkerSession {
	d.sessionsMu.RLock()
	defer d.sessionsMu.RUnlock()
	out := make(map[string]*WorkerSession, len(d.sessions))
	for k, v := range d.sessions {
		out[k] = v
	}
	return out
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
	ws, ok := d.sessions[sessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		log.Printf("[daemon] session %s not registered for incoming worker conn", safeShort(sessionID))
		_, _ = protocol.NewMessage(protocol.MsgError, sessionID, "session not registered").WriteTo(conn)
		return
	}

	ws.mu.Lock()
	if ws.closed {
		ws.mu.Unlock()
		return
	}
	if ws.Conn != nil && ws.Conn != conn {
		_ = ws.Conn.Close()
	}
	ws.Conn = conn
	ws.hbSeen = time.Now()
	alreadyConnected := ws.Connected == nil || isClosedChan(ws.Connected)
	if !alreadyConnected {
		close(ws.Connected)
	}
	ws.mu.Unlock()

	d.connMapMu.Lock()
	d.connToSess[conn] = sessionID
	d.connMapMu.Unlock()
	defer func() {
		d.connMapMu.Lock()
		delete(d.connToSess, conn)
		d.connMapMu.Unlock()
	}()

	if first.Type != protocol.MsgNewSession {
		d.routeMessage(first, ws)
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
		d.routeMessage(msg, ws)
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
		var created *WorkerSession
		var wsReady <-chan struct{} = nil
		created, err := d.NewSession(NewSessionOpts{
			SessionID: sid,
			BotID:     botID,
		})
		if err == nil {
			wsReady = created.Connected
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
		case <-wsReady:
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
	ws, ok := d.sessions[msg.SessionID]
	d.sessionsMu.RUnlock()
	if !ok {
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "session not found").WriteTo(clientConn)
		return
	}
	select {
	case <-ws.Connected:
	case <-time.After(15 * time.Second):
		_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "worker not connected").WriteTo(clientConn)
		return
	}
	if msg.Type == protocol.MsgUserInput {
		if ws.Conn == nil {
			_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "worker conn lost").WriteTo(clientConn)
			return
		}
		inCopy := *msg
		if _, err := inCopy.WriteTo(ws.Conn); err != nil {
			_, _ = protocol.NewMessage(protocol.MsgError, msg.SessionID, "forward: "+err.Error()).WriteTo(clientConn)
			return
		}
		ws.touchActive()
	}

	connMu := sync.Mutex{}
	stop := make(chan struct{})
	ws.mu.Lock()
	ws.onClose = append(ws.onClose, func() { closeOnce(stop) })
	ws.mu.Unlock()

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
			outs := ws.SnapshotOutput()
			if len(outs) <= seen {
				continue
			}
			for i := seen; i < len(outs); i++ {
				m := protocol.NewMessage(protocol.MsgOutput, ws.SessionID, outs[i])
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

func (d *Daemon) routeMessage(msg *protocol.Message, ws *WorkerSession) {
	switch msg.Type {
	case protocol.MsgReady:
		log.Printf("[daemon] session %s -> READY", ws.SessionID[:8])
		ws.touchActive()
		d.store.UpdateLastActive(ws.SessionID)
	case protocol.MsgHeartbeat:
		ws.mu.Lock()
		ws.hbSeen = time.Now()
		ws.mu.Unlock()
	case protocol.MsgOutput:
		ws.AddOutput(msg.Payload)
		ws.touchActive()
		d.store.UpdateOutput(ws.SessionID, msg.Payload)
		fmt.Printf("[session=%s] %s\n", ws.SessionID[:8], msg.Payload)
	case protocol.MsgError:
		log.Printf("[daemon] session %s error: %s", ws.SessionID[:8], msg.Payload)
	case protocol.MsgClose:
		log.Printf("[daemon] session %s closed by worker", ws.SessionID[:8])
		d.removeSession(ws.SessionID)
	default:
		log.Printf("[daemon] session %s unknown msg type=%s", ws.SessionID[:8], msg.Type)
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
	snaps := make([]*WorkerSession, 0, len(d.sessions))
	for _, ws := range d.sessions {
		snaps = append(snaps, ws)
	}
	d.sessionsMu.RUnlock()
	idleLimit := 15 * time.Minute
	hbLimit := 20 * time.Second
	for _, ws := range snaps {
		ws.mu.Lock()
		lastActive := ws.lastActive
		lastHb := ws.hbSeen
		closed := ws.closed
		ws.mu.Unlock()
		if closed {
			continue
		}
		if now.Sub(lastHb) > hbLimit && isClosedChan(ws.Connected) {
			log.Printf("[daemon] session %s heartbeat stale (>%s), closing", ws.SessionID[:8], hbLimit)
			d.CloseSession(ws.SessionID, "heartbeat stale")
			continue
		}
		if now.Sub(lastActive) > idleLimit {
			log.Printf("[daemon] session %s idle (>%s), closing", ws.SessionID[:8], idleLimit)
			d.CloseSession(ws.SessionID, "idle")
		}
	}
}
