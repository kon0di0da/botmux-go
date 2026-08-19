package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (d *Daemon) startHTTPServer() {
	defer d.wg.Done()

	ln, err := net.Listen("tcp", d.cfg.DashboardAddr)
	if err != nil {
		log.Printf("[daemon] http server listen %s: %v", d.cfg.DashboardAddr, err)
		return
	}
	d.httpListener = ln
	log.Printf("[daemon] dashboard HTTP server listening on %s", d.cfg.DashboardAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/healthz", d.cors(d.handleHealthz))
	mux.HandleFunc("/api/bots", d.cors(d.handleListBots))
	mux.HandleFunc("/api/sessions/", d.cors(d.handleSessionsSubrouter))
	mux.HandleFunc("/api/sessions", d.cors(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			d.handleCreateSession(w, r)
		} else {
			d.httpListSessions(w, r)
		}
	}))
	d.registerDashboardRoute(mux)

	srv := &http.Server{Addr: d.cfg.DashboardAddr, Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[daemon] http server error: %v", err)
		}
	}()

	<-d.ctx.Done()
	_ = srv.Close()
}

func (d *Daemon) cors(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (d *Daemon) handleSessionsSubrouter(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/sessions/")
	rest = strings.TrimSuffix(rest, "/")
	if rest == "" {
		d.httpListSessions(w, r)
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	sid := parts[0]
	if sid == "" {
		d.writeError(w, http.StatusBadRequest, "missing session_id")
		return
	}
	sub := ""
	if len(parts) > 1 {
		sub = parts[1]
	}

	switch r.Method {
	case http.MethodGet:
		switch sub {
		case "":
			d.handleGetSession(w, r, sid)
		case "history":
			d.handleGetSessionHistory(w, r, sid)
		default:
			d.writeError(w, http.StatusNotFound, "unknown subpath: "+sub)
		}
	case http.MethodPost:
		switch sub {
		case "send":
			d.handleSessionSend(w, r, sid)
		default:
			d.writeError(w, http.StatusNotFound, "unknown subpath: "+sub)
		}
	case http.MethodDelete:
		switch sub {
		case "":
			d.httpCloseSession(w, r, sid)
		default:
			d.writeError(w, http.StatusNotFound, "unknown subpath: "+sub)
		}
	case http.MethodOptions:
		w.WriteHeader(http.StatusNoContent)
	default:
		d.writeError(w, http.StatusMethodNotAllowed, "method not allowed: "+r.Method)
	}
}

type healthzResponse struct {
	OK            bool   `json:"ok"`
	Listen        string `json:"listen"`
	Dashboard     string `json:"dashboard"`
	StartedAt     string `json:"started_at"`
	SessionsCount int    `json:"sessions_count"`
	WorkersCount  int    `json:"workers_count"`
	BotsCount     int    `json:"bots_count"`
	Version       string `json:"version"`
}

func (d *Daemon) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	d.sessionsMu.RLock()
	sessionsCount := len(d.sessions)
	d.sessionsMu.RUnlock()

	d.workersMu.RLock()
	workersCount := len(d.workers)
	d.workersMu.RUnlock()

	resp := healthzResponse{
		OK:            true,
		Listen:        d.cfg.ListenAddr,
		Dashboard:     d.cfg.DashboardAddr,
		StartedAt:     d.startedAt.Format(time.RFC3339),
		SessionsCount: sessionsCount,
		WorkersCount:  workersCount,
		BotsCount:     len(d.cfg.Bots),
		Version:       "v4-pr-c",
	}

	d.writeJSON(w, http.StatusOK, resp)
}

func (d *Daemon) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[daemon] writeJSON error: %v", err)
	}
}

func (d *Daemon) writeError(w http.ResponseWriter, status int, msg string) {
	d.writeJSON(w, status, map[string]string{"error": fmt.Sprint(msg)})
}

type sessionListItem struct {
	SessionID  string   `json:"session_id"`
	BotID      string   `json:"bot_id"`
	CliType    string   `json:"cli_type"`
	Status     string   `json:"status"`
	Pid        int      `json:"pid"`
	Closed     bool     `json:"closed"`
	CreatedAt  string   `json:"created_at"`
	LastActive string   `json:"last_active"`
	Outputs    []string `json:"outputs"`
}

type sessionListResponse struct {
	Total  int               `json:"total"`
	Count  int               `json:"count"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
	Items  []sessionListItem `json:"items"`
}

func (d *Daemon) httpListSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		d.writeError(w, http.StatusMethodNotAllowed, "method not allowed: "+r.Method)
		return
	}
	q := r.URL.Query()
	botID := q.Get("bot_id")
	statusFilter := q.Get("status")
	sortKey := q.Get("sort")
	if sortKey == "" {
		sortKey = "created_at_desc"
	}
	limit := 50
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	d.sessionsMu.RLock()
	metas := make([]*SessionMeta, 0, len(d.sessions))
	for _, m := range d.sessions {
		if botID != "" && m.BotID != botID {
			continue
		}
		if statusFilter != "" {
			sf := strings.ToLower(statusFilter)
			closedTag := "closed"
			if sf == closedTag && !m.Closed {
				continue
			}
			if sf != closedTag && string(m.Status) != statusFilter {
				continue
			}
		}
		metas = append(metas, m)
	}
	d.sessionsMu.RUnlock()

	switch sortKey {
	case "created_at_asc":
		sort.Slice(metas, func(i, j int) bool { return metas[i].CreatedAt.Before(metas[j].CreatedAt) })
	case "last_active_asc":
		sort.Slice(metas, func(i, j int) bool { return metas[i].LastActive().Before(metas[j].LastActive()) })
	case "last_active_desc":
		sort.Slice(metas, func(i, j int) bool { return metas[i].LastActive().After(metas[j].LastActive()) })
	case "session_id":
		sort.Slice(metas, func(i, j int) bool { return metas[i].SessionID < metas[j].SessionID })
	default:
		sort.Slice(metas, func(i, j int) bool { return metas[i].CreatedAt.After(metas[j].CreatedAt) })
	}

	total := len(metas)
	if offset >= total {
		metas = nil
	} else {
		metas = metas[offset:]
		if len(metas) > limit {
			metas = metas[:limit]
		}
	}

	items := make([]sessionListItem, 0, len(metas))
	for _, m := range metas {
		pid := 0
		d.workersMu.RLock()
		if h, ok := d.workers[m.SessionID]; ok {
			pid = h.Pid
		}
		d.workersMu.RUnlock()
		items = append(items, sessionListItem{
			SessionID:  m.SessionID,
			BotID:      m.BotID,
			CliType:    m.CliType,
			Status:     string(m.Status),
			Pid:        pid,
			Closed:     m.Closed,
			CreatedAt:  m.CreatedAt.Format(time.RFC3339),
			LastActive: m.LastActive().Format(time.RFC3339),
			Outputs:    m.SnapshotOutput(),
		})
	}

	resp := sessionListResponse{
		Total:  total,
		Count:  len(items),
		Limit:  limit,
		Offset: offset,
		Items:  items,
	}
	d.writeJSON(w, http.StatusOK, resp)
}

type sessionDetailResponse struct {
	SessionID  string          `json:"session_id"`
	BotID      string          `json:"bot_id"`
	CliType    string          `json:"cli_type"`
	CliPath    string          `json:"cli_path"`
	WorkingDir string          `json:"working_dir"`
	Status     string          `json:"status"`
	Pid        int             `json:"pid"`
	Closed     bool            `json:"closed"`
	CreatedAt  string          `json:"created_at"`
	LastActive string          `json:"last_active"`
	TotalLines int             `json:"total_lines"`
	Offset     int             `json:"offset"`
	Limit      int             `json:"limit"`
	Outputs    []string        `json:"outputs"`
	Worker     *workerSnapshot `json:"worker"`
}

type workerSnapshot struct {
	Pid    int    `json:"pid"`
	Ready  bool   `json:"ready"`
	LastHb string `json:"last_hb"`
}

func (d *Daemon) handleGetSession(w http.ResponseWriter, r *http.Request, sid string) {
	d.sessionsMu.RLock()
	m, ok := d.sessions[sid]
	d.sessionsMu.RUnlock()
	if !ok {
		d.writeError(w, http.StatusNotFound, "session not found: "+sid)
		return
	}
	q := r.URL.Query()
	defaultLimit := maxMemoryOutputLines
	if defaultLimit > 500 {
		defaultLimit = 500
	}
	limit := defaultLimit
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= maxMemoryOutputLines {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	pid := 0
	var ws *workerSnapshot
	d.workersMu.RLock()
	if h, hw := d.workers[sid]; hw {
		pid = h.Pid
		lh := h.LastHb()
		ws = &workerSnapshot{
			Pid:    h.Pid,
			Ready:  h.IsReady(),
			LastHb: lh.Format(time.RFC3339),
		}
	}
	d.workersMu.RUnlock()
	all := m.SnapshotOutput()
	total := len(all)
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}
	paged := all[start:end]
	resp := sessionDetailResponse{
		SessionID:  m.SessionID,
		BotID:      m.BotID,
		CliType:    m.CliType,
		CliPath:    m.CliPath,
		WorkingDir: m.WorkingDir,
		Status:     string(m.Status),
		Pid:        pid,
		Closed:     m.Closed,
		CreatedAt:  m.CreatedAt.Format(time.RFC3339),
		LastActive: m.LastActive().Format(time.RFC3339),
		TotalLines: total,
		Offset:     start,
		Limit:      limit,
		Outputs:    paged,
		Worker:     ws,
	}
	d.writeJSON(w, http.StatusOK, resp)
}

func (d *Daemon) handleGetSessionHistory(w http.ResponseWriter, r *http.Request, sid string) {
	d.sessionsMu.RLock()
	m, ok := d.sessions[sid]
	d.sessionsMu.RUnlock()
	if !ok {
		d.writeError(w, http.StatusNotFound, "session not found: "+sid)
		return
	}
	outs := m.SnapshotOutput()
	format := strings.ToLower(r.URL.Query().Get("format"))
	switch format {
	case "txt", "text", "plain":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-history.txt"`, sid))
		w.WriteHeader(http.StatusOK)
		for i, line := range outs {
			_, _ = fmt.Fprintf(w, "[%d] %s\n", i+1, line)
		}
	default:
		d.writeJSON(w, http.StatusOK, map[string]any{
			"session_id": sid,
			"lines":      len(outs),
			"outputs":    outs,
		})
	}
}

type createSessionRequest struct {
	SessionID string `json:"session_id"`
	BotID     string `json:"bot_id"`
}

type createSessionResponse struct {
	OK        bool   `json:"ok"`
	SessionID string `json:"session_id"`
	BotID     string `json:"bot_id"`
	Status    string `json:"status"`
	Ready     bool   `json:"ready"`
}

func (d *Daemon) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		d.writeError(w, http.StatusMethodNotAllowed, "method not allowed: "+r.Method)
		return
	}
	var req createSessionRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			d.writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
			return
		}
	}
	if req.SessionID == "" {
		req.SessionID = fmt.Sprintf("s-%d", time.Now().UnixNano())
	}
	force := false
	if v := r.URL.Query().Get("force"); v == "1" || strings.EqualFold(v, "true") {
		force = true
	}
	if force {
		d.PurgeSession(req.SessionID)
	} else {
		d.sessionsMu.RLock()
		exist := d.sessions[req.SessionID]
		d.sessionsMu.RUnlock()
		if exist != nil && !exist.Closed {
			d.writeJSON(w, http.StatusConflict, map[string]any{
				"error":      "session already exists: " + req.SessionID,
				"session_id": req.SessionID,
				"status":     string(exist.Status),
				"closed":     exist.Closed,
				"suggestion": "use DELETE /api/sessions/" + req.SessionID + " first, or POST with ?force=true",
			})
			return
		}
		if exist != nil && exist.Closed {
			d.PurgeSession(req.SessionID)
		}
	}
	readyCh := make(chan struct{})
	opts := NewSessionOpts{
		SessionID: req.SessionID,
		BotID:     req.BotID,
		OnReady:   func(meta *SessionMeta) { closeOnce(readyCh) },
	}
	if _, err := d.NewSession(opts); err != nil {
		d.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ready := false
	select {
	case <-readyCh:
		ready = true
	case <-time.After(10 * time.Second):
	case <-d.ctx.Done():
	}

	d.sessionsMu.RLock()
	meta, _ := d.sessions[req.SessionID]
	status := ""
	botID := ""
	if meta != nil {
		status = string(meta.Status)
		botID = meta.BotID
	}
	d.sessionsMu.RUnlock()

	d.writeJSON(w, http.StatusCreated, createSessionResponse{
		OK:        true,
		SessionID: req.SessionID,
		BotID:     botID,
		Status:    status,
		Ready:     ready,
	})
}

type sendMessageRequest struct {
	Message string `json:"message"`
}

type sendMessageResponse struct {
	OK        bool     `json:"ok"`
	SessionID string   `json:"session_id"`
	Sent      string   `json:"sent"`
	Outputs   []string `json:"outputs"`
}

func (d *Daemon) handleSessionSend(w http.ResponseWriter, r *http.Request, sid string) {
	var req sendMessageRequest
	if r.Body == nil || r.ContentLength == 0 {
		d.writeError(w, http.StatusBadRequest, "missing body: {\"message\":\"...\"}")
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		d.writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Message) == "" {
		d.writeError(w, http.StatusBadRequest, "message required")
		return
	}
	if err := d.SendInput(sid, req.Message); err != nil {
		d.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	time.Sleep(400 * time.Millisecond)
	var outs []string
	d.sessionsMu.RLock()
	if m, ok := d.sessions[sid]; ok {
		outs = m.SnapshotOutput()
	}
	d.sessionsMu.RUnlock()
	d.writeJSON(w, http.StatusOK, sendMessageResponse{
		OK:        true,
		SessionID: sid,
		Sent:      req.Message,
		Outputs:   outs,
	})
}

type closeSessionResponse struct {
	OK        bool   `json:"ok"`
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
	Closed    bool   `json:"closed"`
}

func (d *Daemon) httpCloseSession(w http.ResponseWriter, r *http.Request, sid string) {
	reason := r.URL.Query().Get("reason")
	if reason == "" {
		reason = "dashboard request"
	}
	go d.CloseSession(sid, reason)
	time.Sleep(100 * time.Millisecond)
	closed := false
	d.sessionsMu.RLock()
	if m, ok := d.sessions[sid]; ok {
		closed = m.Closed
	}
	d.sessionsMu.RUnlock()
	d.writeJSON(w, http.StatusOK, closeSessionResponse{
		OK:        true,
		SessionID: sid,
		Reason:    reason,
		Closed:    closed,
	})
}

type botListItem struct {
	Name        string `json:"name"`
	BotID       string `json:"bot_id"`
	CliType     string `json:"cli_type"`
	CliPath     string `json:"cli_path,omitempty"`
	BackendType string `json:"backend_type"`
	WorkingDir  string `json:"working_dir,omitempty"`
}

func (d *Daemon) handleListBots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		d.writeError(w, http.StatusMethodNotAllowed, "method not allowed: "+r.Method)
		return
	}
	items := make([]botListItem, 0, len(d.cfg.Bots))
	for _, b := range d.cfg.Bots {
		items = append(items, botListItem{
			Name:        b.Name,
			BotID:       b.BotID,
			CliType:     string(b.CliType),
			CliPath:     b.CliPath,
			BackendType: string(b.BackendType),
			WorkingDir:  b.WorkingDir,
		})
	}
	d.writeJSON(w, http.StatusOK, map[string]any{
		"total": len(items),
		"bots":  items,
	})
}
