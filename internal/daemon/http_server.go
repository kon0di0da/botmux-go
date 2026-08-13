package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
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
	mux.HandleFunc("/api/healthz", d.handleHealthz)

	srv := &http.Server{Addr: d.cfg.DashboardAddr, Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[daemon] http server error: %v", err)
		}
	}()

	<-d.ctx.Done()
	_ = srv.Close()
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
		Version:       "v4-pr-a",
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
