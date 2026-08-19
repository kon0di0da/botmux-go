package daemon

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed dashboard.html
var dashboardFS embed.FS

func (d *Daemon) registerDashboardRoute(mux *http.ServeMux) {
	sub, _ := fs.Sub(dashboardFS, ".")
	fileServer := http.FileServer(http.FS(sub))
	mux.HandleFunc("/dashboard.html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fileServer.ServeHTTP(w, r)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := dashboardFS.ReadFile("dashboard.html")
		if err != nil {
			http.Error(w, "dashboard.html not embedded", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
}
