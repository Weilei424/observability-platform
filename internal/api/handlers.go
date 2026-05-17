package api

import (
	"encoding/json"
	"net/http"
	"os"
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	f, err := os.CreateTemp(s.cfg.DataDir, ".readyz-probe-*")
	if err != nil {
		writeUnavailable(w, err.Error())
		return
	}
	f.Close()
	os.Remove(f.Name())
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeUnavailable(w http.ResponseWriter, reason string) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{
		"status": "unavailable",
		"reason": reason,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
