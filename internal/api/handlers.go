package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		s.log.Error("encode healthz response", "err", err)
	}
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	f, err := os.CreateTemp(s.cfg.DataDir, ".readyz-probe-*")
	if err != nil {
		writeUnavailable(w, s.log, err.Error())
		return
	}
	f.Close()
	os.Remove(f.Name())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		s.log.Error("encode healthz response", "err", err)
	}
}

func writeUnavailable(w http.ResponseWriter, log *slog.Logger, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "unavailable",
		"reason": reason,
	}); err != nil {
		log.Error("encode unavailable response", "err", err)
	}
}
