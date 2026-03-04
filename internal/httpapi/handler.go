package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"vk-interview-tests/internal/domain"
	"vk-interview-tests/internal/service"
)

const (
	errInvalidJSON = "invalid json"
	errIDRequired  = "id is required"
	errInvalidID   = "invalid backend id"
	errDelay       = "delay_ms must be >= 0"
)

type upsertRequest struct {
	ID      string `json:"id"`
	Healthy bool   `json:"healthy"`
	DelayMS int    `json:"delay_ms"`
	Fail    bool   `json:"fail"`
}

type healthRequest struct {
	Healthy bool `json:"healthy"`
}

type requestResponse struct {
	BackendID string `json:"backend_id"`
	OK        bool   `json:"ok"`
}

type statsResponse struct {
	Backends []domain.Snapshot `json:"backends"`
}

func NewMux(svc *service.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /backends", func(w http.ResponseWriter, r *http.Request) {
		var req upsertRequest
		if !decode(w, r, &req) {
			return
		}
		if req.ID == "" {
			http.Error(w, errIDRequired, http.StatusBadRequest)
			return
		}
		if req.DelayMS < 0 {
			http.Error(w, errDelay, http.StatusBadRequest)
			return
		}
		svc.UpsertBackend(req.ID, req.Healthy, req.DelayMS, req.Fail)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /backends/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/health") {
			http.NotFound(w, r)
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/backends/"), "/health")
		if id == "" || strings.Contains(id, "/") {
			http.Error(w, errInvalidID, http.StatusBadRequest)
			return
		}
		var req healthRequest
		if !decode(w, r, &req) {
			return
		}
		if !svc.SetBackendHealth(id, req.Healthy) {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /request", func(w http.ResponseWriter, r *http.Request) {
		backendID, err := svc.DoRequest(r.Context())
		status := http.StatusOK
		ok := err == nil
		if errors.Is(err, domain.ErrNoHealthyBackend) {
			status = http.StatusServiceUnavailable
		} else if err != nil {
			status = http.StatusBadGateway
		}
		writeJSON(w, status, requestResponse{BackendID: backendID, OK: ok})
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, statsResponse{Backends: svc.Stats()})
	})
	return mux
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		http.Error(w, errInvalidJSON, http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
