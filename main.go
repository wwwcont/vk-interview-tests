package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
)

type backendUpsertRequest struct {
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
	Backends []BackendSnapshot `json:"backends"`
}

func main() {
	b := NewBalancer()
	mux := newMux(b)
	log.Printf("listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newMux(b *Balancer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /backends", func(w http.ResponseWriter, r *http.Request) {
		var req backendUpsertRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.ID == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}
		if req.DelayMS < 0 {
			http.Error(w, "delay_ms must be >= 0", http.StatusBadRequest)
			return
		}
		b.UpsertBackend(req.ID, req.Healthy, req.DelayMS, req.Fail)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /backends/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/health") {
			http.NotFound(w, r)
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/backends/"), "/health")
		if id == "" || strings.Contains(id, "/") {
			http.Error(w, "invalid backend id", http.StatusBadRequest)
			return
		}
		var req healthRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if !b.SetHealth(id, req.Healthy) {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /request", func(w http.ResponseWriter, r *http.Request) {
		backendID, ok := b.DoRequest(r.Context())
		status := http.StatusOK
		if backendID == "" {
			status = http.StatusServiceUnavailable
		} else if !ok {
			status = http.StatusBadGateway
		}
		writeJSON(w, status, requestResponse{BackendID: backendID, OK: ok})
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, statsResponse{Backends: b.Snapshot()})
	})
	return mux
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
