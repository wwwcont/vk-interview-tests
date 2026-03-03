package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"vk-interview-tests/internal/domain/counter"
)

// Server — максимально простой HTTP-адаптер поверх сервиса.
// Здесь только маршруты и преобразование HTTP <-> service calls.
type Server struct {
	svc *counter.Service
}

// New создаёт HTTP-сервер-адаптер.
func New(svc *counter.Service) *Server {
	return &Server{svc: svc}
}

// Handler возвращает готовый http.Handler с тремя endpoint'ами:
// /incr, /get, /flush.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/incr", s.handleIncr)
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/flush", s.handleFlush)
	return mux
}

// handleIncr принимает id и delta в query-параметрах и вызывает Incr.
func (s *Server) handleIncr(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	deltaStr := r.URL.Query().Get("delta")
	if deltaStr == "" {
		deltaStr = "1"
	}
	delta, err := strconv.ParseInt(deltaStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid delta", http.StatusBadRequest)
		return
	}

	s.svc.Incr(id, delta)
	w.WriteHeader(http.StatusNoContent)
}

// handleGet читает значение счётчика и возвращает JSON.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	value, err := s.svc.Get(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":    id,
		"value": value,
	})
}

// handleFlush принудительно запускает flush из HTTP.
func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.svc.Flush(withFallbackContext(r.Context())); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func withFallbackContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		if !errors.Is(err, context.Canceled) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
