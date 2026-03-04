package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"vk-interview-tests/internal/app"
	"vk-interview-tests/internal/domain"
)

type Handler struct{ s *app.Service }

func New(s *app.Service) *Handler { return &Handler{s: s} }
func (h *Handler) Routes() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /jobs", h.create)
	m.HandleFunc("GET /jobs/", h.get)
	m.HandleFunc("POST /admin/pool/resize", h.resize)
	m.HandleFunc("POST /admin/shutdown", h.shutdown)
	return m
}

type req struct {
	Type, Priority, IdempotencyKey string
	Payload                        domain.SleepPayload
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var x req
	if json.NewDecoder(r.Body).Decode(&x) != nil {
		errJ(w, 400, "bad_request", "invalid json")
		return
	}
	j, e := h.s.CreateJob(r.Context(), app.CreateCmd{Type: x.Type, Priority: x.Priority, Payload: x.Payload, IdempotencyKey: x.IdempotencyKey})
	if e != nil {
		h.err(w, e)
		return
	}
	ok(w, map[string]any{"job_id": j.ID, "status": j.Status})
}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	j, e := h.s.GetJob(r.Context(), strings.TrimPrefix(r.URL.Path, "/jobs/"))
	if e != nil {
		h.err(w, e)
		return
	}
	ok(w, j)
}
func (h *Handler) resize(w http.ResponseWriter, r *http.Request) {
	n, e := strconv.Atoi(r.URL.Query().Get("n"))
	if e != nil || n <= 0 {
		errJ(w, 400, "bad_request", "invalid n")
		return
	}
	if e = h.s.ResizePool(r.Context(), n); e != nil {
		h.err(w, e)
		return
	}
	ok(w, map[string]int{"workers": n})
}
func (h *Handler) shutdown(w http.ResponseWriter, r *http.Request) {
	t, _ := strconv.Atoi(r.URL.Query().Get("timeout_ms"))
	if t <= 0 {
		t = 5000
	}
	a, e := h.s.Shutdown(context.Background(), time.Duration(t)*time.Millisecond)
	if e != nil {
		j(w, 503, map[string]any{"error": "shutdown timeout", "code": "timeout", "active_jobs": a})
		return
	}
	ok(w, map[string]string{"status": "ok"})
}
func (h *Handler) err(w http.ResponseWriter, e error) {
	s, c := 500, "internal_error"
	switch {
	case errors.Is(e, domain.ErrQueueFull):
		s, c = 429, "queue_full"
	case errors.Is(e, domain.ErrShuttingDown):
		s, c = 503, "shutting_down"
	case errors.Is(e, domain.ErrNotFound):
		s, c = 404, "not_found"
	case errors.Is(e, domain.ErrInvalidJobType) || errors.Is(e, domain.ErrInvalidPayload) || errors.Is(e, domain.ErrInvalidPriority):
		s, c = 400, "validation_error"
	}
	errJ(w, s, c, e.Error())
}
func ok(w http.ResponseWriter, v any) { j(w, 200, v) }
func errJ(w http.ResponseWriter, s int, c, m string) {
	j(w, s, map[string]string{"error": m, "code": c})
}
func j(w http.ResponseWriter, s int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(s)
	_ = json.NewEncoder(w).Encode(v)
}
