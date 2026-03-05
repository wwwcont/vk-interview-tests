package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"vk-interview-tests/internal/domain/breaker"
)

// ConfigRequest — JSON для POST /cb/config.
type ConfigRequest struct {
	MaxFailures       int `json:"max_failures"`
	ResetTimeoutMS    int `json:"reset_timeout_ms"`
	HalfOpenMaxProbes int `json:"half_open_max_probes"`
}

// CallRequest — JSON для POST /call.
type CallRequest struct {
	DelayMS int  `json:"delay_ms"`
	Fail    bool `json:"fail"`
}

// NewHandler создаёт HTTP-маршруты вокруг конкретного CircuitBreaker.
func NewHandler(b *breaker.CircuitBreaker) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /cb/config", func(w http.ResponseWriter, r *http.Request) {
		var req ConfigRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		cfg := breaker.Config{
			MaxFailures:       req.MaxFailures,
			ResetTimeout:      time.Duration(req.ResetTimeoutMS) * time.Millisecond,
			HalfOpenMaxProbes: req.HalfOpenMaxProbes,
		}
		if err := b.UpdateConfig(cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	mux.HandleFunc("GET /cb/state", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, b.State())
	})

	mux.HandleFunc("POST /call", func(w http.ResponseWriter, r *http.Request) {
		var req CallRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
		defer cancel()

		err := b.Execute(ctx, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(req.DelayMS) * time.Millisecond):
			}
			if req.Fail {
				return errors.New("simulated failure")
			}
			return nil
		})

		if err == nil {
			writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
			return
		}
		if errors.Is(err, breaker.ErrBreakerOpen) || errors.Is(err, breaker.ErrTooManyProbes) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
	})

	return mux
}

// writeJSON пишет статус и JSON-ответ.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
