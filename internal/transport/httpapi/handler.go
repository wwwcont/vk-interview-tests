// Package httpapi exposes a tiny net/http layer for the breaker demo.
//
// In simple words:
// - transport layer parses JSON requests,
// - calls application/domain behavior,
// - maps result to HTTP status codes.
//
// This is intentionally minimal but shows clean boundaries:
// breaker behavior is behind interface and external call simulation is also isolated.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"vk-interview-tests/internal/domain/breaker"
)

// BreakerService is a small dependency contract required by HTTP layer.
type BreakerService interface {
	UpdateConfig(breaker.Config) error
	State() breaker.Snapshot
	Execute(context.Context, func(context.Context) error) error
}

// ExternalCaller abstracts external IO call.
// We keep it tiny so it can be replaced in tests or demos.
type ExternalCaller interface {
	Call(context.Context, CallRequest) error
}

// SleepCaller is a simple stub: sleep, optionally fail.
type SleepCaller struct{}

// Call simulates slow/failing downstream dependency.
func (SleepCaller) Call(ctx context.Context, req CallRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(req.DelayMS) * time.Millisecond):
	}
	if req.Fail {
		return errors.New("simulated failure")
	}
	return nil
}

// ConfigRequest is input DTO for breaker config update.
type ConfigRequest struct {
	MaxFailures       int `json:"max_failures"`
	ResetTimeoutMS    int `json:"reset_timeout_ms"`
	HalfOpenMaxProbes int `json:"half_open_max_probes"`
}

// CallRequest is input DTO for protected call endpoint.
type CallRequest struct {
	DelayMS int  `json:"delay_ms"`
	Fail    bool `json:"fail"`
}

// App holds all dependencies used by handlers.
type App struct {
	breaker BreakerService
	caller  ExternalCaller
}

// NewHandler builds and returns preconfigured ServeMux.
func NewHandler(b BreakerService, c ExternalCaller) http.Handler {
	a := App{breaker: b, caller: c}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cb/config", a.handleConfig)
	mux.HandleFunc("GET /cb/state", a.handleState)
	mux.HandleFunc("POST /call", a.handleCall)
	return mux
}

// handleConfig parses config JSON and updates breaker config.
func (a App) handleConfig(w http.ResponseWriter, r *http.Request) {
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
	if err := a.breaker.UpdateConfig(cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleState returns current breaker snapshot.
func (a App) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.breaker.State())
}

// handleCall executes protected call with hard timeout 100ms.
func (a App) handleCall(w http.ResponseWriter, r *http.Request) {
	var req CallRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
	defer cancel()
	err := a.breaker.Execute(ctx, func(ctx context.Context) error { return a.caller.Call(ctx, req) })

	if err == nil {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if errors.Is(err, breaker.ErrBreakerOpen) || errors.Is(err, breaker.ErrTooManyProbes) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
}

// writeJSON writes response status and JSON body.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
