package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type breakerService interface {
	UpdateConfig(Config) error
	State() Snapshot
	Execute(context.Context, func(context.Context) error) error
}

type externalCaller interface {
	Call(context.Context, callRequest) error
}

type sleepCaller struct{}

func (sleepCaller) Call(ctx context.Context, req callRequest) error {
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

type configRequest struct {
	MaxFailures       int `json:"max_failures"`
	ResetTimeoutMS    int `json:"reset_timeout_ms"`
	HalfOpenMaxProbes int `json:"half_open_max_probes"`
}

type callRequest struct {
	DelayMS int  `json:"delay_ms"`
	Fail    bool `json:"fail"`
}

type httpApp struct {
	breaker breakerService
	caller  externalCaller
}

func newHTTPHandler(b breakerService, c externalCaller) http.Handler {
	a := httpApp{breaker: b, caller: c}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cb/config", a.handleConfig)
	mux.HandleFunc("GET /cb/state", a.handleState)
	mux.HandleFunc("POST /call", a.handleCall)
	return mux
}

func (a httpApp) handleConfig(w http.ResponseWriter, r *http.Request) {
	var req configRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	cfg := Config{
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

func (a httpApp) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.breaker.State())
}

func (a httpApp) handleCall(w http.ResponseWriter, r *http.Request) {
	var req callRequest
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
	if errors.Is(err, ErrBreakerOpen) || errors.Is(err, ErrTooManyProbes) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
