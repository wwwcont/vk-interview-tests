package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
)

type configRequest struct {
	MaxFailures       int `json:"max_failures"`
	ResetTimeoutMS    int `json:"reset_timeout_ms"`
	HalfOpenMaxProbes int `json:"half_open_max_probes"`
}

type callRequest struct {
	DelayMS int  `json:"delay_ms"`
	Fail    bool `json:"fail"`
}

type app struct {
	breaker *CircuitBreaker
}

func main() {
	breaker, err := NewCircuitBreaker(Config{MaxFailures: 3, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	if err != nil {
		log.Fatal(err)
	}

	a := &app{breaker: breaker}
	http.HandleFunc("POST /cb/config", a.handleConfig)
	http.HandleFunc("GET /cb/state", a.handleState)
	http.HandleFunc("POST /call", a.handleCall)

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func (a *app) handleConfig(w http.ResponseWriter, r *http.Request) {
	var req configRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.breaker.State())
}

func (a *app) handleCall(w http.ResponseWriter, r *http.Request) {
	var req callRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
	defer cancel()

	err := a.breaker.Execute(ctx, func(ctx context.Context) error {
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
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	if errors.Is(err, ErrBreakerOpen) {
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
