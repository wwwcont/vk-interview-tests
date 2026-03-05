// Пакет httpapi — это тонкий HTTP-слой поверх domain-логики брекера.
//
// Простая идея слоя:
// - принять JSON из запроса,
// - вызвать нужный метод домена,
// - вернуть понятный HTTP-ответ.
//
// Здесь специально минимум сложности:
// - у брекера и внешнего вызова есть маленькие интерфейсы,
// - внешний вызов можно подменять,
// - логика статусов и ошибок лежит рядом с endpoint'ами.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"vk-interview-tests/internal/domain/breaker"
)

// BreakerService — контракт, который нужен HTTP-слою от домена.
type BreakerService interface {
	UpdateConfig(breaker.Config) error
	State() breaker.Snapshot
	Execute(context.Context, func(context.Context) error) error
}

// ExternalCaller — абстракция внешнего вызова (заглушка/реальный клиент).
type ExternalCaller interface {
	Call(context.Context, CallRequest) error
}

// SleepCaller — простая заглушка: ждёт delay и при необходимости падает ошибкой.
type SleepCaller struct{}

// Call имитирует внешний сервис: либо ждём delay, либо получаем timeout/cancel.
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

// ConfigRequest — входной JSON для обновления конфига брекера.
type ConfigRequest struct {
	MaxFailures       int `json:"max_failures"`
	ResetTimeoutMS    int `json:"reset_timeout_ms"`
	HalfOpenMaxProbes int `json:"half_open_max_probes"`
}

// CallRequest — входной JSON для защищённого вызова.
type CallRequest struct {
	DelayMS int  `json:"delay_ms"`
	Fail    bool `json:"fail"`
}

// App хранит зависимости HTTP-слоя.
type App struct {
	breaker BreakerService
	caller  ExternalCaller
}

// NewHandler собирает маршруты и возвращает готовый http.Handler.
func NewHandler(b BreakerService, c ExternalCaller) http.Handler {
	a := App{breaker: b, caller: c}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cb/config", a.handleConfig)
	mux.HandleFunc("GET /cb/state", a.handleState)
	mux.HandleFunc("POST /call", a.handleCall)
	return mux
}

// handleConfig читает JSON и обновляет runtime-конфиг брекера.
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

// handleState возвращает текущий снимок состояния брекера.
func (a App) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.breaker.State())
}

// handleCall выполняет внешний вызов через брекер с таймаутом 100ms.
// Если брекер не пускает вызов — отдаём 503, иначе ошибки внешнего вызова идут как 502.
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

// writeJSON — маленький helper для единообразных JSON-ответов.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
