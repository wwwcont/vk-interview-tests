package task3_rate_limiter_server

import (
	"net/http"

	"vk-interview-tests/subprojects/task3_rate_limiter/domain"
)

const PathAllow = "/allow"

func NewMux(l domain.Limiter) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(PathAllow, func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			key = "default"
		}
		if !l.Allow(key) {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
