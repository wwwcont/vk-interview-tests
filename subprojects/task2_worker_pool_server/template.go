package task2_worker_pool_server

import (
	"context"
	"net/http"

	"vk-interview-tests/subprojects/task2_worker_pool/domain"
)

const PathTask = "/task"

func NewMux(p domain.Pool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(PathTask, func(w http.ResponseWriter, r *http.Request) {
		prio := domain.Normal
		if r.URL.Query().Get("priority") == "high" {
			prio = domain.High
		}
		err := p.Submit(r.Context(), prio, func(ctx context.Context) error { return nil })
		if err != nil {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	return mux
}
