package task4_circuit_breaker_server

import (
	"context"
	"net/http"

	"vk-interview-tests/subprojects/task4_circuit_breaker/domain"
)

const PathExecute = "/execute"

func NewMux(b domain.Breaker) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(PathExecute, func(w http.ResponseWriter, r *http.Request) {
		err := b.Execute(r.Context(), func(context.Context) error { return nil })
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
