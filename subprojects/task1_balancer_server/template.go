package task1_balancer_server

import (
	"encoding/json"
	"net/http"
	"time"

	"vk-interview-tests/subprojects/task1_balancer/domain"
)

const PathPick = "/pick"

func NewMux(b domain.Balancer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(PathPick, func(w http.ResponseWriter, r *http.Request) {
		be, picked, err := b.Pick()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		st := time.Now()
		_, doErr := be.Do(r.Context(), domain.Request{})
		picked.Done(doErr, time.Since(st))
		_ = json.NewEncoder(w).Encode(map[string]string{"backend": be.ID()})
	})
	return mux
}

func Run(addr string, b domain.Balancer) error { return http.ListenAndServe(addr, NewMux(b)) }
