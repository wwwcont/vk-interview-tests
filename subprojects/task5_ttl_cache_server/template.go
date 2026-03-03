package task5_ttl_cache_server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"vk-interview-tests/subprojects/task5_ttl_cache/domain"
)

const PathGet = "/cache/get"

func NewMux(c domain.Cache) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(PathGet, func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		v, err := c.GetOrLoad(r.Context(), key, time.Minute, func(context.Context) (any, error) { return "demo", nil })
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": v})
	})
	return mux
}
