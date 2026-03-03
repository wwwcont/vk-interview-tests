package memory

import (
	"context"
	"sync"
)

// Repo — очень простой in-memory репозиторий для локального запуска и тестов.
// В реальном проекте сюда можно поставить PostgreSQL/Redis-реализацию с тем же интерфейсом.
type Repo struct {
	mu    sync.Mutex
	store map[string]int64
}

// New создаёт пустой in-memory репозиторий.
func New() *Repo {
	return &Repo{store: make(map[string]int64)}
}

// AddBatch атомарно (под одним lock) применяет все дельты к значениям.
func (r *Repo) AddBatch(_ context.Context, deltas map[string]int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, delta := range deltas {
		r.store[id] += delta
	}
	return nil
}

// Get возвращает текущее значение счётчика по id.
func (r *Repo) Get(_ context.Context, id string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store[id], nil
}
