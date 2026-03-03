package counter

import (
	"context"
	"sync"
)

// mockRepo — потокобезопасный in-memory мок для unit-тестов сервиса.
type mockRepo struct {
	mu    sync.Mutex
	store map[string]int64
}

// newMockRepo создаёт пустой мок-репозиторий.
func newMockRepo() *mockRepo {
	return &mockRepo{store: make(map[string]int64)}
}

// AddBatch имитирует атомарное применение батча в хранилище.
func (m *mockRepo) AddBatch(_ context.Context, deltas map[string]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, delta := range deltas {
		m.store[id] += delta
	}
	return nil
}

// Get возвращает текущее значение счётчика из мока.
func (m *mockRepo) Get(_ context.Context, id string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store[id], nil
}
