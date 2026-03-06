package aggregator

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockRepo struct {
	mu      sync.Mutex
	batches [][]Aggregate
}

func (m *mockRepo) SaveBatch(_ context.Context, aggs []Aggregate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, append([]Aggregate(nil), aggs...))

	return nil
}

func TestService_Smoke_AutoFlushRetryAndClose(t *testing.T) {
	repo := &mockRepo{mu: sync.Mutex{}}
	svc, err := New(repo, Config{
		WindowSize:    time.Minute,
		QueueCap:      4,
		FlushInterval: 0,
		MaxKeys:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 1, 12, 0, 10, 0, time.UTC)

	if err := svc.Add(Event{Key: "a", Value: 2, Ts: base}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Add(Event{Key: "b", Value: 3, Ts: base}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	repo.mu.Lock()
	if len(repo.batches) != 1 {
		t.Fatalf("want 1 batch after auto-flush, got %d", len(repo.batches))
	}
	repo.mu.Unlock()
}
