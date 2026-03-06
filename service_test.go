package counter

import (
	"context"
	"sync"
	"testing"
)

// mockRepo — простой потокобезопасный in-memory репозиторий для базового теста.
type mockRepo struct {
	mu sync.Mutex
	m  map[string]int64
}

func newMockRepo() *mockRepo {
	return &mockRepo{m: make(map[string]int64)}
}

func (r *mockRepo) AddBatch(_ context.Context, deltas map[string]int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, delta := range deltas {
		r.m[id] += delta
	}
	return nil
}

func (r *mockRepo) Get(_ context.Context, id string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[id], nil
}

// Базовый алгоритм: pending учитывается в Get, после Flush уходит в repo,
// а повторный Flush без новых Incr ничего не меняет.
func TestCounterService_BasicFlow(t *testing.T) {
	repo := newMockRepo()
	svc, err := New(repo, Config{Shards: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	svc.Incr("video-1", 2)
	svc.Incr("video-1", 3)

	got, err := svc.Get(context.Background(), "video-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("pending get: got %d, want 5", got)
	}

	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	persisted, _ := repo.Get(context.Background(), "video-1")
	if persisted != 5 {
		t.Fatalf("repo after flush: got %d, want 5", persisted)
	}

	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	persisted2, _ := repo.Get(context.Background(), "video-1")
	if persisted2 != 5 {
		t.Fatalf("repo after empty flush: got %d, want 5", persisted2)
	}
}
