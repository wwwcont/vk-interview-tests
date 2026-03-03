package interview

import (
	"context"
	"sync"
	"testing"
)

type counterRepoMock struct {
	mu     sync.Mutex
	values map[string]int64
	adds   map[string]int64
}

func newCounterRepoMock() *counterRepoMock {
	return &counterRepoMock{values: make(map[string]int64), adds: make(map[string]int64)}
}

func (m *counterRepoMock) Add(_ context.Context, id string, delta int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[id] += delta
	m.adds[id] += delta
	return nil
}

func (m *counterRepoMock) Get(_ context.Context, id string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.values[id], nil
}

func TestCounterServiceConcurrentIncr(t *testing.T) {
	repo := newCounterRepoMock()
	svc := NewCounterService(repo)

	const goroutines = 32
	const iterations = 1000

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				svc.Incr("a", 1)
			}
		}()
	}
	wg.Wait()

	got, err := svc.Get(context.Background(), "a")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	want := int64(goroutines * iterations)
	if got != want {
		t.Fatalf("Get() = %d, want %d", got, want)
	}
}

func TestCounterServiceFlushSendsData(t *testing.T) {
	repo := newCounterRepoMock()
	svc := NewCounterService(repo)

	svc.Incr("x", 5)
	svc.Incr("x", 7)
	svc.Incr("y", 3)

	if err := svc.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.adds["x"] != 12 || repo.adds["y"] != 3 {
		t.Fatalf("unexpected adds: %#v", repo.adds)
	}
}

func TestCounterServiceGetIncludesPending(t *testing.T) {
	repo := newCounterRepoMock()
	repo.values["id"] = 10

	svc := NewCounterService(repo)
	svc.Incr("id", 4)

	got, err := svc.Get(context.Background(), "id")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != 14 {
		t.Fatalf("Get() = %d, want 14", got)
	}
}
