package interview

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type aggRepoMock struct {
	mu       sync.Mutex
	saved    map[string]int64
	calls    int
	lastRaw  map[string]int64
	failSave bool
}

func (m *aggRepoMock) SaveBatch(_ context.Context, data map[string]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSave {
		return errors.New("save failed")
	}
	m.calls++
	m.lastRaw = make(map[string]int64, len(data))
	if m.saved == nil {
		m.saved = make(map[string]int64)
	}
	for k, v := range data {
		m.lastRaw[k] = v
		m.saved[k] += v
	}
	return nil
}

func TestAggregatorAggregatesEvents(t *testing.T) {
	repo := &aggRepoMock{}
	agg := NewAggregator(repo)

	agg.Add(Event{Key: "k1", Value: 2})
	agg.Add(Event{Key: "k1", Value: 5})
	agg.Add(Event{Key: "k2", Value: 1})

	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.saved["k1"] != 7 || repo.saved["k2"] != 1 {
		t.Fatalf("unexpected saved data: %#v", repo.saved)
	}
}

func TestAggregatorConcurrentAdd(t *testing.T) {
	repo := &aggRepoMock{}
	agg := NewAggregator(repo)

	const goroutines = 20
	const iterations = 500

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				agg.Add(Event{Key: "k", Value: 1})
			}
		}()
	}
	wg.Wait()

	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	want := int64(goroutines * iterations)
	if repo.saved["k"] != want {
		t.Fatalf("saved[k] = %d, want %d", repo.saved["k"], want)
	}
}

func TestAggregatorFlushClearsBuffer(t *testing.T) {
	repo := &aggRepoMock{}
	agg := NewAggregator(repo)

	agg.Add(Event{Key: "k", Value: 10})
	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.calls != 1 {
		t.Fatalf("SaveBatch calls = %d, want 1", repo.calls)
	}
}

func TestAggregatorFlushRestoreOnError(t *testing.T) {
	repo := &aggRepoMock{failSave: true}
	agg := NewAggregator(repo)
	agg.Add(Event{Key: "k", Value: 10})

	err := agg.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() error = nil, want non-nil")
	}

	repo.failSave = false
	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.saved["k"] != 10 {
		t.Fatalf("saved[k] = %d, want 10", repo.saved["k"])
	}
}
