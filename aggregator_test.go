package aggregator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockRepo struct {
	mu      sync.Mutex
	batches [][]Aggregate
	failN   int
	blockCh chan struct{}
	startCh chan struct{}
}

func (m *mockRepo) SaveBatch(_ context.Context, aggs []Aggregate) error {
	if m.startCh != nil {
		select {
		case m.startCh <- struct{}{}:
		default:
		}
	}
	if m.blockCh != nil {
		<-m.blockCh
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failN > 0 {
		m.failN--
		return errors.New("boom")
	}
	cpy := append([]Aggregate(nil), aggs...)
	m.batches = append(m.batches, cpy)
	return nil
}

func sumsFromBatches(bs [][]Aggregate) map[string]int64 {
	out := map[string]int64{}
	for _, b := range bs {
		for _, a := range b {
			out[a.WindowStart.Format(time.RFC3339)+"|"+a.Key] += a.Sum
		}
	}
	return out
}

func TestAggregate_SumsByWindowAndKey(t *testing.T) {
	repo := &mockRepo{}
	svc, _ := New(repo, Config{WindowSize: time.Minute, QueueCap: 16})
	base := time.Date(2025, 1, 1, 12, 0, 10, 0, time.UTC)
	_ = svc.Add(Event{Key: "a", Value: 2, Ts: base})
	_ = svc.Add(Event{Key: "a", Value: 3, Ts: base.Add(20 * time.Second)})
	_ = svc.Add(Event{Key: "b", Value: 5, Ts: base})
	_ = svc.Add(Event{Key: "a", Value: 7, Ts: base.Add(time.Minute)})
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = svc.Close(context.Background())

	got := sumsFromBatches(repo.batches)
	w1 := base.Truncate(time.Minute).Format(time.RFC3339) + "|a"
	w1b := base.Truncate(time.Minute).Format(time.RFC3339) + "|b"
	w2 := base.Add(time.Minute).Truncate(time.Minute).Format(time.RFC3339) + "|a"
	if got[w1] != 5 || got[w1b] != 5 || got[w2] != 7 || len(got) != 3 {
		t.Fatalf("unexpected sums: %#v", got)
	}
}

func TestFlush_SavesAndClears(t *testing.T) {
	repo := &mockRepo{}
	svc, _ := New(repo, Config{WindowSize: time.Minute, QueueCap: 16})
	now := time.Now().UTC()
	_ = svc.Add(Event{Key: "k", Value: 10, Ts: now})
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = svc.Close(context.Background())

	if len(repo.batches) != 1 || len(repo.batches[0]) != 1 || repo.batches[0][0].Sum != 10 {
		t.Fatalf("unexpected batches: %#v", repo.batches)
	}
}

func TestFlush_ErrorDoesNotLoseData(t *testing.T) {
	repo := &mockRepo{failN: 1}
	svc, _ := New(repo, Config{WindowSize: time.Minute, QueueCap: 16})
	now := time.Now().UTC()
	_ = svc.Add(Event{Key: "x", Value: 4, Ts: now})
	_ = svc.Add(Event{Key: "x", Value: 6, Ts: now})
	if err := svc.Flush(context.Background()); err == nil {
		t.Fatal("expected first flush error")
	}
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = svc.Close(context.Background())

	got := sumsFromBatches(repo.batches)
	key := now.Truncate(time.Minute).Format(time.RFC3339) + "|x"
	if got[key] != 10 || len(repo.batches) != 1 {
		t.Fatalf("unexpected persisted data: batches=%#v sums=%#v", repo.batches, got)
	}
}

func TestAdd_BackpressureQueueFull(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	repo := &mockRepo{blockCh: block, startCh: started}
	svc, _ := New(repo, Config{WindowSize: time.Minute, QueueCap: 1})
	now := time.Now().UTC()
	_ = svc.Add(Event{Key: "pre", Value: 1, Ts: now})

	flushDone := make(chan error, 1)
	go func() { flushDone <- svc.Flush(context.Background()) }()
	<-started

	if err := svc.Add(Event{Key: "a", Value: 1, Ts: now}); err != nil {
		t.Fatalf("first add must pass, got %v", err)
	}
	if err := svc.Add(Event{Key: "b", Value: 1, Ts: now}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	close(block)
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	_ = svc.Close(context.Background())
}

func TestConcurrentAdd(t *testing.T) {
	repo := &mockRepo{}
	svc, _ := New(repo, Config{WindowSize: time.Minute, QueueCap: 20000})
	now := time.Now().UTC()
	const goroutines = 50
	const perG = 200
	wg := sync.WaitGroup{}
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				if err := svc.Add(Event{Key: "k", Value: 1, Ts: now}); err != nil {
					t.Errorf("add failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = svc.Close(context.Background())
	got := sumsFromBatches(repo.batches)
	key := now.Truncate(time.Minute).Format(time.RFC3339) + "|k"
	if got[key] != goroutines*perG {
		t.Fatalf("sum mismatch: got=%d want=%d", got[key], goroutines*perG)
	}
}
