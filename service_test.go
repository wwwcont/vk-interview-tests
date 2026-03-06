package counter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockRepo — потокобезопасный in-memory репозиторий с опциональным hook AddBatch.
type mockRepo struct {
	mu sync.Mutex
	m  map[string]int64

	addCalls int64

	addBatchHook func(ctx context.Context, deltas map[string]int64) error
}

func newMockRepo() *mockRepo {
	return &mockRepo{m: make(map[string]int64)}
}

func (r *mockRepo) AddBatch(ctx context.Context, deltas map[string]int64) error {
	atomic.AddInt64(&r.addCalls, 1)
	if r.addBatchHook != nil {
		if err := r.addBatchHook(ctx, deltas); err != nil {
			return err
		}
	}

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

func (r *mockRepo) addCount() int64 {
	return atomic.LoadInt64(&r.addCalls)
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

// Проверяем timeout в background auto-flush:
// AddBatch блокируется до ctx.Done(), после чего должен завершиться по deadline.
func TestAutoFlush_UsesTimeout(t *testing.T) {
	repo := newMockRepo()
	repo.addBatchHook = func(ctx context.Context, _ map[string]int64) error {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}

	svc, err := New(repo, Config{Shards: 4, FlushInterval: 10 * time.Millisecond, FlushTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	svc.Incr("timeout-id", 1)

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if repo.addCount() >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected multiple AddBatch attempts, got %d", repo.addCount())
}

// Проверяем coalescing:
// при множестве параллельных Flush должен быть только один реальный AddBatch.
func TestFlush_CoalescesParallelCalls(t *testing.T) {
	repo := newMockRepo()
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	repo.addBatchHook = func(_ context.Context, _ map[string]int64) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		return nil
	}

	svc, err := New(repo, Config{Shards: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	svc.Incr("coalesce", 10)

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			errCh <- svc.Flush(context.Background())
		}()
	}

	select {
	case <-entered:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("no AddBatch call observed")
	}

	close(block)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("flush returned error: %v", err)
		}
	}

	if calls := repo.addCount(); calls != 1 {
		t.Fatalf("AddBatch calls=%d, want 1", calls)
	}
	v, _ := repo.Get(context.Background(), "coalesce")
	if v != 10 {
		t.Fatalf("repo value=%d, want 10", v)
	}
}
