package counter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockRepo — простой потокобезопасный in-memory репозиторий для тестов.
// Никаких внешних библиотек: только map+mutex+несколько вспомогательных хуков.
type mockRepo struct {
	mu sync.Mutex
	m  map[string]int64

	addCalls int64
	addCh    chan struct{}

	// failFirst заставляет первый AddBatch вернуть ошибку,
	// чтобы проверить rollback логики в сервисе.
	failFirst atomic.Bool
}

func newMockRepo() *mockRepo {
	return &mockRepo{m: make(map[string]int64), addCh: make(chan struct{}, 64)}
}

func (r *mockRepo) AddBatch(_ context.Context, deltas map[string]int64) error {
	if r.failFirst.CompareAndSwap(true, false) {
		return errors.New("temporary error")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for id, delta := range deltas {
		r.m[id] += delta
	}
	atomic.AddInt64(&r.addCalls, 1)

	// Сигнализируем тестам, что AddBatch был вызван.
	select {
	case r.addCh <- struct{}{}:
	default:
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

// Проверяем, что Get учитывает pending-дельту до flush.
func TestIncrAndGet_PendingIncluded(t *testing.T) {
	repo := newMockRepo()
	svc, err := New(repo, Config{Shards: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	svc.Incr("video-1", 3)
	svc.Incr("video-1", 4)

	got, err := svc.Get(context.Background(), "video-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
}

// Проверяем, что flush пишет в repo и очищает pending-состояние.
func TestFlush_WritesToRepoAndClearsPending(t *testing.T) {
	repo := newMockRepo()
	svc, err := New(repo, Config{Shards: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	for i := 0; i < 10; i++ {
		svc.Incr("a", 1)
		svc.Incr("b", 2)
	}
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	a, _ := repo.Get(context.Background(), "a")
	b, _ := repo.Get(context.Background(), "b")
	if a != 10 || b != 20 {
		t.Fatalf("unexpected repo state: a=%d b=%d", a, b)
	}

	calls := repo.addCount()
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.addCount() != calls {
		t.Fatalf("empty flush should not call AddBatch")
	}
}

// Проверяем, что при ошибке AddBatch данные не теряются
// и при следующем успешном flush записываются ровно один раз.
func TestFlush_ErrorDoesNotLoseData(t *testing.T) {
	repo := newMockRepo()
	repo.failFirst.Store(true)

	svc, err := New(repo, Config{Shards: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	svc.Incr("x", 5)
	svc.Incr("x", 2)

	if err := svc.Flush(context.Background()); err == nil {
		t.Fatal("expected flush error")
	}
	if got, _ := repo.Get(context.Background(), "x"); got != 0 {
		t.Fatalf("repo should stay unchanged, got %d", got)
	}

	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := repo.Get(context.Background(), "x"); got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
}

// Нагрузочный сценарий: много goroutine одновременно инкрементят
// один общий id и набор разных id.
func TestConcurrentIncr(t *testing.T) {
	repo := newMockRepo()
	svc, err := New(repo, Config{Shards: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	const goroutines = 24
	const loops = 2000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				svc.Incr("shared", 1)
				svc.Incr("id-"+string(rune('a'+(g%8))), 1)
			}
		}()
	}
	wg.Wait()

	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	shared, _ := repo.Get(context.Background(), "shared")
	if shared != goroutines*loops {
		t.Fatalf("shared=%d want=%d", shared, goroutines*loops)
	}

	var sum int64
	for i := 0; i < 8; i++ {
		v, _ := repo.Get(context.Background(), "id-"+string(rune('a'+i)))
		sum += v
	}
	if sum != goroutines*loops {
		t.Fatalf("sum=%d want=%d", sum, goroutines*loops)
	}
}

// Проверяем, что авто-flush действительно срабатывает по таймеру.
func TestAutoFlush(t *testing.T) {
	repo := newMockRepo()
	svc, err := New(repo, Config{Shards: 8, FlushInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	svc.Incr("auto", 9)

	select {
	case <-repo.addCh:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timed out waiting AddBatch")
	}

	got, _ := repo.Get(context.Background(), "auto")
	if got != 9 {
		t.Fatalf("got %d, want 9", got)
	}
}

// Проверяем поведение Close:
// - финальный flush выполняется,
// - повторный Close безопасен,
// - после Close новые записи не отправляются,
// - ручной Flush возвращает ErrClosed.
func TestClose_FlushesAndStops(t *testing.T) {
	repo := newMockRepo()
	svc, err := New(repo, Config{Shards: 8, FlushInterval: 15 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	svc.Incr("z", 4)
	if err := svc.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	got, _ := repo.Get(context.Background(), "z")
	if got != 4 {
		t.Fatalf("got %d, want 4", got)
	}

	before := repo.addCount()
	svc.Incr("z", 100)
	time.Sleep(60 * time.Millisecond)
	after := repo.addCount()
	if after != before {
		t.Fatalf("expected no AddBatch calls after close, before=%d after=%d", before, after)
	}

	if err := svc.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
