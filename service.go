package counter

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// Repo описывает постоянное хранилище счетчиков.
// Сервис пишет в него только батчами через AddBatch.
type Repo interface {
	// AddBatch атомарно применяет приращения по id: counts[id] += delta.
	AddBatch(ctx context.Context, deltas map[string]int64) error
	// Get возвращает уже сохраненное значение из хранилища.
	Get(ctx context.Context, id string) (int64, error)
}

// Service — публичный интерфейс счетчика просмотров.
type Service interface {
	// Incr увеличивает счетчик только в памяти (без I/O).
	Incr(id string, delta int64)
	// Get возвращает сохраненное значение + еще не сброшенный in-memory хвост.
	Get(ctx context.Context, id string) (int64, error)
	// Flush принудительно сбрасывает накопленные дельты в Repo.
	Flush(ctx context.Context) error
	// Close останавливает фоновые процессы и делает финальный Flush.
	Close(ctx context.Context) error
}

// ErrClosed возвращается из Flush после закрытия сервиса.
var ErrClosed = errors.New("service closed")

// Config задает параметры шардирования и авто-сброса.
type Config struct {
	Shards        int
	FlushInterval time.Duration
}

// shard — изолированный сегмент in-memory буфера.
// Идея: каждый Incr блокирует только один шард и не конкурирует с остальными.
type shard struct {
	mu sync.Mutex
	m  map[string]int64
}

// CounterService — потокобезопасный счетчик с буферизацией и батчевым Flush.
type CounterService struct {
	repo   Repo
	shards []shard

	// flushMu сериализует flush-пути (ручной, авто и финальный Close),
	// чтобы одновременно не выполнялось несколько AddBatch.
	flushMu sync.Mutex

	// closed блокирует новые Incr и ручные Flush после Close.
	closed atomic.Bool
	// once гарантирует идемпотентность Close.
	once sync.Once

	// stopCh/doneCh управляют жизненным циклом фоновой goroutine авто-flush.
	stopCh chan struct{}
	doneCh chan struct{}
}

// New создает сервис, валидирует конфиг и (опционально) запускает auto-flush.
func New(repo Repo, cfg Config) (*CounterService, error) {
	if repo == nil {
		return nil, errors.New("repo is nil")
	}
	if cfg.Shards <= 0 {
		return nil, fmt.Errorf("invalid shards: %d", cfg.Shards)
	}

	s := &CounterService{
		repo:   repo,
		shards: make([]shard, cfg.Shards),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[string]int64)
	}

	if cfg.FlushInterval > 0 {
		go s.runAutoFlush(cfg.FlushInterval)
	} else {
		// Если авто-flush выключен, doneCh сразу закрыт,
		// чтобы Close не ждал несуществующую goroutine.
		close(s.doneCh)
	}

	return s, nil
}

// Incr — самый «горячий» путь.
// Решения для скорости:
// 1) никаких обращений к Repo;
// 2) lock только одного шарда;
// 3) невалидные входы (id=="" или delta<=0) тихо игнорируются.
func (s *CounterService) Incr(id string, delta int64) {
	if delta <= 0 || id == "" || s.closed.Load() {
		return
	}

	sh := &s.shards[s.shardIndex(id)]
	sh.mu.Lock()
	sh.m[id] += delta
	sh.mu.Unlock()
}

// Get возвращает eventual-consistent значение:
// persisted(repo) + pending(in-memory).
func (s *CounterService) Get(ctx context.Context, id string) (int64, error) {
	stored, err := s.repo.Get(ctx, id)
	if err != nil {
		return 0, err
	}

	sh := &s.shards[s.shardIndex(id)]
	sh.mu.Lock()
	pending := sh.m[id]
	sh.mu.Unlock()

	return stored + pending, nil
}

// Flush доступен только до Close.
func (s *CounterService) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.flush(ctx)
}

// flush — внутренняя версия без проверки closed.
// Нужна, чтобы Close мог выполнить финальный сброс после выставления closed=true.
func (s *CounterService) flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	// snapshots хранит «снятые» карты по шардам.
	// batch — агрегированная карта для одного вызова Repo.AddBatch.
	snapshots := make([]map[string]int64, len(s.shards))
	batch := make(map[string]int64)

	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if len(sh.m) > 0 {
			// Snapshot+swap: забираем текущую карту, а в шард ставим новую пустую.
			// Важно: после unlock Incr уже пишет в новую карту и не блокируется I/O.
			snapshots[i] = sh.m
			sh.m = make(map[string]int64)
		}
		sh.mu.Unlock()

		for id, delta := range snapshots[i] {
			batch[id] += delta
		}
	}

	if len(batch) == 0 {
		return nil
	}

	if err := s.repo.AddBatch(ctx, batch); err != nil {
		// При ошибке возвращаем все snapshot-дельты обратно в их шарды,
		// чтобы не потерять данные и дать возможность повторить Flush.
		for i, snap := range snapshots {
			if len(snap) == 0 {
				continue
			}
			sh := &s.shards[i]
			sh.mu.Lock()
			for id, delta := range snap {
				sh.m[id] += delta
			}
			sh.mu.Unlock()
		}
		return err
	}

	return nil
}

// Close:
// 1) запрещает новые Incr;
// 2) останавливает auto-flush goroutine (если была);
// 3) выполняет финальный flush.
// Метод идемпотентен.
func (s *CounterService) Close(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.stopCh)
		<-s.doneCh
		err = s.flush(ctx)
	})
	return err
}

// runAutoFlush периодически запускает flush до получения stop-сигнала.
func (s *CounterService) runAutoFlush(interval time.Duration) {
	defer close(s.doneCh)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Ошибку авто-flush намеренно игнорируем:
			// данные откатятся обратно в память и могут быть сброшены позже.
			_ = s.flush(context.Background())
		case <-s.stopCh:
			return
		}
	}
}

// shardIndex детерминированно относит id к конкретному шарду.
func (s *CounterService) shardIndex(id string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return h.Sum32() % uint32(len(s.shards))
}
