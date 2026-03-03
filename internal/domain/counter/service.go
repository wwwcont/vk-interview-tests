package counter

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

// Repo — минимальный контракт хранилища.
// Сервису важно только уметь батчево добавлять дельты и читать текущее значение по id.
type Repo interface {
	AddBatch(ctx context.Context, deltas map[string]int64) error
	Get(ctx context.Context, id string) (int64, error)
}

// Config — простая конфигурация сервиса.
// Shards отвечает за параллелизм в памяти,
// FlushInterval — как часто фоново писать батч в repo,
// FlushTimeout — максимальное время одной записи в repo.
type Config struct {
	Shards        int
	FlushInterval time.Duration
	FlushTimeout  time.Duration
}

// Service — сервисный слой счётчиков.
// Он буферизует инкременты в памяти и периодически отправляет их в хранилище.
type Service struct {
	repo Repo
	cfg  Config

	shards []counterShard

	flushMu sync.Mutex

	stopCh chan struct{}
	doneCh chan struct{}

	closeOnce sync.Once
	closeErr  error
}

type counterShard struct {
	mu     sync.Mutex
	deltas map[string]int64
}

// New создаёт сервис, валидирует конфиг и запускает фоновый flush-loop.
// Идея простая: сразу подготовить шарды и отдельную горутину, чтобы Incr был быстрым.
func New(repo Repo, cfg Config) (*Service, error) {
	if repo == nil {
		return nil, errors.New("repo is nil")
	}
	if cfg.Shards <= 0 {
		return nil, errors.New("shards must be > 0")
	}
	if cfg.FlushInterval <= 0 {
		return nil, errors.New("flush interval must be > 0")
	}
	if cfg.FlushTimeout < 0 {
		return nil, errors.New("flush timeout must be >= 0")
	}

	s := &Service{
		repo:   repo,
		cfg:    cfg,
		shards: make([]counterShard, cfg.Shards),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	for i := range s.shards {
		s.shards[i].deltas = make(map[string]int64)
	}

	go s.flushLoop()

	return s, nil
}

// Incr быстро добавляет дельту в память.
// Мы берём только лок шарда (а не глобальный), поэтому под нагрузкой меньше конкуренции.
func (s *Service) Incr(id string, delta int64) {
	if delta == 0 {
		return
	}
	shard := &s.shards[s.shardIndex(id)]
	shard.mu.Lock()
	shard.deltas[id] += delta
	shard.mu.Unlock()
}

// Get возвращает значение из repo плюс ещё не сброшенные дельты из памяти.
// Это даёт eventual consistency: часть данных уже в repo, часть пока в буфере.
func (s *Service) Get(ctx context.Context, id string) (int64, error) {
	stored, err := s.repo.Get(ctx, id)
	if err != nil {
		return 0, err
	}

	shard := &s.shards[s.shardIndex(id)]
	shard.mu.Lock()
	pending := shard.deltas[id]
	shard.mu.Unlock()

	return stored + pending, nil
}

// Flush потокобезопасно собирает все накопленные дельты и пишет их одним батчем в repo.
// Если запись не удалась, дельты возвращаются обратно в память, чтобы не потерять данные.
func (s *Service) Flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	deltas := s.snapshotAndReset()
	if len(deltas) == 0 {
		return nil
	}

	flushCtx := ctx
	cancel := func() {}
	if s.cfg.FlushTimeout > 0 {
		flushCtx, cancel = context.WithTimeout(ctx, s.cfg.FlushTimeout)
	}
	defer cancel()

	if err := s.repo.AddBatch(flushCtx, deltas); err != nil {
		s.restore(deltas)
		return err
	}

	return nil
}

// Close останавливает фоновый воркер и делает финальный flush.
// Это нужно, чтобы при завершении процесса всё накопленное точно ушло в repo.
func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		close(s.stopCh)
		<-s.doneCh
		s.closeErr = s.Flush(context.Background())
	})
	return s.closeErr
}

// flushLoop по таймеру вызывает Flush в фоне.
// Логика максимально простая: тикер + stop-канал для корректного завершения.
func (s *Service) flushLoop() {
	defer close(s.doneCh)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = s.Flush(context.Background())
		case <-s.stopCh:
			return
		}
	}
}

// snapshotAndReset забирает текущие дельты из всех шардов и обнуляет буфер.
// Так мы готовим батч для записи и сразу освобождаем память под новые инкременты.
func (s *Service) snapshotAndReset() map[string]int64 {
	merged := make(map[string]int64)
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for id, delta := range shard.deltas {
			merged[id] += delta
		}
		shard.deltas = make(map[string]int64)
		shard.mu.Unlock()
	}
	return merged
}

// restore возвращает дельты обратно в шарды после неуспешного flush.
// Это простая защита от потери данных при временной ошибке хранилища.
func (s *Service) restore(deltas map[string]int64) {
	for id, delta := range deltas {
		shard := &s.shards[s.shardIndex(id)]
		shard.mu.Lock()
		shard.deltas[id] += delta
		shard.mu.Unlock()
	}
}

// shardIndex стабильно выбирает shard для id через FNV-хеш.
// Один и тот же id всегда попадает в один shard, что упрощает чтение pending-дельты.
func (s *Service) shardIndex(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(len(s.shards)))
}
