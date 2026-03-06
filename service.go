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

type Repo interface {
	AddBatch(ctx context.Context, deltas map[string]int64) error
	Get(ctx context.Context, id string) (int64, error)
}

type Service interface {
	Incr(id string, delta int64)
	Get(ctx context.Context, id string) (int64, error)
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

var ErrClosed = errors.New("service closed")

type Config struct {
	Shards        int
	FlushInterval time.Duration
}

type shard struct {
	mu sync.Mutex
	m  map[string]int64
}

type CounterService struct {
	repo   Repo
	shards []shard

	flushMu sync.Mutex

	closed atomic.Bool
	once   sync.Once

	stopCh chan struct{}
	doneCh chan struct{}
}

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
		close(s.doneCh)
	}

	return s, nil
}

// Incr ignores delta <= 0 and does nothing for closed service.
func (s *CounterService) Incr(id string, delta int64) {
	if delta <= 0 || id == "" || s.closed.Load() {
		return
	}
	sh := &s.shards[s.shardIndex(id)]
	sh.mu.Lock()
	sh.m[id] += delta
	sh.mu.Unlock()
}

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

func (s *CounterService) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.flush(ctx)
}

func (s *CounterService) flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	snapshots := make([]map[string]int64, len(s.shards))
	batch := make(map[string]int64)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if len(sh.m) > 0 {
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

func (s *CounterService) runAutoFlush(interval time.Duration) {
	defer close(s.doneCh)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = s.flush(context.Background())
		case <-s.stopCh:
			return
		}
	}
}

func (s *CounterService) shardIndex(id string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return h.Sum32() % uint32(len(s.shards))
}
