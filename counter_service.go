package interview

import (
	"context"
	"sort"
	"sync"
)

// Задача 1: реализовать CounterService c буферизацией инкрементов в памяти,
// потокобезопасным доступом и батчевым Flush без удержания lock во время I/O.
// Get должен возвращать сумму значения из репозитория и ещё не сброшенного pending delta.
type CounterRepo interface {
	Add(ctx context.Context, id string, delta int64) error
	Get(ctx context.Context, id string) (int64, error)
}

type CounterService interface {
	Incr(id string, delta int64)
	Get(ctx context.Context, id string) (int64, error)
	Flush(ctx context.Context) error
}

type counterService struct {
	repo CounterRepo

	mu      sync.Mutex
	pending map[string]int64
}

func NewCounterService(repo CounterRepo) CounterService {
	return &counterService{
		repo:    repo,
		pending: make(map[string]int64),
	}
}

func (s *counterService) Incr(id string, delta int64) {
	s.mu.Lock()
	s.pending[id] += delta
	s.mu.Unlock()
}

func (s *counterService) Get(ctx context.Context, id string) (int64, error) {
	base, err := s.repo.Get(ctx, id)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	delta := s.pending[id]
	s.mu.Unlock()

	return base + delta, nil
}

func (s *counterService) Flush(ctx context.Context) error {
	batch := s.takeSnapshot()
	if len(batch) == 0 {
		return nil
	}

	keys := sortedKeys(batch)
	for i, id := range keys {
		delta := batch[id]
		if delta == 0 {
			continue
		}

		if err := s.repo.Add(ctx, id, delta); err != nil {
			s.restoreNotFlushed(keys[i:], batch)
			return err
		}
	}

	return nil
}

func (s *counterService) takeSnapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return nil
	}

	batch := s.pending
	s.pending = make(map[string]int64)
	return batch
}

func (s *counterService) restoreNotFlushed(ids []string, batch map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		s.pending[id] += batch[id]
	}
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
