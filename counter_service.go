package interview

import (
	"context"
	"sync"
)

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
	s.mu.Lock()
	batch := s.pending
	s.pending = make(map[string]int64)
	s.mu.Unlock()

	for id, delta := range batch {
		if delta == 0 {
			continue
		}
		if err := s.repo.Add(ctx, id, delta); err != nil {
			s.mu.Lock()
			s.pending[id] += delta
			for k, v := range batch {
				if k == id {
					continue
				}
				s.pending[k] += v
			}
			s.mu.Unlock()
			return err
		}
	}

	return nil
}
