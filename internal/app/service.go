package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
	"vk-interview-tests/internal/domain"
)

type CreateCmd struct {
	Type, Priority, IdempotencyKey string
	Payload                        domain.SleepPayload
}

type Pool interface {
	Enqueue(context.Context, *domain.Job) error
	Resize(int) error
	Shutdown(context.Context) ([]string, error)
	IsDown() bool
}

type call struct {
	done chan struct{}
	job  *domain.Job
	err  error
}
type Service struct {
	repo     domain.Repo
	pool     Pool
	mu       sync.Mutex
	inflight map[string]*call
}

func New(repo domain.Repo, pool Pool) *Service {
	return &Service{repo: repo, pool: pool, inflight: map[string]*call{}}
}

func (s *Service) CreateJob(ctx context.Context, c CreateCmd) (*domain.Job, error) {
	if s.pool.IsDown() {
		return nil, domain.ErrShuttingDown
	}
	if c.IdempotencyKey == "" {
		return s.create(ctx, c)
	}
	if j, e := s.repo.ByKey(c.IdempotencyKey); e == nil {
		return j, nil
	}

	s.mu.Lock()
	if in := s.inflight[c.IdempotencyKey]; in != nil {
		s.mu.Unlock()
		<-in.done
		return in.job, in.err
	}
	in := &call{done: make(chan struct{})}
	s.inflight[c.IdempotencyKey] = in
	s.mu.Unlock()
	in.job, in.err = s.create(ctx, c)
	close(in.done)
	s.mu.Lock()
	delete(s.inflight, c.IdempotencyKey)
	s.mu.Unlock()
	return in.job, in.err
}

func (s *Service) create(ctx context.Context, c CreateCmd) (*domain.Job, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	j, e := domain.NewJob(id(), domain.JobType(c.Type), c.Payload, domain.Priority(c.Priority), c.IdempotencyKey)
	if e != nil {
		return nil, e
	}
	s.repo.Save(j)
	if c.IdempotencyKey != "" {
		s.repo.Bind(c.IdempotencyKey, j.ID)
	}
	if e = s.pool.Enqueue(ctx, j); e != nil {
		return nil, e
	}
	return j, nil
}

func (s *Service) GetJob(_ context.Context, id string) (*domain.Job, error) { return s.repo.ByID(id) }
func (s *Service) ResizePool(_ context.Context, n int) error                { return s.pool.Resize(n) }
func (s *Service) Shutdown(ctx context.Context, t time.Duration) ([]string, error) {
	c, x := context.WithTimeout(ctx, t)
	defer x()
	return s.pool.Shutdown(c)
}
func id() string { b := make([]byte, 12); _, _ = rand.Read(b); return hex.EncodeToString(b) }
