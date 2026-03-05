package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Key   string
	Value int64
	Ts    time.Time
}

type Aggregate struct {
	WindowStart time.Time
	Key         string
	Sum         int64
}

type Repo interface {
	SaveBatch(ctx context.Context, aggs []Aggregate) error
}

var ErrQueueFull = errors.New("queue full")
var ErrClosed = errors.New("aggregator closed")

type Aggregator interface {
	Add(e Event) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

type Config struct {
	WindowSize time.Duration
	QueueCap   int
}

type Service struct {
	repo       Repo
	windowSize time.Duration
	queue      chan request
	closed     atomic.Bool
	closeOnce  sync.Once
	done       chan struct{}
}

type request struct {
	event *Event
	flush *flushReq
	stop  chan struct{}
}

type flushReq struct {
	ctx  context.Context
	errC chan error
}

type aggKey struct {
	windowStart time.Time
	key         string
}

func New(repo Repo, cfg Config) (*Service, error) {
	if repo == nil {
		return nil, errors.New("repo is nil")
	}
	if cfg.WindowSize <= 0 {
		return nil, errors.New("window size must be > 0")
	}
	if cfg.QueueCap <= 0 {
		return nil, errors.New("queue cap must be > 0")
	}
	s := &Service{
		repo:       repo,
		windowSize: cfg.WindowSize,
		queue:      make(chan request, cfg.QueueCap),
		done:       make(chan struct{}),
	}
	go s.worker()
	return s, nil
}

func (s *Service) Add(e Event) error {
	if s.closed.Load() {
		return ErrClosed
	}
	select {
	case s.queue <- request{event: &e}:
		return nil
	default:
		if s.closed.Load() {
			return ErrClosed
		}
		return ErrQueueFull
	}
}

func (s *Service) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resp := make(chan error, 1)
	req := request{flush: &flushReq{ctx: ctx, errC: resp}}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	case s.queue <- req:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-resp:
		return err
	}
}

func (s *Service) Close(ctx context.Context) error {
	flushErr := error(nil)
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		flushErr = s.Flush(ctx)
		stop := make(chan struct{})
		s.queue <- request{stop: stop}
		<-stop
		<-s.done
	})
	return flushErr
}

func (s *Service) worker() {
	defer close(s.done)
	state := make(map[aggKey]int64)
	for {
		req := <-s.queue
		switch {
		case req.event != nil:
			e := req.event
			k := aggKey{windowStart: e.Ts.Truncate(s.windowSize), key: e.Key}
			state[k] += e.Value
		case req.flush != nil:
			err := s.flushState(req.flush.ctx, state)
			req.flush.errC <- err
		case req.stop != nil:
			close(req.stop)
			return
		default:
			panic("invalid request")
		}
	}
}

func (s *Service) flushState(ctx context.Context, state map[aggKey]int64) error {
	if len(state) == 0 {
		return nil
	}
	batch := make([]Aggregate, 0, len(state))
	for k, sum := range state {
		batch = append(batch, Aggregate{WindowStart: k.windowStart, Key: k.key, Sum: sum})
	}
	for k := range state {
		delete(state, k)
	}
	if err := s.repo.SaveBatch(ctx, batch); err != nil {
		for _, a := range batch {
			state[aggKey{windowStart: a.WindowStart, key: a.Key}] += a.Sum
		}
		return fmt.Errorf("save batch: %w", err)
	}
	return nil
}
