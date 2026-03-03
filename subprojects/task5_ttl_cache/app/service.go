package app

import (
	"container/list"
	"context"
	"sync"
	"time"

	"vk-interview-tests/subprojects/task5_ttl_cache/domain"
)

/*
DDD-версия задачи 5.
Domain держит контракт и конфиг, app реализует LRU+TTL+singleflight и stale-on-error.
*/

type entry struct {
	key  string
	v    any
	exp  time.Time
	elem *list.Element
}
type call struct {
	done chan struct{}
	v    any
	err  error
}

type Service struct {
	mu    sync.Mutex
	cfg   domain.Config
	items map[string]*entry
	lru   *list.List
	in    map[string]*call
}

func New(cfg domain.Config) *Service {
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 1
	}
	if cfg.StaleGrace <= 0 {
		cfg.StaleGrace = domain.DefaultStaleGrace
	}
	return &Service{cfg: cfg, items: map[string]*entry{}, lru: list.New(), in: map[string]*call{}}
}

func (s *Service) Get(key string) (any, bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[key]
	if !ok {
		return nil, false
	}
	if now.After(e.exp) {
		s.remove(e)
		return nil, false
	}
	s.lru.MoveToFront(e.elem)
	return e.v, true
}
func (s *Service) Set(key string, value any, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(key, value, time.Now().Add(ttl))
}

func (s *Service) GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	var stale any
	var staleOK bool
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		if now.Before(e.exp) {
			s.lru.MoveToFront(e.elem)
			v := e.v
			s.mu.Unlock()
			return v, nil
		}
		if now.Before(e.exp.Add(s.cfg.StaleGrace)) {
			stale, staleOK = e.v, true
		} else {
			s.remove(e)
		}
	}
	s.mu.Unlock()
	c, leader := s.getCall(key)
	if !leader {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return c.v, c.err
		}
	}
	v, err := loader(ctx)
	s.mu.Lock()
	if err == nil {
		s.set(key, v, time.Now().Add(ttl))
	}
	delete(s.in, key)
	c.v, c.err = v, err
	close(c.done)
	s.mu.Unlock()
	if err != nil && staleOK {
		return stale, nil
	}
	return v, err
}

func (s *Service) getCall(key string) (*call, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.in[key]; ok {
		return c, false
	}
	c := &call{done: make(chan struct{})}
	s.in[key] = c
	return c, true
}
func (s *Service) set(k string, v any, exp time.Time) {
	if e, ok := s.items[k]; ok {
		e.v = v
		e.exp = exp
		s.lru.MoveToFront(e.elem)
		return
	}
	el := s.lru.PushFront(k)
	s.items[k] = &entry{key: k, v: v, exp: exp, elem: el}
	for len(s.items) > s.cfg.MaxEntries {
		bk := s.lru.Back().Value.(string)
		s.remove(s.items[bk])
	}
}
func (s *Service) remove(e *entry) {
	if e == nil {
		return
	}
	delete(s.items, e.key)
	s.lru.Remove(e.elem)
}
