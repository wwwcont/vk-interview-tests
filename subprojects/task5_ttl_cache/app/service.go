package app

import (
	"container/list"
	"context"
	"sync"
	"time"
)

/*
Усложнение задачи 5: TTL cache + LRU limit + singleflight + stale-on-error.
Постановка: cache с ограничением размера и дедупликацией loader по ключу.
Дополнение: если значение истекло, но loader вернул ошибку, можно вернуть stale
(если не старше StaleGrace), что часто практично для деградационных сценариев.
Алгоритм: map+LRU под mutex, inflight map для singleflight, leader/follower схема.
*/

const StaleGrace = 2 * time.Second

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
	max   int
	items map[string]*entry
	lru   *list.List
	in    map[string]*call
}

func New(max int) *Service {
	if max <= 0 {
		max = 1
	}
	return &Service{max: max, items: map[string]*entry{}, lru: list.New(), in: map[string]*call{}}
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
		if now.Before(e.exp.Add(StaleGrace)) {
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
	for len(s.items) > s.max {
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
