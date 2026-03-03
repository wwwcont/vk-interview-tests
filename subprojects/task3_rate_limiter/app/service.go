package app

import (
	"context"
	"sync"
	"time"

	"vk-interview-tests/subprojects/task3_rate_limiter/domain"
)

/*
DDD-версия задачи 3.
Domain задаёт контракт + константу cleanup budget, application реализует механику token bucket.
*/

type bucket struct {
	tok        float64
	last, seen time.Time
}

type Service struct {
	mu            sync.Mutex
	rate, burst   float64
	ttl           time.Duration
	cleanupBudget int
	m             map[string]*bucket
}

func New(rate float64, burst int, ttl time.Duration) *Service {
	if rate < 0 {
		rate = 0
	}
	if burst < 0 {
		burst = 0
	}
	if ttl < 0 {
		ttl = 0
	}
	return &Service{rate: rate, burst: float64(burst), ttl: ttl, cleanupBudget: domain.DefaultCleanupBudget, m: map[string]*bucket{}}
}

func (s *Service) Allow(key string) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanup(now)
	b := s.get(key, now)
	s.refill(b, now)
	b.seen = now
	if b.tok < 1 {
		return false
	}
	b.tok--
	return true
}

func (s *Service) Acquire(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		now := time.Now()
		s.mu.Lock()
		s.cleanup(now)
		b := s.get(key, now)
		s.refill(b, now)
		b.seen = now
		if b.tok >= 1 {
			b.tok--
			s.mu.Unlock()
			return nil
		}
		wait := s.waitDur(b)
		s.mu.Unlock()
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (s *Service) get(key string, now time.Time) *bucket {
	if b, ok := s.m[key]; ok {
		return b
	}
	b := &bucket{tok: s.burst, last: now, seen: now}
	s.m[key] = b
	return b
}
func (s *Service) refill(b *bucket, now time.Time) {
	if s.rate <= 0 {
		b.last = now
		return
	}
	dt := now.Sub(b.last).Seconds()
	if dt <= 0 {
		return
	}
	b.tok += dt * s.rate
	if b.tok > s.burst {
		b.tok = s.burst
	}
	b.last = now
}
func (s *Service) waitDur(b *bucket) time.Duration {
	if s.rate <= 0 {
		return time.Second
	}
	miss := 1 - b.tok
	if miss < 0 {
		miss = 0
	}
	d := time.Duration((miss / s.rate) * float64(time.Second))
	if d <= 0 {
		d = time.Millisecond
	}
	return d
}
func (s *Service) cleanup(now time.Time) {
	if s.ttl <= 0 {
		return
	}
	n := 0
	for k, b := range s.m {
		if now.Sub(b.seen) > s.ttl {
			delete(s.m, k)
			n++
			if n >= s.cleanupBudget {
				break
			}
		}
	}
}
