package app

import (
	"context"
	"sync"
	"time"
)

/*
Усложнение задачи 3: per-key token bucket + blocking Acquire + idle TTL + cleanup budget.
Постановка: независимые лимиты по ключам, Allow неблокирующий, Acquire ждёт токен или ctx.Done.
Дополнительно: lazy-cleanup выполняется с ограничением на число удалений за вызов,
чтобы единичный запрос не деградировал при большом числе ключей.
Алгоритм: map[key]bucket под mutex, refill через time.Now, Acquire через timer-loop.
*/

const CleanupBudget = 32

type bucket struct {
	tok        float64
	last, seen time.Time
}

type Service struct {
	mu          sync.Mutex
	rate, burst float64
	ttl         time.Duration
	m           map[string]*bucket
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
	return &Service{rate: rate, burst: float64(burst), ttl: ttl, m: map[string]*bucket{}}
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
			if n >= CleanupBudget {
				break
			}
		}
	}
}
