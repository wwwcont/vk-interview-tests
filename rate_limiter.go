package interview

import (
	"sync"
	"time"
)

type Limiter interface {
	Allow() bool
}

type tokenBucketLimiter struct {
	mu sync.Mutex

	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func NewTokenBucketLimiter(rate float64, burst int) Limiter {
	if burst < 0 {
		burst = 0
	}
	if rate < 0 {
		rate = 0
	}

	now := time.Now()
	return &tokenBucketLimiter{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   now,
	}
}

func (l *tokenBucketLimiter) Allow() bool {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = now
	}

	if l.tokens < 1 {
		return false
	}

	l.tokens--
	return true
}
