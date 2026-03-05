package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

var (
	ErrBreakerOpen   = errors.New("circuit breaker is open")
	ErrInvalidConfig = errors.New("invalid breaker config")
)

type Config struct {
	MaxFailures       int
	ResetTimeout      time.Duration
	HalfOpenMaxProbes int
}

type Snapshot struct {
	State    State `json:"state"`
	Failures int   `json:"failures"`
}

type CircuitBreaker struct {
	mu             sync.Mutex
	state          State
	failures       int
	openedAt       time.Time
	inFlightProbes int
	cfg            Config
	now            func() time.Time
}

func NewCircuitBreaker(cfg Config) (*CircuitBreaker, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return &CircuitBreaker{state: StateClosed, cfg: cfg, now: time.Now}, nil
}

func validateConfig(cfg Config) error {
	if cfg.MaxFailures <= 0 || cfg.ResetTimeout <= 0 || cfg.HalfOpenMaxProbes <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

func (b *CircuitBreaker) UpdateConfig(cfg Config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg = cfg
	return nil
}

func (b *CircuitBreaker) State() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lazyTransitionLocked(b.now())
	return Snapshot{State: b.state, Failures: b.failures}
}

func (b *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	now := b.now()
	probe := false

	b.mu.Lock()
	b.lazyTransitionLocked(now)
	switch b.state {
	case StateOpen:
		b.mu.Unlock()
		return ErrBreakerOpen
	case StateHalfOpen:
		if b.inFlightProbes >= b.cfg.HalfOpenMaxProbes {
			b.mu.Unlock()
			return ErrBreakerOpen
		}
		b.inFlightProbes++
		probe = true
	}
	b.mu.Unlock()

	err := fn(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.inFlightProbes--
	}

	if err == nil {
		if b.state == StateClosed {
			b.failures = 0
		} else if b.state == StateHalfOpen {
			b.toClosedLocked()
		}
		return nil
	}

	switch b.state {
	case StateClosed:
		b.failures++
		if b.failures >= b.cfg.MaxFailures {
			b.toOpenLocked(b.now())
		}
	case StateHalfOpen:
		b.toOpenLocked(b.now())
	}

	return err
}

func (b *CircuitBreaker) lazyTransitionLocked(now time.Time) {
	if b.state == StateOpen && now.Sub(b.openedAt) >= b.cfg.ResetTimeout {
		b.state = StateHalfOpen
	}
}

func (b *CircuitBreaker) toOpenLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
	b.inFlightProbes = 0
}

func (b *CircuitBreaker) toClosedLocked() {
	b.state = StateClosed
	b.failures = 0
	b.inFlightProbes = 0
}
