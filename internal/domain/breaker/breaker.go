// Package breaker implements a very small, interview-friendly Circuit Breaker.
//
// Problem in simple words:
// We call an external dependency (HTTP, DB, RPC). Sometimes it fails for a while.
// If we continue sending every request there, we only make things worse:
// slower responses, bigger queues, and more pressure on broken dependency.
//
// Circuit Breaker solves this by switching between three states:
// 1) CLOSED: allow calls, count failures.
// 2) OPEN: reject calls immediately for some timeout.
// 3) HALF_OPEN: allow only a small number of probe calls to check recovery.
//
// This file keeps the algorithm intentionally compact, but production-like:
// - thread-safe with mutex,
// - lazy time transitions (no background goroutines/tickers),
// - configurable failure classifier,
// - no lock while executing user function.
package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// State is a readable name of current breaker mode.
type State string

const (
	// StateClosed means calls are allowed; failures are counted.
	StateClosed State = "CLOSED"
	// StateOpen means calls are rejected immediately.
	StateOpen State = "OPEN"
	// StateHalfOpen means only limited probe calls are allowed.
	StateHalfOpen State = "HALF_OPEN"
)

var (
	// ErrBreakerOpen is returned when breaker is OPEN.
	ErrBreakerOpen = errors.New("circuit breaker is open")
	// ErrTooManyProbes is returned when HALF_OPEN probe limit is reached.
	ErrTooManyProbes = errors.New("too many half-open probes")
	// ErrInvalidConfig is returned for non-positive config values.
	ErrInvalidConfig = errors.New("invalid breaker config")
)

// Config stores breaker tuning knobs.
// IsFailure lets caller define which errors should affect breaker state.
type Config struct {
	MaxFailures       int
	ResetTimeout      time.Duration
	HalfOpenMaxProbes int
	IsFailure         func(error) bool
}

// Snapshot is a small state view for APIs and logs.
type Snapshot struct {
	State    State `json:"state"`
	Failures int   `json:"failures"`
}

// CircuitBreaker holds mutable state and synchronization primitives.
type CircuitBreaker struct {
	mu             sync.Mutex
	state          State
	failures       int
	openedAt       time.Time
	inFlightProbes int
	cfg            Config
	now            func() time.Time
}

// New creates breaker with CLOSED initial state and validates config.
func New(cfg Config) (*CircuitBreaker, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return &CircuitBreaker{state: StateClosed, cfg: cfg, now: time.Now}, nil
}

// validateConfig checks simple positive limits.
func validateConfig(cfg Config) error {
	if cfg.MaxFailures <= 0 || cfg.ResetTimeout <= 0 || cfg.HalfOpenMaxProbes <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

// UpdateConfig replaces runtime config after validation.
func (b *CircuitBreaker) UpdateConfig(cfg Config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg = cfg
	return nil
}

// State returns current state and failure counter.
// It also performs lazy OPEN -> HALF_OPEN transition if timeout passed.
func (b *CircuitBreaker) State() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lazyTransitionLocked(b.now())
	return Snapshot{State: b.state, Failures: b.failures}
}

// defaultIsFailure defines default classification:
// - nil => not failure
// - context.Canceled => not failure
// - everything else => failure (including DeadlineExceeded)
func defaultIsFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

// Execute runs protected function through breaker algorithm.
// Important concurrency rule: lock is released before fn(ctx) execution.
func (b *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	now := b.now()
	probe := false
	execState := StateClosed
	classifier := defaultIsFailure

	b.mu.Lock()
	b.lazyTransitionLocked(now)
	execState = b.state
	if b.cfg.IsFailure != nil {
		classifier = b.cfg.IsFailure
	}

	switch execState {
	case StateOpen:
		b.mu.Unlock()
		return ErrBreakerOpen
	case StateHalfOpen:
		if b.inFlightProbes >= b.cfg.HalfOpenMaxProbes {
			b.mu.Unlock()
			return ErrTooManyProbes
		}
		b.inFlightProbes++
		probe = true
	}
	b.mu.Unlock()

	err := fn(ctx)
	isFailure := classifier(err)

	b.mu.Lock()
	defer b.mu.Unlock()

	if probe {
		b.inFlightProbes--
	}
	if err != nil && !isFailure {
		return err
	}

	switch execState {
	case StateClosed:
		if err == nil {
			if b.state == StateClosed {
				b.failures = 0
			}
			return nil
		}
		if b.state == StateClosed {
			b.failures++
			if b.failures >= b.cfg.MaxFailures {
				b.toOpenLocked(b.now())
			}
		}
	case StateHalfOpen:
		if err == nil {
			if b.state != StateOpen {
				b.toClosedLocked()
			}
			return nil
		}
		b.toOpenLocked(b.now())
	}

	return err
}

// lazyTransitionLocked moves OPEN to HALF_OPEN after timeout.
func (b *CircuitBreaker) lazyTransitionLocked(now time.Time) {
	if b.state == StateOpen && now.Sub(b.openedAt) >= b.cfg.ResetTimeout {
		b.state = StateHalfOpen
	}
}

// toOpenLocked switches to OPEN and records open timestamp.
func (b *CircuitBreaker) toOpenLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
}

// toClosedLocked switches to CLOSED and clears counters.
func (b *CircuitBreaker) toClosedLocked() {
	b.state = StateClosed
	b.failures = 0
	b.inFlightProbes = 0
}
