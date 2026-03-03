package interview

import (
	"context"
	"errors"
	"sync"
	"time"
)

type CircuitBreaker interface {
	Execute(ctx context.Context, fn func(context.Context) error) error
}

var errCircuitOpen = errors.New("circuit is open")

type cbState int

const (
	stateClosed cbState = iota
	stateOpen
	stateHalfOpen
)

type circuitBreaker struct {
	mu sync.Mutex

	state        cbState
	failures     int
	maxFailures  int
	resetTimeout time.Duration

	openedAt          time.Time
	halfOpenExecuting bool
}

func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) CircuitBreaker {
	if maxFailures <= 0 {
		maxFailures = 1
	}
	if resetTimeout < 0 {
		resetTimeout = 0
	}
	return &circuitBreaker{maxFailures: maxFailures, resetTimeout: resetTimeout, state: stateClosed}
}

func (c *circuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}

	now := time.Now()

	c.mu.Lock()
	switch c.state {
	case stateOpen:
		if now.Sub(c.openedAt) >= c.resetTimeout {
			c.state = stateHalfOpen
			c.halfOpenExecuting = false
		} else {
			c.mu.Unlock()
			return errCircuitOpen
		}
	}

	if c.state == stateHalfOpen {
		if c.halfOpenExecuting {
			c.mu.Unlock()
			return errCircuitOpen
		}
		c.halfOpenExecuting = true
	}
	c.mu.Unlock()

	err := fn(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case stateClosed:
		if err != nil {
			c.failures++
			if c.failures >= c.maxFailures {
				c.state = stateOpen
				c.openedAt = time.Now()
			}
		} else {
			c.failures = 0
		}
	case stateHalfOpen:
		c.halfOpenExecuting = false
		if err != nil {
			c.state = stateOpen
			c.openedAt = time.Now()
			c.failures = 0
		} else {
			c.state = stateClosed
			c.failures = 0
		}
	}

	return err
}
