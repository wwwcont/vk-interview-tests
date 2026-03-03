package interview

import (
	"context"
	"errors"
	"sync"
)

type Backend interface {
	ID() string
	Do(ctx context.Context, req Request) (Response, error)
	Healthy() bool
}

type Balancer interface {
	Next() (Backend, error)
	Update(backends []Backend)
}

type Request struct{}
type Response struct{}

var errNoHealthyBackends = errors.New("no healthy backends")

type roundRobinBalancer struct {
	mu       sync.Mutex
	backends []Backend
	next     int
}

func NewRoundRobinBalancer(backends []Backend) Balancer {
	b := &roundRobinBalancer{}
	b.Update(backends)
	return b
}

func (b *roundRobinBalancer) Next() (Backend, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(b.backends)
	if n == 0 {
		return nil, errNoHealthyBackends
	}

	start := b.next % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		candidate := b.backends[idx]
		if candidate.Healthy() {
			b.next = (idx + 1) % n
			return candidate, nil
		}
	}

	return nil, errNoHealthyBackends
}

func (b *roundRobinBalancer) Update(backends []Backend) {
	cloned := make([]Backend, len(backends))
	copy(cloned, backends)

	b.mu.Lock()
	b.backends = cloned
	b.next = 0
	b.mu.Unlock()
}
