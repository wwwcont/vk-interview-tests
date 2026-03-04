package main

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const requestTimeout = 100 * time.Millisecond

type BackendState struct {
	id      string
	healthy bool
	delay   time.Duration
	fail    bool

	inflight atomic.Int64
	fails    atomic.Int64
}

type BackendSnapshot struct {
	ID       string `json:"id"`
	Healthy  bool   `json:"healthy"`
	Inflight int64  `json:"inflight"`
	Fails    int64  `json:"fails"`
}

type Balancer struct {
	mu       sync.RWMutex
	backends map[string]*BackendState
}

func NewBalancer() *Balancer {
	return &Balancer{backends: map[string]*BackendState{}}
}

func (b *Balancer) UpsertBackend(id string, healthy bool, delayMS int, fail bool) {
	delay := time.Duration(delayMS) * time.Millisecond
	b.mu.Lock()
	defer b.mu.Unlock()
	be, ok := b.backends[id]
	if !ok {
		be = &BackendState{id: id}
		b.backends[id] = be
	}
	be.healthy = healthy
	be.delay = delay
	be.fail = fail
}

func (b *Balancer) SetHealth(id string, healthy bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	be, ok := b.backends[id]
	if !ok {
		return false
	}
	be.healthy = healthy
	return true
}

func (b *Balancer) Pick(excludeID string) (*BackendState, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var best *BackendState
	for _, be := range b.backends {
		if !be.healthy || be.id == excludeID {
			continue
		}
		if best == nil || be.inflight.Load() < best.inflight.Load() {
			best = be
		}
	}
	if best == nil {
		return nil, false
	}
	best.inflight.Add(1)
	return best, true
}

func (b *Balancer) DoRequest(ctx context.Context) (backendID string, ok bool) {
	first, found := b.Pick("")
	if !found {
		return "", false
	}
	if b.run(ctx, first) == nil {
		return first.id, true
	}

	second, found := b.Pick(first.id)
	if !found {
		return first.id, false
	}
	if b.run(ctx, second) == nil {
		return second.id, true
	}
	return second.id, false
}

func (b *Balancer) run(parent context.Context, be *BackendState) error {
	defer be.inflight.Add(-1)
	ctx, cancel := context.WithTimeout(parent, requestTimeout)
	defer cancel()

	select {
	case <-time.After(be.delay):
		if be.fail {
			be.fails.Add(1)
			return errors.New("backend failed")
		}
		return nil
	case <-ctx.Done():
		be.fails.Add(1)
		return ctx.Err()
	}
}

func (b *Balancer) Snapshot() []BackendSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]BackendSnapshot, 0, len(b.backends))
	for _, be := range b.backends {
		out = append(out, BackendSnapshot{
			ID:       be.id,
			Healthy:  be.healthy,
			Inflight: be.inflight.Load(),
			Fails:    be.fails.Load(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
