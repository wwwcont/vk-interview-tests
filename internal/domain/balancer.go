package domain

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const RequestTimeout = 100 * time.Millisecond

var (
	ErrNoHealthyBackend = errors.New("no healthy backend")
	ErrBackendFailed    = errors.New("backend failed")
)

type Backend struct {
	ID      string
	Healthy bool
	Delay   time.Duration
	Fail    bool

	Inflight atomic.Int64
	Fails    atomic.Int64
}

type Snapshot struct {
	ID       string `json:"id"`
	Healthy  bool   `json:"healthy"`
	Inflight int64  `json:"inflight"`
	Fails    int64  `json:"fails"`
}

type Balancer struct {
	mu       sync.RWMutex
	backends map[string]*Backend
}

func NewBalancer() *Balancer { return &Balancer{backends: map[string]*Backend{}} }

func (b *Balancer) Upsert(id string, healthy bool, delayMS int, fail bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	be, ok := b.backends[id]
	if !ok {
		be = &Backend{ID: id}
		b.backends[id] = be
	}
	be.Healthy = healthy
	be.Delay = time.Duration(delayMS) * time.Millisecond
	be.Fail = fail
}

func (b *Balancer) SetHealth(id string, healthy bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	be, ok := b.backends[id]
	if !ok {
		return false
	}
	be.Healthy = healthy
	return true
}

func (b *Balancer) Pick(excludeID string) (*Backend, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var best *Backend
	for _, be := range b.backends {
		if !be.Healthy || be.ID == excludeID {
			continue
		}
		if best == nil || be.Inflight.Load() < best.Inflight.Load() {
			best = be
		}
	}
	if best == nil {
		return nil, false
	}
	best.Inflight.Add(1)
	return best, true
}

func RunBackend(parent context.Context, be *Backend) error {
	defer be.Inflight.Add(-1)
	ctx, cancel := context.WithTimeout(parent, RequestTimeout)
	defer cancel()
	select {
	case <-time.After(be.Delay):
		if be.Fail {
			be.Fails.Add(1)
			return ErrBackendFailed
		}
		return nil
	case <-ctx.Done():
		be.Fails.Add(1)
		return ctx.Err()
	}
}

func (b *Balancer) Stats() []Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Snapshot, 0, len(b.backends))
	for _, be := range b.backends {
		out = append(out, Snapshot{ID: be.ID, Healthy: be.Healthy, Inflight: be.Inflight.Load(), Fails: be.Fails.Load()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
