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

type Invoker interface {
	Invoke(context.Context) error
}

type SimulatedInvoker struct {
	Delay time.Duration
	Fail  bool
}

func (s SimulatedInvoker) Invoke(ctx context.Context) error {
	select {
	case <-time.After(s.Delay):
		if s.Fail {
			return ErrBackendFailed
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Backend struct {
	ID      string
	Healthy bool

	invoker   atomic.Value // Invoker
	Inflight  atomic.Int64
	Fails     atomic.Int64
	LatencyNS atomic.Int64
}

func NewBackend(id string, healthy bool, invoker Invoker) *Backend {
	b := &Backend{ID: id, Healthy: healthy}
	b.invoker.Store(invoker)
	return b
}

func (b *Backend) SetInvoker(invoker Invoker) { b.invoker.Store(invoker) }
func (b *Backend) Invoker() Invoker           { return b.invoker.Load().(Invoker) }

type Snapshot struct {
	ID        string `json:"id"`
	Healthy   bool   `json:"healthy"`
	Inflight  int64  `json:"inflight"`
	Fails     int64  `json:"fails"`
	LatencyMS int64  `json:"latency_ms"`
}

type Balancer struct {
	mu       sync.RWMutex
	backends map[string]*Backend
}

func NewBalancer() *Balancer { return &Balancer{backends: map[string]*Backend{}} }

func (b *Balancer) Upsert(id string, healthy bool, delayMS int, fail bool) {
	b.UpsertWithInvoker(id, healthy, SimulatedInvoker{Delay: time.Duration(delayMS) * time.Millisecond, Fail: fail})
}

func (b *Balancer) UpsertWithInvoker(id string, healthy bool, invoker Invoker) {
	b.mu.Lock()
	defer b.mu.Unlock()
	be, ok := b.backends[id]
	if !ok {
		b.backends[id] = NewBackend(id, healthy, invoker)
		return
	}
	be.Healthy = healthy
	be.SetInvoker(invoker)
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

func better(a, best *Backend) bool {
	if best == nil {
		return true
	}
	ai, bi := a.Inflight.Load(), best.Inflight.Load()
	if ai != bi {
		return ai < bi
	}
	af, bf := a.Fails.Load(), best.Fails.Load()
	if af != bf {
		return af < bf
	}
	return a.LatencyNS.Load() < best.LatencyNS.Load()
}

func (b *Balancer) Pick(excludeID string) (*Backend, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var best *Backend
	for _, be := range b.backends {
		if !be.Healthy || be.ID == excludeID {
			continue
		}
		if better(be, best) {
			best = be
		}
	}
	if best == nil {
		return nil, false
	}
	best.Inflight.Add(1)
	return best, true
}

func updateAvgLatency(be *Backend, sample int64) {
	for {
		old := be.LatencyNS.Load()
		next := sample
		if old > 0 {
			next = (old + sample) / 2
		}
		if be.LatencyNS.CompareAndSwap(old, next) {
			return
		}
	}
}

func RunBackend(parent context.Context, be *Backend) error {
	defer be.Inflight.Add(-1)
	ctx, cancel := context.WithTimeout(parent, RequestTimeout)
	defer cancel()
	start := time.Now()
	err := be.Invoker().Invoke(ctx)
	updateAvgLatency(be, time.Since(start).Nanoseconds())
	if err != nil {
		be.Fails.Add(1)
	}
	return err
}

func (b *Balancer) Stats() []Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Snapshot, 0, len(b.backends))
	for _, be := range b.backends {
		out = append(out, Snapshot{
			ID:        be.ID,
			Healthy:   be.Healthy,
			Inflight:  be.Inflight.Load(),
			Fails:     be.Fails.Load(),
			LatencyMS: be.LatencyNS.Load() / int64(time.Millisecond),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
