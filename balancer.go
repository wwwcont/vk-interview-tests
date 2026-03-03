package interview

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

type Backend interface {
	ID() string
	Do(ctx context.Context, req Request) (Response, error)
	Healthy() bool
}

type Request struct{}
type Response struct{}

type Picked interface {
	Done(err error, latency time.Duration)
}

type Balancer interface {
	Pick() (Backend, Picked, error)
	Update(backends []Backend)
}

var errNoHealthyBackend = errors.New("no healthy backend")

const latencyAlpha = 0.2

type backendStat struct {
	inflight   int64
	latencyEMA float64
}

type leastLoadBalancer struct {
	mu       sync.Mutex
	backends []Backend
	stats    map[string]*backendStat
}

type pickedBackend struct {
	once sync.Once
	b    *leastLoadBalancer
	id   string
}

func NewLeastLoadBalancer(backends []Backend) Balancer {
	lb := &leastLoadBalancer{stats: make(map[string]*backendStat)}
	lb.Update(backends)
	return lb
}

func (b *leastLoadBalancer) Pick() (Backend, Picked, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var chosen Backend
	var chosenScore float64

	for _, be := range b.backends {
		if !be.Healthy() {
			continue
		}
		st := b.getOrCreateStat(be.ID())
		score := float64(st.inflight) + st.latencyEMA
		if chosen == nil || score < chosenScore {
			chosen = be
			chosenScore = score
		}
	}

	if chosen == nil {
		return nil, nil, errNoHealthyBackend
	}

	st := b.getOrCreateStat(chosen.ID())
	st.inflight++
	return chosen, &pickedBackend{b: b, id: chosen.ID()}, nil
}

func (b *leastLoadBalancer) Update(backends []Backend) {
	cloned := make([]Backend, len(backends))
	copy(cloned, backends)

	b.mu.Lock()
	defer b.mu.Unlock()

	nextStats := make(map[string]*backendStat, len(cloned))
	for _, be := range cloned {
		if old, ok := b.stats[be.ID()]; ok {
			nextStats[be.ID()] = old
			continue
		}
		nextStats[be.ID()] = &backendStat{}
	}

	b.backends = cloned
	b.stats = nextStats
}

func (b *leastLoadBalancer) done(id string, latency time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.stats[id]
	if !ok {
		return
	}
	if st.inflight > 0 {
		st.inflight--
	}

	latencyMs := math.Max(0, float64(latency.Milliseconds()))
	if st.latencyEMA == 0 {
		st.latencyEMA = latencyMs
		return
	}
	st.latencyEMA = latencyAlpha*latencyMs + (1-latencyAlpha)*st.latencyEMA
}

func (p *pickedBackend) Done(_ error, latency time.Duration) {
	p.once.Do(func() {
		p.b.done(p.id, latency)
	})
}

func (b *leastLoadBalancer) getOrCreateStat(id string) *backendStat {
	if st, ok := b.stats[id]; ok {
		return st
	}
	st := &backendStat{}
	b.stats[id] = st
	return st
}
