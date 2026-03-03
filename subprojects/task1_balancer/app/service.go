package app

import (
	"errors"
	"math"
	"sync"
	"time"

	"vk-interview-tests/subprojects/task1_balancer/domain"
)

/*
Усложнённая задача 1 (в рамках интервью): Least-Load + latency EMA + error penalty.
Постановка: выбрать healthy backend по минимальному score = inflight + latencyEMA + errorPenalty.
Done() уменьшает inflight, обновляет latency EMA и слегка увеличивает penalty на ошибках
(с мягким затуханием на успешных вызовах). Update() атомарно заменяет список backend.
Алгоритм: mutex на краткие критические секции; Pick только выбирает и резервирует inflight,
реальный Do вызывается снаружи; Done idempotent через sync.Once.
*/

const (
	latencyAlpha    = 0.2
	penaltyOnError  = 1.0
	penaltyRecovery = 0.15
)

var ErrNoHealthyBackend = errors.New("no healthy backend")

type stat struct {
	inflight               int64
	latencyEMA, errPenalty float64
}

type Service struct {
	mu       sync.Mutex
	backends []domain.Backend
	stats    map[string]*stat
}

type picked struct {
	once sync.Once
	s    *Service
	id   string
}

func New(backends []domain.Backend) *Service {
	s := &Service{stats: map[string]*stat{}}
	s.Update(backends)
	return s
}

func (s *Service) Pick() (domain.Backend, domain.Picked, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var chosen domain.Backend
	best := math.MaxFloat64
	for _, b := range s.backends {
		if !b.Healthy() {
			continue
		}
		st := s.get(b.ID())
		score := float64(st.inflight) + st.latencyEMA + st.errPenalty
		if chosen == nil || score < best {
			chosen, best = b, score
		}
	}
	if chosen == nil {
		return nil, nil, ErrNoHealthyBackend
	}
	s.get(chosen.ID()).inflight++
	return chosen, &picked{s: s, id: chosen.ID()}, nil
}

func (s *Service) Update(backends []domain.Backend) {
	cp := append([]domain.Backend(nil), backends...)
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := make(map[string]*stat, len(cp))
	for _, b := range cp {
		if old, ok := s.stats[b.ID()]; ok {
			ns[b.ID()] = old
		} else {
			ns[b.ID()] = &stat{}
		}
	}
	s.backends, s.stats = cp, ns
}

func (p *picked) Done(err error, latency time.Duration) {
	p.once.Do(func() { p.s.done(p.id, err, latency) })
}

func (s *Service) done(id string, err error, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.stats[id]
	if !ok {
		return
	}
	if st.inflight > 0 {
		st.inflight--
	}
	l := math.Max(0, float64(latency.Milliseconds()))
	if st.latencyEMA == 0 {
		st.latencyEMA = l
	} else {
		st.latencyEMA = latencyAlpha*l + (1-latencyAlpha)*st.latencyEMA
	}
	if err != nil {
		st.errPenalty += penaltyOnError
	} else {
		st.errPenalty = math.Max(0, st.errPenalty-penaltyRecovery)
	}
}

func (s *Service) get(id string) *stat {
	if st, ok := s.stats[id]; ok {
		return st
	}
	st := &stat{}
	s.stats[id] = st
	return st
}
