package app

import (
	"math"
	"sync"
	"time"

	"vk-interview-tests/subprojects/task1_balancer/domain"
)

/*
DDD-версия задачи 1.
Домен определяет интерфейсы, ошибки и дефолтные коэффициенты.
Application-сервис реализует стратегию least-load + latency EMA + error penalty.
*/

type stat struct {
	inflight               int64
	latencyEMA, errPenalty float64
}

type Service struct {
	mu       sync.Mutex
	backends []domain.Backend
	stats    map[string]*stat

	latencyAlpha    float64
	penaltyOnError  float64
	penaltyRecovery float64
}

type picked struct {
	once sync.Once
	s    *Service
	id   string
}

func New(backends []domain.Backend) *Service {
	s := &Service{
		stats:           map[string]*stat{},
		latencyAlpha:    domain.DefaultLatencyAlpha,
		penaltyOnError:  domain.DefaultPenaltyOnError,
		penaltyRecovery: domain.DefaultPenaltyRecovery,
	}
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
		return nil, nil, domain.ErrNoHealthyBackend
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
		st.latencyEMA = s.latencyAlpha*l + (1-s.latencyAlpha)*st.latencyEMA
	}
	if err != nil {
		st.errPenalty += s.penaltyOnError
	} else {
		st.errPenalty = math.Max(0, st.errPenalty-s.penaltyRecovery)
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
