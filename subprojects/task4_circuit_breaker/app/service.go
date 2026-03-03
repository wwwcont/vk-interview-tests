package app

import (
	"context"
	"sync"
	"time"

	"vk-interview-tests/subprojects/task4_circuit_breaker/domain"
)

/*
DDD-версия задачи 4.
Конфиг/ошибки/тип состояния вынесены в домен. Приложение управляет переходами и rolling-метрикой.
*/

type Service struct {
	mu               sync.Mutex
	cfg              domain.Config
	state            domain.State
	openedAt         time.Time
	win              []bool
	pos, cnt, fail   int
	probeIn, probeOK int
}

func New(cfg domain.Config) *Service {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 10
	}
	if cfg.ErrorThreshold <= 0 {
		cfg.ErrorThreshold = .5
	}
	if cfg.ErrorThreshold > 1 {
		cfg.ErrorThreshold = 1
	}
	if cfg.MaxProbe <= 0 {
		cfg.MaxProbe = 1
	}
	if cfg.MinRequestsToTrip <= 0 {
		cfg.MinRequestsToTrip = cfg.WindowSize
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(err error) bool { return err != nil }
	}
	return &Service{cfg: cfg, state: domain.Closed, win: make([]bool, cfg.WindowSize)}
}

func (s *Service) Execute(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if err := s.before(); err != nil {
		return err
	}
	err := fn(ctx)
	s.after(err)
	return err
}

func (s *Service) State() domain.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == domain.Open && time.Since(s.openedAt) >= s.cfg.ResetTimeout {
		return domain.HalfOpen
	}
	return s.state
}

func (s *Service) before() error {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == domain.Open && now.Sub(s.openedAt) >= s.cfg.ResetTimeout {
		s.toHalf()
	}
	switch s.state {
	case domain.Open:
		return domain.ErrBreakerOpen
	case domain.HalfOpen:
		if s.probeIn >= s.cfg.MaxProbe {
			return domain.ErrBreakerOpen
		}
		s.probeIn++
	}
	return nil
}

func (s *Service) after(err error) {
	failed := s.cfg.IsFailure(err)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case domain.Closed:
		s.push(failed)
		if s.cnt < s.cfg.MinRequestsToTrip {
			return
		}
		if float64(s.fail)/float64(s.cnt) >= s.cfg.ErrorThreshold {
			s.toOpen()
		}
	case domain.HalfOpen:
		if s.probeIn > 0 {
			s.probeIn--
		}
		if failed {
			s.toOpen()
			return
		}
		s.probeOK++
		if s.probeOK >= s.cfg.MaxProbe && s.probeIn == 0 {
			s.toClosed()
		}
	}
}

func (s *Service) push(f bool) {
	if s.cnt == len(s.win) {
		if s.win[s.pos] {
			s.fail--
		}
	} else {
		s.cnt++
	}
	s.win[s.pos] = f
	if f {
		s.fail++
	}
	s.pos = (s.pos + 1) % len(s.win)
}
func (s *Service) toOpen() {
	s.state = domain.Open
	s.openedAt = time.Now()
	s.probeIn, s.probeOK = 0, 0
}
func (s *Service) toHalf() { s.state = domain.HalfOpen; s.probeIn, s.probeOK = 0, 0 }
func (s *Service) toClosed() {
	s.state = domain.Closed
	s.win = make([]bool, len(s.win))
	s.pos, s.cnt, s.fail = 0, 0, 0
	s.probeIn, s.probeOK = 0, 0
}
