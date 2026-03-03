package interview

import (
	"context"
	"errors"
	"sync"
	"time"
)

/*
ЗАДАЧА 4 — Circuit Breaker (Rolling Window + Half-Open Probes)

Постановка:
- Нужно реализовать breaker со состояниями Closed/Open/HalfOpen.
- В Closed копим rolling-статистику ошибок.
- Если доля ошибок в окне >= errorThreshold, переходим в Open.
- В Open запросы отклоняются с ошибкой "breaker open".
- Через resetTimeout из Open допускаем переход в HalfOpen.
- В HalfOpen разрешено не более maxProbeRequests параллельных проб:
  - если проба фейлится (по predicate IsFailure) -> сразу Open,
  - если нужное число проб успешно -> Closed (восстановление).
- Потокобезопасность обязательна.
- Lock нельзя держать во время выполнения fn().

Алгоритм решения:
1) Храним конфиг + состояние:
   - rolling окно bool-значений (failure/success),
   - текущую позицию, число записей и число ошибок,
   - openedAt,
   - счётчики probeInFlight/probeSuccess.
2) beforeExecute под mutex:
   - если Open и resetTimeout прошёл -> HalfOpen,
   - Open -> ошибка "breaker open",
   - HalfOpen -> пускаем не более MaxProbeRequests.
3) Выполняем fn(ctx) без mutex.
4) afterExecute под mutex:
   - Closed: кладём результат в окно, считаем errorRate и при превышении порога -> Open.
   - HalfOpen: обновляем probe-счётчики,
     при failure -> Open,
     при достаточном числе успешных probe и отсутствии inflight -> Closed.
5) State(): возвращает текущее состояние; для Open учитывает, что логически
   после resetTimeout состояние готово к HalfOpen.
*/

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

type Breaker interface {
	Execute(ctx context.Context, fn func(context.Context) error) error
	State() State
}

var errBreakerOpen = errors.New("breaker open")

type IsFailureFunc func(error) bool

type breakerConfig struct {
	WindowSize       int
	ErrorThreshold   float64
	ResetTimeout     time.Duration
	MaxProbeRequests int
	IsFailure        IsFailureFunc
}

type rollingBreaker struct {
	mu sync.Mutex

	cfg breakerConfig

	state    State
	openedAt time.Time

	window      []bool
	windowPos   int
	windowCount int
	failures    int

	probeInFlight int
	probeSuccess  int
}

func NewRollingCircuitBreaker(cfg breakerConfig) Breaker {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 10
	}
	if cfg.ErrorThreshold <= 0 {
		cfg.ErrorThreshold = 0.5
	}
	if cfg.ErrorThreshold > 1 {
		cfg.ErrorThreshold = 1
	}
	if cfg.ResetTimeout < 0 {
		cfg.ResetTimeout = 0
	}
	if cfg.MaxProbeRequests <= 0 {
		cfg.MaxProbeRequests = 1
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(err error) bool { return err != nil }
	}

	return &rollingBreaker{cfg: cfg, state: Closed, window: make([]bool, cfg.WindowSize)}
}

func (b *rollingBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}

	if err := b.beforeExecute(); err != nil {
		return err
	}

	err := fn(ctx)
	b.afterExecute(err)
	return err
}

func (b *rollingBreaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && time.Since(b.openedAt) >= b.cfg.ResetTimeout {
		return HalfOpen
	}
	return b.state
}

func (b *rollingBreaker) beforeExecute() error {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == Open && now.Sub(b.openedAt) >= b.cfg.ResetTimeout {
		b.toHalfOpen()
	}

	switch b.state {
	case Open:
		return errBreakerOpen
	case HalfOpen:
		if b.probeInFlight >= b.cfg.MaxProbeRequests {
			return errBreakerOpen
		}
		b.probeInFlight++
	}

	return nil
}

func (b *rollingBreaker) afterExecute(err error) {
	isFailure := b.cfg.IsFailure(err)

	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		b.pushResult(isFailure)
		if b.windowCount < b.cfg.WindowSize {
			return
		}
		errorRate := float64(b.failures) / float64(b.windowCount)
		if errorRate >= b.cfg.ErrorThreshold {
			b.toOpenLocked()
		}
	case HalfOpen:
		if b.probeInFlight > 0 {
			b.probeInFlight--
		}
		if isFailure {
			b.toOpenLocked()
			return
		}
		b.probeSuccess++
		if b.probeSuccess >= b.cfg.MaxProbeRequests && b.probeInFlight == 0 {
			b.toClosedLocked()
		}
	}
}

func (b *rollingBreaker) pushResult(failed bool) {
	if b.windowCount == b.cfg.WindowSize {
		if b.window[b.windowPos] {
			b.failures--
		}
	} else {
		b.windowCount++
	}

	b.window[b.windowPos] = failed
	if failed {
		b.failures++
	}
	b.windowPos = (b.windowPos + 1) % b.cfg.WindowSize
}

func (b *rollingBreaker) toOpenLocked() {
	b.state = Open
	b.openedAt = time.Now()
	b.probeInFlight = 0
	b.probeSuccess = 0
}

func (b *rollingBreaker) toHalfOpen() {
	b.state = HalfOpen
	b.probeInFlight = 0
	b.probeSuccess = 0
}

func (b *rollingBreaker) toClosedLocked() {
	b.state = Closed
	b.window = make([]bool, b.cfg.WindowSize)
	b.windowPos = 0
	b.windowCount = 0
	b.failures = 0
	b.probeInFlight = 0
	b.probeSuccess = 0
}
