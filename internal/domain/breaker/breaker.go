// Package breaker реализует ПРИМЕР маленького, но практичного Circuit Breaker.
//
// Технические принципы реализации:
// - потокобезопасность через mutex;
// - никаких фоновых воркеров/тикеров;
// - lock не держим во время выполнения пользовательской функции;
// - есть настраиваемая классификация ошибок (что считать failure).

// Правило: один breaker на одну “зависимость” (обычно на host/cluster), а не “на весь сервис”.
//Обычно считают failure: timeout, сетевые ошибки, connection reset, refused, HTTP 5xx / gRPC Unavailable
//“много 429/503”
//
//Обычно НЕ считают failure: context.Canceled (клиент отменил запрос), бизнес-ошибки типа “not found”, “validation failed”
//HTTP 4xx

// Открывают обычно по error rate с скользящим окном или порогу p95
// Если breaker открыт: вернуть кэш, вернуть дефолт, деградировать функциональность
// Bulkhead - Это уже “сосед” breaker’а: ограничение параллелизма на зависимость.

package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

var (
	ErrBreakerOpen   = errors.New("circuit breaker is open")
	ErrTooManyProbes = errors.New("too many half-open probes")
	ErrInvalidConfig = errors.New("invalid breaker config")
)

// IsFailure позволяет переопределить правило: какая ошибка влияет на брекер.
type Config struct {
	MaxFailures       int
	ResetTimeout      time.Duration
	HalfOpenMaxProbes int
	IsFailure         func(error) bool
}

// Snapshot — лёгкий снимок состояния для API/логов.
type Snapshot struct {
	State    State `json:"state"`
	Failures int   `json:"failures"`
}

// CircuitBreaker хранит состояние автомата и синхронизацию.
type CircuitBreaker struct {
	mu             sync.Mutex
	state          State
	failures       int
	openedAt       time.Time
	inFlightProbes int
	cfg            Config
	now            func() time.Time
}

func New(cfg Config) (*CircuitBreaker, error) {
	if cfg.MaxFailures <= 0 || cfg.ResetTimeout <= 0 || cfg.HalfOpenMaxProbes <= 0 {
		return nil, ErrInvalidConfig
	}
	return &CircuitBreaker{state: StateClosed, cfg: cfg, now: time.Now}, nil
}

func (b *CircuitBreaker) UpdateConfig(cfg Config) error {
	if cfg.MaxFailures <= 0 || cfg.ResetTimeout <= 0 || cfg.HalfOpenMaxProbes <= 0 {
		return ErrInvalidConfig
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg = cfg
	return nil
}

// State возвращает текущее состояние и число подряд ошибок.
// Здесь же лениво применяем переход OPEN -> HALF_OPEN по времени.
func (b *CircuitBreaker) State() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lazyTransitionLocked(b.now())
	return Snapshot{State: b.state, Failures: b.failures}
}

func defaultIsFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

// Execute запускает защищаемую функцию через брекер.
// Важный момент: mutex отпускается до вызова fn(ctx), чтобы не блокировать систему.
func (b *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	now := b.now()
	probe := false
	classifier := defaultIsFailure

	b.mu.Lock()
	b.lazyTransitionLocked(now)
	execState := b.state
	if b.cfg.IsFailure != nil {
		classifier = b.cfg.IsFailure
	}

	switch execState {
	case StateOpen:
		b.mu.Unlock()
		return ErrBreakerOpen
	case StateHalfOpen:
		if b.inFlightProbes >= b.cfg.HalfOpenMaxProbes {
			b.mu.Unlock()
			return ErrTooManyProbes
		}
		b.inFlightProbes++
		probe = true
	}
	b.mu.Unlock()

	err := fn(ctx)
	isFailure := classifier(err)

	b.mu.Lock()
	defer b.mu.Unlock()

	if probe {
		b.inFlightProbes--
	}
	if err != nil && !isFailure {
		return err
	}

	switch execState {
	case StateClosed:
		if err == nil {
			if b.state == StateClosed {
				b.failures = 0
			}
			return nil
		} else if b.state == StateClosed {
			b.failures++
			if b.failures >= b.cfg.MaxFailures {
				b.toOpenLocked(b.now())
			}
		}
	case StateHalfOpen:
		if err == nil {
			if b.state != StateOpen {
				b.toClosedLocked()
			}
			return nil
		} else {
			b.toOpenLocked(b.now())
		}
	}

	return err
}

// lazyTransitionLocked переводит OPEN в HALF_OPEN,
// если прошло достаточно времени с момента открытия.
func (b *CircuitBreaker) lazyTransitionLocked(now time.Time) {
	if b.state == StateOpen && now.Sub(b.openedAt) >= b.cfg.ResetTimeout {
		b.state = StateHalfOpen
	}
}

func (b *CircuitBreaker) toOpenLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
}

func (b *CircuitBreaker) toClosedLocked() {
	b.state = StateClosed
	b.failures = 0
	b.inFlightProbes = 0
}
