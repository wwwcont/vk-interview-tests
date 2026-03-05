// Пакет breaker реализует маленький, но практичный Circuit Breaker.
//
// Простыми словами о задаче:
// мы ходим во внешний сервис (HTTP/БД/RPC), и он может временно «болеть».
// Если в этот момент продолжать слать туда все запросы, мы только усугубляем ситуацию:
// задержки растут, очередь растёт, и наш сервис тоже начинает деградировать.
//
// Что делает алгоритм:
// 1) CLOSED — вызовы пропускаем, подряд идущие ошибки считаем.
// 2) OPEN — вызовы сразу отклоняем на время reset timeout.
// 3) HALF_OPEN — после timeout пускаем только ограниченное число «проб».
//
// Идея переходов:
// - в CLOSED при достижении лимита ошибок открываем брекер;
// - в OPEN после таймаута (лениво, на входящем запросе) переходим в HALF_OPEN;
// - в HALF_OPEN успешная проба закрывает брекер, неуспешная снова открывает.
//
// Технические принципы реализации:
// - потокобезопасность через mutex;
// - никаких фоновых воркеров/тикеров;
// - lock не держим во время выполнения пользовательской функции;
// - есть настраиваемая классификация ошибок (что считать failure).
package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// State — текущее состояние автомата брекера в читаемом виде.
type State string

const (
	// StateClosed: штатный режим, вызовы разрешены, ошибки копятся подряд.
	StateClosed State = "CLOSED"
	// StateOpen: защитный режим, вызовы сразу отклоняются.
	StateOpen State = "OPEN"
	// StateHalfOpen: режим проверки восстановления, разрешены только пробы.
	StateHalfOpen State = "HALF_OPEN"
)

var (
	// ErrBreakerOpen возвращается, когда брекер в состоянии OPEN.
	ErrBreakerOpen = errors.New("circuit breaker is open")
	// ErrTooManyProbes возвращается, когда в HALF_OPEN заняты все слоты проб.
	ErrTooManyProbes = errors.New("too many half-open probes")
	// ErrInvalidConfig возвращается при невалидном конфиге.
	ErrInvalidConfig = errors.New("invalid breaker config")
)

// Config задаёт параметры работы брекера.
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

// New создаёт брекер в состоянии CLOSED и валидирует конфиг.
func New(cfg Config) (*CircuitBreaker, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return &CircuitBreaker{state: StateClosed, cfg: cfg, now: time.Now}, nil
}

// validateConfig проверяет базовые ограничения конфига.
func validateConfig(cfg Config) error {
	if cfg.MaxFailures <= 0 || cfg.ResetTimeout <= 0 || cfg.HalfOpenMaxProbes <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

// UpdateConfig обновляет конфиг во время работы.
// Если входной конфиг невалидный — возвращает ошибку.
func (b *CircuitBreaker) UpdateConfig(cfg Config) error {
	if err := validateConfig(cfg); err != nil {
		return err
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

// defaultIsFailure — дефолтная классификация ошибок.
// Правило простое:
// - nil: не ошибка;
// - context.Canceled: нейтрально, на брекер не влияет;
// - всё остальное (включая DeadlineExceeded): считаем failure.
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
	execState := StateClosed
	classifier := defaultIsFailure

	b.mu.Lock()
	b.lazyTransitionLocked(now)
	execState = b.state
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
		}
		if b.state == StateClosed {
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
		}
		b.toOpenLocked(b.now())
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

// toOpenLocked переводит брекер в OPEN и запоминает время открытия.
func (b *CircuitBreaker) toOpenLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
}

// toClosedLocked переводит брекер в CLOSED и сбрасывает счётчики.
func (b *CircuitBreaker) toClosedLocked() {
	b.state = StateClosed
	b.failures = 0
	b.inFlightProbes = 0
}
