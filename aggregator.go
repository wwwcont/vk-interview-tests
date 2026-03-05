package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Event — входное событие потока.
type Event struct {
	Key   string
	Value int64
	Ts    time.Time
}

// Aggregate — агрегат по паре (WindowStart, Key).
type Aggregate struct {
	WindowStart time.Time
	Key         string
	Sum         int64
}

// Repo — внешнее хранилище агрегатов.
type Repo interface {
	SaveBatch(ctx context.Context, aggs []Aggregate) error
}

var ErrQueueFull = errors.New("queue full")
var ErrClosed = errors.New("aggregator closed")

// Aggregator — контракт сервиса.
type Aggregator interface {
	Add(e Event) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

// Config — минимальные настройки.
type Config struct {
	WindowSize    time.Duration
	QueueCap      int
	FlushInterval time.Duration
	MaxKeys       int
}

// Service — простой потокобезопасный агрегатор с одним воркером.
type Service struct {
	repo       Repo
	windowSize time.Duration
	maxKeys    int

	// eventCh ограничивает скорость Add (backpressure).
	eventCh chan request
	// flushCh отдельный канал для приоритетного Flush.
	flushCh chan *flushReq

	closed    atomic.Bool
	closeOnce sync.Once
	stopCh    chan struct{}
	done      chan struct{}
}

// request хранит Event значением (без *Event), чтобы избежать лишней escape-аллокации.
type request struct {
	hasEvent bool
	event    Event
}

type flushReq struct {
	ctx  context.Context
	errC chan error
}

type aggKey struct {
	windowStart time.Time
	key         string
}

func New(repo Repo, cfg Config) (*Service, error) {
	if repo == nil {
		return nil, errors.New("repo is nil")
	}
	if cfg.WindowSize <= 0 {
		return nil, errors.New("window size must be > 0")
	}
	if cfg.QueueCap <= 0 {
		return nil, errors.New("queue cap must be > 0")
	}
	s := &Service{
		repo:       repo,
		windowSize: cfg.WindowSize,
		maxKeys:    cfg.MaxKeys,
		eventCh:    make(chan request, cfg.QueueCap),
		flushCh:    make(chan *flushReq, 8),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
	go s.worker(cfg.FlushInterval)
	return s, nil
}

// Add неблокирующе пишет событие в bounded-очередь.
func (s *Service) Add(e Event) error {
	if s.closed.Load() {
		return ErrClosed
	}
	select {
	case s.eventCh <- request{hasEvent: true, event: e}:
		return nil
	default:
		if s.closed.Load() {
			return ErrClosed
		}
		return ErrQueueFull
	}
}

// Flush отправляет приоритетный запрос на flush и ждёт результат.
func (s *Service) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	fr := &flushReq{ctx: ctx, errC: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	case s.flushCh <- fr:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-fr.errC:
		return err
	}
}

// Close запрещает новые Add, делает финальный flush и останавливает воркер.
func (s *Service) Close(ctx context.Context) error {
	flushErr := error(nil)
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		flushErr = s.Flush(ctx)
		close(s.stopCh)
		<-s.done
	})
	return flushErr
}

// worker — единственная горутина, которая владеет state.
// Поэтому глобальные mutex не нужны, и мы не держим lock во время I/O.
func (s *Service) worker(flushInterval time.Duration) {
	defer close(s.done)
	state := make(map[aggKey]int64)

	var ticker *time.Ticker
	var tickerC <-chan time.Time
	if flushInterval > 0 {
		ticker = time.NewTicker(flushInterval)
		tickerC = ticker.C
		defer ticker.Stop()
	}

	for {
		// 1) Приоритетно вычитываем все pending flush-запросы.
		for {
			select {
			case fr := <-s.flushCh:
				fr.errC <- s.flushState(fr.ctx, state)
			default:
				goto mainLoop
			}
		}

	mainLoop:
		// 2) Обычная обработка: stop/flush/event/ticker.
		select {
		case <-s.stopCh:
			return
		case fr := <-s.flushCh:
			fr.errC <- s.flushState(fr.ctx, state)
		case req := <-s.eventCh:
			if !req.hasEvent {
				continue
			}
			e := req.event
			k := aggKey{windowStart: e.Ts.Truncate(s.windowSize), key: e.Key}
			state[k] += e.Value
			// Порог по ключам: best-effort auto-flush, чтобы не разрасталось состояние.
			if s.maxKeys > 0 && len(state) >= s.maxKeys {
				s.autoFlush(state)
			}
		case <-tickerC:
			// Периодический auto-flush (если включен).
			s.autoFlush(state)
		}
	}
}

// autoFlush с коротким таймаутом, чтобы воркер не зависал навсегда на I/O.
func (s *Service) autoFlush(state map[aggKey]int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = s.flushState(ctx, state)
}

// flushState формирует снимок, очищает state, пишет батч и при ошибке возвращает данные обратно.
func (s *Service) flushState(ctx context.Context, state map[aggKey]int64) error {
	if len(state) == 0 {
		return nil
	}
	batch := make([]Aggregate, 0, len(state))
	for k, sum := range state {
		batch = append(batch, Aggregate{WindowStart: k.windowStart, Key: k.key, Sum: sum})
	}
	for k := range state {
		delete(state, k)
	}
	if err := s.repo.SaveBatch(ctx, batch); err != nil {
		for _, a := range batch {
			state[aggKey{windowStart: a.WindowStart, key: a.Key}] += a.Sum
		}
		return fmt.Errorf("save batch: %w", err)
	}
	return nil
}
