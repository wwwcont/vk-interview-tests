package app

import (
	"context"
	"errors"
	"sync"

	"vk-interview-tests/subprojects/task2_worker_pool/domain"
)

/*
Усложнение задачи 2: Dynamic Resize + Priority + Graceful Shutdown + bounded queue.
Постановка: High приоритет выше Normal, Resize меняет число воркеров на лету,
Close(ctx) дожидается принятых задач, Submit после Close -> ошибка.
Дополнительно: ограниченная очередь (queueCap), чтобы не раздувать память.
Алгоритм: 2 очереди каналов, индивидуальный stop на воркер, tasksWG/workersWG,
worker сначала читает high non-blocking, потом select(high/normal/stop).
*/

const QueueCap = 256

var ErrClosed = errors.New("pool closed")

type taskItem struct {
	ctx context.Context
	t   domain.Task
}
type worker struct{ stop chan struct{} }

type Service struct {
	mu                 sync.Mutex
	closed             bool
	highQ, normQ       chan taskItem
	closeCh            chan struct{}
	workers            []*worker
	tasksWG, workersWG sync.WaitGroup
}

func New(n int) *Service {
	if n <= 0 {
		n = 1
	}
	s := &Service{highQ: make(chan taskItem, QueueCap), normQ: make(chan taskItem, QueueCap), closeCh: make(chan struct{})}
	s.Resize(n)
	return s
}

func (s *Service) Submit(ctx context.Context, p domain.Priority, task domain.Task) error {
	if task == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.tasksWG.Add(1)
	s.mu.Unlock()
	q := s.normQ
	if p == domain.High {
		q = s.highQ
	}
	select {
	case <-ctx.Done():
		s.tasksWG.Done()
		return ctx.Err()
	case <-s.closeCh:
		s.tasksWG.Done()
		return ErrClosed
	case q <- taskItem{ctx: ctx, t: task}:
		return nil
	}
}
func (s *Service) Resize(n int) {
	if n <= 0 {
		n = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	cur := len(s.workers)
	if n > cur {
		for i := cur; i < n; i++ {
			w := &worker{stop: make(chan struct{})}
			s.workers = append(s.workers, w)
			s.workersWG.Add(1)
			go s.run(w)
		}
		return
	}
	for i := cur - 1; i >= n; i-- {
		close(s.workers[i].stop)
	}
	s.workers = s.workers[:n]
}
func (s *Service) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.closeCh)
	s.mu.Unlock()
	d := make(chan struct{})
	go func() { s.tasksWG.Wait(); close(d) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d:
	}
	s.mu.Lock()
	for _, w := range s.workers {
		close(w.stop)
	}
	s.workers = nil
	s.mu.Unlock()
	wd := make(chan struct{})
	go func() { s.workersWG.Wait(); close(wd) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wd:
		return nil
	}
}
func (s *Service) run(w *worker) {
	defer s.workersWG.Done()
	for {
		select {
		case <-w.stop:
			return
		default:
		}
		select {
		case it := <-s.highQ:
			s.exec(it)
			continue
		default:
		}
		select {
		case <-w.stop:
			return
		case it := <-s.highQ:
			s.exec(it)
		case it := <-s.normQ:
			s.exec(it)
		}
	}
}
func (s *Service) exec(it taskItem) {
	defer s.tasksWG.Done()
	if it.ctx.Err() != nil {
		return
	}
	_ = it.t(it.ctx)
}
