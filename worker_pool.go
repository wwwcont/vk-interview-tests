package interview

import (
	"context"
	"errors"
	"sync"
)

/*
ЗАДАЧА 2 — Worker Pool (Dynamic Resize + Priority + Graceful Shutdown)

Постановка:
- Есть задачи Task(ctx) и пул, который должен выполнять их конкурентно.
- Нужно ограничить одновременное выполнение максимум N воркерами.
- Поддержать динамический Resize(n): увеличить/уменьшить число воркеров на лету.
- Есть приоритеты: High должен исполняться раньше Normal.
- Submit принимает context:
  - если контекст уже отменён или отменился до постановки, задача не должна попасть в очередь.
- После Close новые Submit должны возвращать ошибку.
- Close(ctx) должен дождаться завершения уже принятых задач,
  либо вернуть ошибку по таймауту/отмене ctx.
- Потокобезопасность обязательна и нельзя допустить утечек горутин.

Алгоритм решения:
1) Две очереди задач (каналы): highQ и normQ.
2) Пул воркеров:
   - каждый воркер сначала non-blocking проверяет highQ,
   - затем блокируется на select(highQ, normQ, stop).
   Это обеспечивает приоритет High.
3) Для dynamic resize:
   - при увеличении запускаем новые горутины воркеров,
   - при уменьшении закрываем персональные stop-каналы лишних воркеров.
4) Для корректного Close:
   - ставим флаг closed и закрываем closeCh (чтобы Submit перестал принимать),
   - ждём tasksWG (все принятые задачи завершены),
   - останавливаем воркеры stop-сигналом,
   - ждём workersWG,
   - на каждом этапе учитываем ctx.Done().
5) Submit:
   - проверяет ctx и флаг closed,
   - инкрементит tasksWG только для реально принятых задач,
   - при отмене/закрытии корректно откатывает счётчик tasksWG.
*/

type Priority int

const (
	High Priority = iota
	Normal
)

type Task func(ctx context.Context) error

type Pool interface {
	Submit(ctx context.Context, p Priority, task Task) error
	Resize(n int)
	Close(ctx context.Context) error
}

var errPoolClosed = errors.New("pool closed")

type taskItem struct {
	ctx  context.Context
	task Task
}

type workerHandle struct {
	stop chan struct{}
}

type priorityPool struct {
	mu sync.Mutex

	closed bool

	highQ chan taskItem
	normQ chan taskItem

	closeCh chan struct{}

	workers []*workerHandle

	tasksWG   sync.WaitGroup
	workersWG sync.WaitGroup
}

func NewWorkerPool(n int) Pool {
	if n <= 0 {
		n = 1
	}

	p := &priorityPool{
		highQ:   make(chan taskItem, 1024),
		normQ:   make(chan taskItem, 1024),
		closeCh: make(chan struct{}),
	}
	p.Resize(n)
	return p
}

func (p *priorityPool) Submit(ctx context.Context, pr Priority, task Task) error {
	if task == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errPoolClosed
	}
	p.tasksWG.Add(1)
	p.mu.Unlock()

	item := taskItem{ctx: ctx, task: task}
	queue := p.normQ
	if pr == High {
		queue = p.highQ
	}

	select {
	case <-ctx.Done():
		p.tasksWG.Done()
		return ctx.Err()
	case <-p.closeCh:
		p.tasksWG.Done()
		return errPoolClosed
	case queue <- item:
		return nil
	}
}

func (p *priorityPool) Resize(n int) {
	if n <= 0 {
		n = 1
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}

	current := len(p.workers)
	if n > current {
		for i := current; i < n; i++ {
			h := &workerHandle{stop: make(chan struct{})}
			p.workers = append(p.workers, h)
			p.workersWG.Add(1)
			go p.worker(h)
		}
		return
	}

	if n < current {
		for i := current - 1; i >= n; i-- {
			close(p.workers[i].stop)
		}
		p.workers = p.workers[:n]
	}
}

func (p *priorityPool) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.closeCh)
	p.mu.Unlock()

	tasksDone := make(chan struct{})
	go func() {
		p.tasksWG.Wait()
		close(tasksDone)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tasksDone:
	}

	p.mu.Lock()
	for _, h := range p.workers {
		close(h.stop)
	}
	p.workers = nil
	p.mu.Unlock()

	workersDone := make(chan struct{})
	go func() {
		p.workersWG.Wait()
		close(workersDone)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-workersDone:
		return nil
	}
}

func (p *priorityPool) worker(h *workerHandle) {
	defer p.workersWG.Done()

	for {
		select {
		case <-h.stop:
			return
		default:
		}

		select {
		case item := <-p.highQ:
			p.run(item)
			continue
		default:
		}

		select {
		case <-h.stop:
			return
		case item := <-p.highQ:
			p.run(item)
		case item := <-p.normQ:
			p.run(item)
		}
	}
}

func (p *priorityPool) run(item taskItem) {
	defer p.tasksWG.Done()
	if item.ctx.Err() != nil {
		return
	}
	_ = item.task(item.ctx)
}
