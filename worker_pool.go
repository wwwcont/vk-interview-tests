package interview

import (
	"context"
	"errors"
	"sync"
)

type Task func(ctx context.Context) error

type Pool interface {
	Submit(task Task) error
	Close() error
}

var errPoolClosed = errors.New("pool is closed")

type workerPool struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	tasks  chan Task

	tasksWG   sync.WaitGroup
	workersWG sync.WaitGroup
}

func NewWorkerPool(workers int) Pool {
	if workers <= 0 {
		workers = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &workerPool{
		ctx:    ctx,
		cancel: cancel,
		tasks:  make(chan Task, workers*2),
	}

	for i := 0; i < workers; i++ {
		p.workersWG.Add(1)
		go p.worker()
	}

	return p
}

func (p *workerPool) Submit(task Task) error {
	if task == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errPoolClosed
	}

	p.tasksWG.Add(1)
	p.tasks <- task
	return nil
}

func (p *workerPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.tasks)
	p.mu.Unlock()

	p.tasksWG.Wait()
	p.cancel()
	p.workersWG.Wait()
	return nil
}

func (p *workerPool) worker() {
	defer p.workersWG.Done()
	for task := range p.tasks {
		_ = task(p.ctx)
		p.tasksWG.Done()
	}
}
