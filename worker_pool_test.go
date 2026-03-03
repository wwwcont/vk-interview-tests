package interview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPoolParallelismLimit(t *testing.T) {
	const workers = 3
	pool := NewWorkerPool(workers)

	var running int32
	var maxRunning int32
	for i := 0; i < 20; i++ {
		err := pool.Submit(func(context.Context) error {
			cur := atomic.AddInt32(&running, 1)
			for {
				max := atomic.LoadInt32(&maxRunning)
				if cur <= max || atomic.CompareAndSwapInt32(&maxRunning, max, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&running, -1)
			return nil
		})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := atomic.LoadInt32(&maxRunning); got > workers {
		t.Fatalf("max parallelism = %d, want <= %d", got, workers)
	}
}

func TestWorkerPoolAllTasksExecuted(t *testing.T) {
	pool := NewWorkerPool(4)
	var count int32
	const total = 100

	for i := 0; i < total; i++ {
		if err := pool.Submit(func(context.Context) error {
			atomic.AddInt32(&count, 1)
			return nil
		}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if atomic.LoadInt32(&count) != total {
		t.Fatalf("executed = %d, want %d", count, total)
	}
}

func TestWorkerPoolCloseWaitsForTasks(t *testing.T) {
	pool := NewWorkerPool(1)
	block := make(chan struct{})
	if err := pool.Submit(func(context.Context) error {
		<-block
		return nil
	}); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		_ = pool.Close()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Close() returned before task completed")
	case <-time.After(30 * time.Millisecond):
	}

	close(block)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close() did not return after task completion")
	}
}

func TestWorkerPoolSubmitAfterClose(t *testing.T) {
	pool := NewWorkerPool(1)
	if err := pool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := pool.Submit(func(context.Context) error { return nil }); err == nil {
		t.Fatal("Submit() error = nil, want non-nil")
	}
}

func TestWorkerPoolConcurrentSubmit(t *testing.T) {
	pool := NewWorkerPool(8)
	var wg sync.WaitGroup
	var count int32

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pool.Submit(func(context.Context) error {
				atomic.AddInt32(&count, 1)
				return nil
			}); err != nil {
				t.Errorf("Submit() error = %v", err)
			}
		}()
	}
	wg.Wait()
	_ = pool.Close()
	if atomic.LoadInt32(&count) != 200 {
		t.Fatalf("executed = %d, want 200", count)
	}
}
