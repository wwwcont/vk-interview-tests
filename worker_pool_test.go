package interview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolParallelismLimit(t *testing.T) {
	p := NewWorkerPool(3)
	defer func() { _ = p.Close(context.Background()) }()

	var running int32
	var maxRunning int32
	for i := 0; i < 30; i++ {
		err := p.Submit(context.Background(), Normal, func(context.Context) error {
			cur := atomic.AddInt32(&running, 1)
			for {
				m := atomic.LoadInt32(&maxRunning)
				if cur <= m || atomic.CompareAndSwapInt32(&maxRunning, m, cur) {
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

	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := atomic.LoadInt32(&maxRunning); got > 3 {
		t.Fatalf("max running = %d, want <= 3", got)
	}
}

func TestPoolResizeWorks(t *testing.T) {
	p := NewWorkerPool(1)
	defer func() { _ = p.Close(context.Background()) }()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	for i := 0; i < 2; i++ {
		if err := p.Submit(context.Background(), Normal, func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}

	p.Resize(2)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second task did not start after resize")
	}

	close(release)
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestPoolPriority(t *testing.T) {
	p := NewWorkerPool(1)
	defer func() { _ = p.Close(context.Background()) }()

	start := make(chan struct{})
	if err := p.Submit(context.Background(), Normal, func(context.Context) error {
		<-start
		return nil
	}); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	order := make(chan string, 2)
	_ = p.Submit(context.Background(), Normal, func(context.Context) error { order <- "normal"; return nil })
	_ = p.Submit(context.Background(), High, func(context.Context) error { order <- "high"; return nil })

	close(start)
	first := <-order
	if first != "high" {
		t.Fatalf("first executed = %s, want high", first)
	}
}

func TestPoolCancelledContextTaskNotExecuted(t *testing.T) {
	p := NewWorkerPool(1)
	defer func() { _ = p.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var ran int32
	err := p.Submit(ctx, Normal, func(context.Context) error {
		atomic.StoreInt32(&ran, 1)
		return nil
	})
	if err == nil {
		t.Fatal("Submit() error = nil, want context canceled")
	}

	_ = p.Close(context.Background())
	if atomic.LoadInt32(&ran) != 0 {
		t.Fatal("task should not execute for canceled context")
	}
}

func TestPoolCloseGracefulAndSubmitAfterClose(t *testing.T) {
	p := NewWorkerPool(2)

	block := make(chan struct{})
	if err := p.Submit(context.Background(), Normal, func(context.Context) error { <-block; return nil }); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := p.Close(closeCtx)
	if err == nil {
		t.Fatal("Close() should return timeout while task is blocked")
	}

	close(block)
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := p.Submit(context.Background(), Normal, func(context.Context) error { return nil }); err == nil {
		t.Fatal("Submit() after close should fail")
	}
}

func TestPoolConcurrentSubmit(t *testing.T) {
	p := NewWorkerPool(8)
	defer func() { _ = p.Close(context.Background()) }()

	var wg sync.WaitGroup
	var done int32
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Submit(context.Background(), Normal, func(context.Context) error {
				atomic.AddInt32(&done, 1)
				return nil
			}); err != nil {
				t.Errorf("Submit() error = %v", err)
			}
		}()
	}
	wg.Wait()
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if atomic.LoadInt32(&done) != 200 {
		t.Fatalf("done = %d, want 200", done)
	}
}
