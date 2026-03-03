package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"vk-interview-tests/subprojects/task2_worker_pool/domain"
)

func TestPriorityAndResize(t *testing.T) {
	s := New(1)
	defer s.Close(context.Background())
	block := make(chan struct{})
	_ = s.Submit(context.Background(), domain.Normal, func(context.Context) error { <-block; return nil })
	order := make(chan string, 2)
	_ = s.Submit(context.Background(), domain.Normal, func(context.Context) error { order <- "n"; return nil })
	_ = s.Submit(context.Background(), domain.High, func(context.Context) error { order <- "h"; return nil })
	close(block)
	if <-order != "h" {
		t.Fatal("high first")
	}
	s.Resize(2)
}
func TestCloseTimeout(t *testing.T) {
	s := New(1)
	ch := make(chan struct{})
	_ = s.Submit(context.Background(), domain.Normal, func(context.Context) error { <-ch; return nil })
	cctx, _ := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if s.Close(cctx) == nil {
		t.Fatal("want timeout")
	}
	close(ch)
	_ = s.Close(context.Background())
}
func TestCancelledSubmit(t *testing.T) {
	s := New(1)
	defer s.Close(context.Background())
	ctx, c := context.WithCancel(context.Background())
	c()
	if err := s.Submit(ctx, domain.Normal, func(context.Context) error { return nil }); err == nil {
		t.Fatal("want err")
	}
}
func TestParallelLimit(t *testing.T) {
	s := New(2)
	defer s.Close(context.Background())
	var run, max int32
	for i := 0; i < 20; i++ {
		_ = s.Submit(context.Background(), domain.Normal, func(context.Context) error {
			c := atomic.AddInt32(&run, 1)
			for {
				m := atomic.LoadInt32(&max)
				if c <= m || atomic.CompareAndSwapInt32(&max, m, c) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&run, -1)
			return nil
		})
	}
	_ = s.Close(context.Background())
	if max > 2 {
		t.Fatal("parallel limit broken")
	}
}
