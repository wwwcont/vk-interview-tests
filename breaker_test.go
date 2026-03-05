package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestBreaker(t *testing.T, cfg Config) *CircuitBreaker {
	t.Helper()
	b, err := NewCircuitBreaker(cfg)
	if err != nil {
		t.Fatalf("NewCircuitBreaker() error = %v", err)
	}
	return b
}

func TestClosedToOpenAfterMaxFailures(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 3, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	for i := 0; i < 2; i++ {
		err := b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
		if err == nil {
			t.Fatalf("expected error")
		}
	}

	s := b.State()
	if s.State != StateClosed || s.Failures != 2 {
		t.Fatalf("unexpected state before threshold: %+v", s)
	}

	err := b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	if err == nil {
		t.Fatalf("expected error")
	}

	s = b.State()
	if s.State != StateOpen {
		t.Fatalf("state = %s, want OPEN", s.State)
	}
}

func TestOpenRejectsAndDoesNotCallFn(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })

	called := false
	err := b.Execute(context.Background(), func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("error = %v, want ErrBreakerOpen", err)
	}
	if called {
		t.Fatalf("function should not be called while open")
	}
}

func TestExecuteAfterResetTimeoutEntersHalfOpen(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: 30 * time.Millisecond, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	time.Sleep(40 * time.Millisecond)

	started := make(chan struct{})
	finish := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- b.Execute(context.Background(), func(context.Context) error {
			close(started)
			<-finish
			return nil
		})
	}()

	<-started
	s := b.State()
	if s.State != StateHalfOpen {
		t.Fatalf("state = %s, want HALF_OPEN", s.State)
	}
	close(finish)
	if err := <-result; err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}
}

func TestHalfOpenSuccessClosesBreaker(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: 20 * time.Millisecond, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	time.Sleep(30 * time.Millisecond)

	err := b.Execute(context.Background(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("execute error = %v", err)
	}

	s := b.State()
	if s.State != StateClosed || s.Failures != 0 {
		t.Fatalf("unexpected state after half-open success: %+v", s)
	}
}

func TestHalfOpenFailureReopensBreaker(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: 20 * time.Millisecond, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	time.Sleep(30 * time.Millisecond)

	err := b.Execute(context.Background(), func(context.Context) error { return errors.New("still bad") })
	if err == nil {
		t.Fatalf("expected error")
	}

	s := b.State()
	if s.State != StateOpen {
		t.Fatalf("state = %s, want OPEN", s.State)
	}
}

func TestHalfOpenProbeLimitParallel(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: 20 * time.Millisecond, HalfOpenMaxProbes: 2})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	time.Sleep(30 * time.Millisecond)

	block := make(chan struct{})
	var started atomic.Int32
	var wg sync.WaitGroup
	errs := make([]error, 4)

	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = b.Execute(context.Background(), func(context.Context) error {
				started.Add(1)
				<-block
				return nil
			})
		}(i)
	}

	time.Sleep(20 * time.Millisecond)
	close(block)
	wg.Wait()

	allowed := 0
	rejected := 0
	for _, err := range errs {
		if err == nil {
			allowed++
			continue
		}
		if errors.Is(err, ErrBreakerOpen) {
			rejected++
		}
	}
	if started.Load() > 2 {
		t.Fatalf("started probes = %d, want <= 2", started.Load())
	}
	if allowed == 0 || rejected == 0 {
		t.Fatalf("want mix of allowed/rejected calls, got allowed=%d rejected=%d", allowed, rejected)
	}
}
