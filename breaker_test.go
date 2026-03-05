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

func setFakeNow(b *CircuitBreaker, start time.Time) func(time.Duration) {
	cur := start
	b.now = func() time.Time { return cur }
	return func(d time.Duration) { cur = cur.Add(d) }
}

func openAndMoveToHalfOpen(t *testing.T, b *CircuitBreaker, advance func(time.Duration), d time.Duration) {
	t.Helper()
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	advance(d)
	_ = b.State()
	if s := b.State(); s.State != StateHalfOpen {
		t.Fatalf("state=%s, want HALF_OPEN", s.State)
	}
}

func TestClosedToOpenAfterMaxFailures(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 3, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	for i := 0; i < 2; i++ {
		err := b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
		if err == nil {
			t.Fatalf("expected error")
		}
	}
	if s := b.State(); s.State != StateClosed || s.Failures != 2 {
		t.Fatalf("unexpected state before threshold: %+v", s)
	}
	if err := b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") }); err == nil {
		t.Fatalf("expected error")
	}
	if s := b.State(); s.State != StateOpen {
		t.Fatalf("state=%s, want OPEN", s.State)
	}
}

func TestOpenRejectsAndDoesNotCallFn(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	called := false
	err := b.Execute(context.Background(), func(context.Context) error { called = true; return nil })
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("error=%v, want ErrBreakerOpen", err)
	}
	if called {
		t.Fatalf("function should not be called while OPEN")
	}
}

func TestExecuteAfterResetTimeoutEntersHalfOpen(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	advance := setFakeNow(b, time.Unix(100, 0))
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	advance(time.Second)
	if s := b.State(); s.State != StateHalfOpen {
		t.Fatalf("state=%s, want HALF_OPEN", s.State)
	}
}

func TestHalfOpenSuccessClosesBreaker(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	advance := setFakeNow(b, time.Unix(100, 0))
	openAndMoveToHalfOpen(t, b, advance, time.Second)
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("execute error=%v", err)
	}
	if s := b.State(); s.State != StateClosed || s.Failures != 0 {
		t.Fatalf("unexpected state: %+v", s)
	}
}

func TestHalfOpenFailureReopensBreaker(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	advance := setFakeNow(b, time.Unix(100, 0))
	openAndMoveToHalfOpen(t, b, advance, time.Second)
	if err := b.Execute(context.Background(), func(context.Context) error { return errors.New("still bad") }); err == nil {
		t.Fatalf("expected error")
	}
	if s := b.State(); s.State != StateOpen {
		t.Fatalf("state=%s, want OPEN", s.State)
	}
}

func TestClassifier_ContextCanceledNotFailure(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 2, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	err := b.Execute(context.Background(), func(context.Context) error { return context.Canceled })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
	if s := b.State(); s.State != StateClosed || s.Failures != 0 {
		t.Fatalf("unexpected state: %+v", s)
	}
}

func TestClassifier_DeadlineExceededIsFailure(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	err := b.Execute(context.Background(), func(context.Context) error { return context.DeadlineExceeded })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want context.DeadlineExceeded", err)
	}
	if s := b.State(); s.State != StateOpen {
		t.Fatalf("state=%s, want OPEN", s.State)
	}
}

func TestHalfOpen_NonFailureDoesNotReopen(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	advance := setFakeNow(b, time.Unix(100, 0))
	openAndMoveToHalfOpen(t, b, advance, time.Second)
	err := b.Execute(context.Background(), func(context.Context) error { return context.Canceled })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
	if s := b.State(); s.State != StateHalfOpen || s.Failures != 1 {
		t.Fatalf("unexpected state after non-failure probe: %+v", s)
	}
}

func TestHalfOpen_ProbeLimitErrTooManyProbes(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	advance := setFakeNow(b, time.Unix(100, 0))
	openAndMoveToHalfOpen(t, b, advance, time.Second)

	block := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = b.Execute(context.Background(), func(context.Context) error { close(started); <-block; return nil })
	}()
	<-started
	err := b.Execute(context.Background(), func(context.Context) error { return nil })
	close(block)
	if !errors.Is(err, ErrTooManyProbes) {
		t.Fatalf("error=%v, want ErrTooManyProbes", err)
	}
}

func TestHalfOpenProbeLimitParallel(t *testing.T) {
	b := newTestBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 2})
	advance := setFakeNow(b, time.Unix(100, 0))
	openAndMoveToHalfOpen(t, b, advance, time.Second)

	block := make(chan struct{})
	var started atomic.Int32
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = b.Execute(context.Background(), func(context.Context) error { started.Add(1); <-block; return nil })
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
	close(block)
	wg.Wait()
	if started.Load() > 2 {
		t.Fatalf("started probes=%d, want <=2", started.Load())
	}
	rejected := 0
	for _, err := range errs {
		if errors.Is(err, ErrTooManyProbes) {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatalf("expected at least one ErrTooManyProbes")
	}
}
