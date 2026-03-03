package interview

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestBreaker() Breaker {
	return NewRollingCircuitBreaker(breakerConfig{
		WindowSize:       4,
		ErrorThreshold:   0.5,
		ResetTimeout:     40 * time.Millisecond,
		MaxProbeRequests: 2,
		IsFailure: func(err error) bool {
			return err != nil
		},
	})
}

func TestBreakerClosedToOpen(t *testing.T) {
	b := newTestBreaker()
	fail := errors.New("fail")

	_ = b.Execute(context.Background(), func(context.Context) error { return fail })
	_ = b.Execute(context.Background(), func(context.Context) error { return fail })
	_ = b.Execute(context.Background(), func(context.Context) error { return nil })
	_ = b.Execute(context.Background(), func(context.Context) error { return nil })

	if b.State() != Open {
		t.Fatalf("State() = %v, want Open", b.State())
	}
}

func TestBreakerOpenToHalfOpen(t *testing.T) {
	b := newTestBreaker()
	fail := errors.New("fail")
	for i := 0; i < 4; i++ {
		_ = b.Execute(context.Background(), func(context.Context) error { return fail })
	}
	if b.State() != Open {
		t.Fatalf("State() = %v, want Open", b.State())
	}
	time.Sleep(50 * time.Millisecond)
	if b.State() != HalfOpen {
		t.Fatalf("State() = %v, want HalfOpen", b.State())
	}
}

func TestBreakerHalfOpenRecovery(t *testing.T) {
	b := newTestBreaker()
	for i := 0; i < 4; i++ {
		_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("f") })
	}
	time.Sleep(50 * time.Millisecond)

	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("probe 1 err = %v", err)
	}
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("probe 2 err = %v", err)
	}
	if b.State() != Closed {
		t.Fatalf("State() = %v, want Closed", b.State())
	}
}

func TestBreakerHalfOpenFailureBackToOpen(t *testing.T) {
	b := newTestBreaker()
	for i := 0; i < 4; i++ {
		_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("f") })
	}
	time.Sleep(50 * time.Millisecond)

	err := b.Execute(context.Background(), func(context.Context) error { return errors.New("probe") })
	if err == nil {
		t.Fatal("probe err = nil, want error")
	}
	if b.State() != Open {
		t.Fatalf("State() = %v, want Open", b.State())
	}
}

func TestBreakerPredicate(t *testing.T) {
	b := NewRollingCircuitBreaker(breakerConfig{
		WindowSize:       2,
		ErrorThreshold:   0.5,
		ResetTimeout:     10 * time.Millisecond,
		MaxProbeRequests: 1,
		IsFailure: func(err error) bool {
			return err != nil && err.Error() == "fatal"
		},
	})

	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("non-fatal") })
	_ = b.Execute(context.Background(), func(context.Context) error { return nil })

	if b.State() != Closed {
		t.Fatalf("State() = %v, want Closed", b.State())
	}
}

func TestBreakerConcurrentExecute(t *testing.T) {
	b := NewRollingCircuitBreaker(breakerConfig{
		WindowSize:       4,
		ErrorThreshold:   0.5,
		ResetTimeout:     time.Second,
		MaxProbeRequests: 1,
	})

	var calls int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Execute(context.Background(), func(context.Context) error {
				atomic.AddInt32(&calls, 1)
				return errors.New("x")
			})
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&calls) == 0 {
		t.Fatal("expected at least one call")
	}
	if b.State() != Open {
		t.Fatalf("State() = %v, want Open", b.State())
	}
}
