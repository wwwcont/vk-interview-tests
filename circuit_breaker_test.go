package interview

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreakerClosedToOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)
	fail := errors.New("fail")

	if err := cb.Execute(context.Background(), func(context.Context) error { return fail }); !errors.Is(err, fail) {
		t.Fatalf("first Execute err = %v, want fail", err)
	}
	if err := cb.Execute(context.Background(), func(context.Context) error { return fail }); !errors.Is(err, fail) {
		t.Fatalf("second Execute err = %v, want fail", err)
	}
	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, errCircuitOpen) {
		t.Fatalf("third Execute err = %v, want errCircuitOpen", err)
	}
}

func TestCircuitBreakerOpenToHalfOpenAndRecover(t *testing.T) {
	cb := NewCircuitBreaker(1, 40*time.Millisecond)
	_ = cb.Execute(context.Background(), func(context.Context) error { return errors.New("fail") })

	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, errCircuitOpen) {
		t.Fatalf("Execute in OPEN err = %v, want errCircuitOpen", err)
	}

	time.Sleep(50 * time.Millisecond)
	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("half-open probe err = %v, want nil", err)
	}
	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("after recovery Execute err = %v, want nil", err)
	}
}

func TestCircuitBreakerHalfOpenFailureBackToOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)
	_ = cb.Execute(context.Background(), func(context.Context) error { return errors.New("fail") })
	time.Sleep(25 * time.Millisecond)

	probeErr := errors.New("probe-fail")
	if err := cb.Execute(context.Background(), func(context.Context) error { return probeErr }); !errors.Is(err, probeErr) {
		t.Fatalf("half-open err = %v, want probeErr", err)
	}
	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, errCircuitOpen) {
		t.Fatalf("should be open again, err = %v", err)
	}
}

func TestCircuitBreakerConcurrentExecute(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Second)
	var called int32
	start := make(chan struct{})

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cb.Execute(context.Background(), func(context.Context) error {
				atomic.AddInt32(&called, 1)
				<-start
				return errors.New("x")
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	openErrors := 0
	for err := range errs {
		if errors.Is(err, errCircuitOpen) {
			openErrors++
		}
	}

	if atomic.LoadInt32(&called) == 0 {
		t.Fatal("expected at least one function call")
	}
	if openErrors == 0 {
		t.Fatal("expected some errCircuitOpen in concurrent scenario")
	}
}
