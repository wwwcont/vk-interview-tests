// Tests in this file focus only on the core breaker algorithm.
//
// Goal in simple words:
// keep a very small set of tests that are easy to explain in live coding,
// while still checking the most important behavior:
// - opening after repeated failures,
// - rejecting while open,
// - half-open recovery path,
// - failure classifier rules,
// - half-open probe limit.
package breaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newBreaker creates breaker for test and fails test on config error.
func newBreaker(t *testing.T, cfg Config) *CircuitBreaker {
	t.Helper()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeClock injects deterministic time source and returns step function.
func fakeClock(b *CircuitBreaker, start time.Time) func(time.Duration) {
	now := start
	b.now = func() time.Time { return now }
	return func(d time.Duration) { now = now.Add(d) }
}

// toHalfOpen opens breaker with one failure, then advances clock to timeout.
func toHalfOpen(t *testing.T, b *CircuitBreaker, step func(time.Duration), timeout time.Duration) {
	t.Helper()
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	step(timeout)
	if got := b.State().State; got != StateHalfOpen {
		t.Fatalf("state=%s want HALF_OPEN", got)
	}
}

// TestClosedToOpenAtThreshold verifies failure counter opens breaker at threshold.
func TestClosedToOpenAtThreshold(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 2, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	if got := b.State(); got.State != StateClosed || got.Failures != 1 {
		t.Fatalf("unexpected state: %+v", got)
	}
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	if got := b.State().State; got != StateOpen {
		t.Fatalf("state=%s want OPEN", got)
	}
}

// TestOpenRejectsAndSkipsFn verifies open breaker rejects and does not run fn.
func TestOpenRejectsAndSkipsFn(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	called := false
	err := b.Execute(context.Background(), func(context.Context) error { called = true; return nil })
	if !errors.Is(err, ErrBreakerOpen) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}

// TestLazyHalfOpenAndProbeSuccessCloses verifies lazy half-open and successful recovery.
func TestLazyHalfOpenAndProbeSuccessCloses(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	step := fakeClock(b, time.Unix(10, 0))
	toHalfOpen(t, b, step, time.Second)
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := b.State(); got.State != StateClosed || got.Failures != 0 {
		t.Fatalf("unexpected state: %+v", got)
	}
}

// TestClassifierRules verifies default classifier: canceled is neutral, deadline is failure.
func TestClassifierRules(t *testing.T) {
	t.Run("context canceled is not failure", func(t *testing.T) {
		b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
		err := b.Execute(context.Background(), func(context.Context) error { return context.Canceled })
		if !errors.Is(err, context.Canceled) || b.State().State != StateClosed {
			t.Fatalf("err=%v state=%s", err, b.State().State)
		}
	})

	t.Run("deadline exceeded is failure", func(t *testing.T) {
		b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
		_ = b.Execute(context.Background(), func(context.Context) error { return context.DeadlineExceeded })
		if got := b.State().State; got != StateOpen {
			t.Fatalf("state=%s want OPEN", got)
		}
	})
}

// TestHalfOpenNonFailureAndProbeLimit verifies neutral error in half-open and probe limit behavior.
func TestHalfOpenNonFailureAndProbeLimit(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	step := fakeClock(b, time.Unix(10, 0))
	toHalfOpen(t, b, step, time.Second)

	if err := b.Execute(context.Background(), func(context.Context) error { return context.Canceled }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if got := b.State(); got.State != StateHalfOpen || got.Failures != 1 {
		t.Fatalf("unexpected after non-failure: %+v", got)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = b.Execute(context.Background(), func(context.Context) error { close(started); <-release; return nil })
	}()
	<-started
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrTooManyProbes) {
		t.Fatalf("err=%v want ErrTooManyProbes", err)
	}
	close(release)
}
