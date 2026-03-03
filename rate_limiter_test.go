package interview

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPerKeyIndependent(t *testing.T) {
	l := NewPerKeyLimiter(0, 1, time.Second)
	if !l.Allow("a") {
		t.Fatal("a first allow should pass")
	}
	if l.Allow("a") {
		t.Fatal("a second allow should fail")
	}
	if !l.Allow("b") {
		t.Fatal("b should be independent from a")
	}
}

func TestPerKeyBurst(t *testing.T) {
	l := NewPerKeyLimiter(0, 3, time.Second)
	if !l.Allow("k") || !l.Allow("k") || !l.Allow("k") {
		t.Fatal("first three allows should pass")
	}
	if l.Allow("k") {
		t.Fatal("fourth allow should fail")
	}
}

func TestAcquireBlocksAndThenPasses(t *testing.T) {
	l := NewPerKeyLimiter(5, 1, time.Second)
	if !l.Allow("k") {
		t.Fatal("initial allow should pass")
	}

	start := time.Now()
	if err := l.Acquire(context.Background(), "k"); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("Acquire() did not block long enough")
	}
}

func TestAcquireCancelled(t *testing.T) {
	l := NewPerKeyLimiter(0, 1, time.Second)
	if !l.Allow("k") {
		t.Fatal("initial allow should pass")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := l.Acquire(ctx, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire() err = %v, want deadline exceeded", err)
	}
}

func TestKeyTTLRemovesIdleKey(t *testing.T) {
	l := NewPerKeyLimiter(0, 1, 40*time.Millisecond)
	if !l.Allow("k") {
		t.Fatal("first allow should pass")
	}
	if l.Allow("k") {
		t.Fatal("second allow should fail with zero rate")
	}

	time.Sleep(60 * time.Millisecond)
	if !l.Allow("k") {
		t.Fatal("allow should pass after key ttl cleanup")
	}
}
