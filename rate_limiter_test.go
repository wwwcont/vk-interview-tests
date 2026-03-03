package interview

import (
	"sync"
	"testing"
	"time"
)

func TestTokenBucketBurst(t *testing.T) {
	l := NewTokenBucketLimiter(1, 3)
	if !l.Allow() || !l.Allow() || !l.Allow() {
		t.Fatal("expected first 3 Allow() calls to pass")
	}
	if l.Allow() {
		t.Fatal("expected 4th Allow() to fail")
	}
}

func TestTokenBucketRPSLimit(t *testing.T) {
	l := NewTokenBucketLimiter(5, 1)
	if !l.Allow() {
		t.Fatal("first Allow() should pass")
	}
	if l.Allow() {
		t.Fatal("immediate second Allow() should fail")
	}
	time.Sleep(230 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("Allow() should pass after refill time")
	}
}

func TestTokenBucketConcurrentAllow(t *testing.T) {
	l := NewTokenBucketLimiter(0, 50)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 50 {
		t.Fatalf("allowed = %d, want 50", allowed)
	}
}
