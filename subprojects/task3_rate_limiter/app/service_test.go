package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIndependentAndBurst(t *testing.T) {
	s := New(0, 2, time.Second)
	if !s.Allow("a") || !s.Allow("a") || s.Allow("a") {
		t.Fatal("burst")
	}
	if !s.Allow("b") {
		t.Fatal("independent key")
	}
}
func TestAcquireWaitAndCancel(t *testing.T) {
	s := New(5, 1, time.Second)
	_ = s.Allow("k")
	st := time.Now()
	if err := s.Acquire(context.Background(), "k"); err != nil || time.Since(st) < 150*time.Millisecond {
		t.Fatal("acquire wait")
	}
	ctx, c := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer c()
	_ = s.Allow("x")
	if err := s.Acquire(ctx, "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cancel")
	}
}
func TestTTL(t *testing.T) {
	s := New(0, 1, 30*time.Millisecond)
	_ = s.Allow("k")
	if s.Allow("k") {
		t.Fatal("should empty")
	}
	time.Sleep(40 * time.Millisecond)
	if !s.Allow("k") {
		t.Fatal("ttl cleanup")
	}
}
