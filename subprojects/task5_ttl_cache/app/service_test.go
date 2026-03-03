package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vk-interview-tests/subprojects/task5_ttl_cache/domain"
)

func TestSingleflight(t *testing.T) {
	s := New(domain.Config{MaxEntries: 10})
	var n int32
	l := func(context.Context) (any, error) {
		atomic.AddInt32(&n, 1)
		time.Sleep(10 * time.Millisecond)
		return "x", nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = s.GetOrLoad(context.Background(), "k", time.Second, l) }()
	}
	wg.Wait()
	if n != 1 {
		t.Fatal("singleflight")
	}
}
func TestTTLAndEviction(t *testing.T) {
	s := New(domain.Config{MaxEntries: 2})
	s.Set("a", 1, time.Second)
	s.Set("b", 2, time.Second)
	_, _ = s.Get("a")
	s.Set("c", 3, time.Second)
	if _, ok := s.Get("b"); ok {
		t.Fatal("eviction")
	}
	s.Set("x", 1, 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("x"); ok {
		t.Fatal("ttl")
	}
}
func TestStaleOnError(t *testing.T) {
	s := New(domain.Config{MaxEntries: 2, StaleGrace: 2 * time.Second})
	s.Set("k", "old", 10*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	v, err := s.GetOrLoad(context.Background(), "k", time.Second, func(context.Context) (any, error) { return nil, errors.New("fail") })
	if err != nil || v.(string) != "old" {
		t.Fatal("stale fallback")
	}
}
