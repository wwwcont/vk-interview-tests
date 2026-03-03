package interview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheSingleflight(t *testing.T) {
	c := NewTTLCache(10)
	var loaderCalls int32

	loader := func(context.Context) (any, error) {
		atomic.AddInt32(&loaderCalls, 1)
		time.Sleep(20 * time.Millisecond)
		return "value", nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.GetOrLoad(context.Background(), "k", time.Second, loader)
			if err != nil {
				t.Errorf("GetOrLoad() error = %v", err)
				return
			}
			if v.(string) != "value" {
				t.Errorf("value = %v, want value", v)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&loaderCalls) != 1 {
		t.Fatalf("loader calls = %d, want 1", loaderCalls)
	}
}

func TestCacheTTLExpires(t *testing.T) {
	c := NewTTLCache(10)
	c.Set("k", 1, 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected key expired")
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewTTLCache(2)
	c.Set("a", 1, time.Second)
	c.Set("b", 2, time.Second)
	_, _ = c.Get("a") // a becomes most recent
	c.Set("c", 3, time.Second)

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewTTLCache(100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Set("k", i, time.Second)
			_, _ = c.Get("k")
		}(i)
	}
	wg.Wait()
}
