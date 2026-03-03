package interview

import (
	"sync"
	"testing"
	"time"
)

func TestTTLCacheValueBeforeExpiry(t *testing.T) {
	c := NewTTLCache()
	c.Set("k", 42, 100*time.Millisecond)

	v, ok := c.Get("k")
	if !ok || v.(int) != 42 {
		t.Fatalf("Get() = (%v, %v), want (42, true)", v, ok)
	}
}

func TestTTLCacheValueAfterExpiry(t *testing.T) {
	c := NewTTLCache()
	c.Set("k", 42, 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	if _, ok := c.Get("k"); ok {
		t.Fatal("Get() ok = true, want false")
	}
}

func TestTTLCacheOverwrite(t *testing.T) {
	c := NewTTLCache()
	c.Set("k", "v1", time.Second)
	c.Set("k", "v2", time.Second)

	v, ok := c.Get("k")
	if !ok || v.(string) != "v2" {
		t.Fatalf("Get() = (%v, %v), want (v2, true)", v, ok)
	}
}

func TestTTLCacheConcurrentAccess(t *testing.T) {
	c := NewTTLCache()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.Set("k", idx, time.Second)
			_, _ = c.Get("k")
		}(i)
	}
	wg.Wait()

	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected key to exist")
	}
}
