package interview

import (
	"sync"
	"time"
)

type Cache interface {
	Set(key string, value any, ttl time.Duration)
	Get(key string) (any, bool)
}

type cacheItem struct {
	value     any
	expiresAt time.Time
}

type ttlCache struct {
	mu    sync.Mutex
	items map[string]cacheItem
}

func NewTTLCache() Cache {
	return &ttlCache{items: make(map[string]cacheItem)}
}

func (c *ttlCache) Set(key string, value any, ttl time.Duration) {
	expiresAt := time.Now().Add(ttl)
	c.mu.Lock()
	c.items[key] = cacheItem{value: value, expiresAt: expiresAt}
	c.mu.Unlock()
}

func (c *ttlCache) Get(key string) (any, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if now.After(item.expiresAt) {
		delete(c.items, key)
		return nil, false
	}
	return item.value, true
}
