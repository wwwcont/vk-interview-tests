package interview

import (
	"container/list"
	"context"
	"sync"
	"time"
)

/*
ЗАДАЧА 5 — TTL Cache (Singleflight + Size Limit + Stale-While-Revalidate)

Постановка:
- Нужен потокобезопасный in-memory cache.
- У каждого значения свой TTL.
- При превышении maxEntries должны удаляться старые элементы (LRU допустим).
- Метод GetOrLoad должен поддерживать singleflight:
  для одного key параллельные запросы должны делить один loader-вызов.
- Запрещено использовать x/sync/singleflight.
- (Опционально) stale-while-revalidate — в этом решении не включён,
  используется строгая проверка TTL.

Алгоритм решения:
1) Храним:
   - map key -> entry(value, expiresAt, elem),
   - двусвязный список LRU,
   - map inflight key -> loadCall(done,val,err) для singleflight.
2) Get:
   - под mutex проверяем наличие и TTL,
   - истёкшие записи удаляем лениво,
   - живые поднимаем в голову LRU.
3) Set:
   - upsert записи,
   - обновляем LRU,
   - если размер > maxEntries, выталкиваем с хвоста LRU.
4) GetOrLoad:
   - сначала fast-path через Get,
   - затем через inflight map определяем leader/follower:
     - leader запускает loader,
     - followers ждут completion того же loadCall,
   - leader публикует результат всем waiters и сохраняет в cache при успехе.
*/

type Cache interface {
	Get(key string) (any, bool)
	Set(key string, value any, ttl time.Duration)
	GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error)
}

type cacheEntry struct {
	key       string
	value     any
	expiresAt time.Time
	elem      *list.Element
}

type loadCall struct {
	done chan struct{}
	val  any
	err  error
}

type lruTTLCache struct {
	mu sync.Mutex

	maxEntries int
	items      map[string]*cacheEntry
	lru        *list.List
	inflight   map[string]*loadCall
}

func NewTTLCache(maxEntries int) Cache {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	return &lruTTLCache{
		maxEntries: maxEntries,
		items:      make(map[string]*cacheEntry),
		lru:        list.New(),
		inflight:   make(map[string]*loadCall),
	}
}

func (c *lruTTLCache) Get(key string) (any, bool) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if now.After(e.expiresAt) {
		c.removeEntry(e)
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	return e.value, true
}

func (c *lruTTLCache) Set(key string, value any, ttl time.Duration) {
	now := time.Now()
	expiresAt := now.Add(ttl)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(key, value, expiresAt)
}

func (c *lruTTLCache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if v, ok := c.Get(key); ok {
		return v, nil
	}

	call, leader := c.getOrCreateCall(key)
	if !leader {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return call.val, call.err
		}
	}

	val, err := loader(ctx)

	c.mu.Lock()
	if err == nil {
		c.setLocked(key, val, time.Now().Add(ttl))
	}
	delete(c.inflight, key)
	call.val = val
	call.err = err
	close(call.done)
	c.mu.Unlock()

	return val, err
}

func (c *lruTTLCache) getOrCreateCall(key string) (*loadCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.inflight[key]; ok {
		return existing, false
	}
	call := &loadCall{done: make(chan struct{})}
	c.inflight[key] = call
	return call, true
}

func (c *lruTTLCache) setLocked(key string, value any, expiresAt time.Time) {
	if e, ok := c.items[key]; ok {
		e.value = value
		e.expiresAt = expiresAt
		c.lru.MoveToFront(e.elem)
		return
	}

	elem := c.lru.PushFront(key)
	entry := &cacheEntry{key: key, value: value, expiresAt: expiresAt, elem: elem}
	c.items[key] = entry
	for len(c.items) > c.maxEntries {
		back := c.lru.Back()
		if back == nil {
			break
		}
		k := back.Value.(string)
		c.removeEntry(c.items[k])
	}
}

func (c *lruTTLCache) removeEntry(e *cacheEntry) {
	if e == nil {
		return
	}
	delete(c.items, e.key)
	c.lru.Remove(e.elem)
}
