package interview

import (
	"context"
	"sync"
	"time"
)

/*
ЗАДАЧА 3 — Rate Limiter (Per-Key Token Bucket + Blocking Acquire)

Постановка:
- Нужно ограничивать частоту отдельно для каждого key.
- Интерфейс:
  - Allow(key) bool: неблокирующая попытка взять токен.
  - Acquire(ctx, key) error: блокируется, пока не появится токен или ctx не отменён.
- Конфигурация: rate (tokens/sec), burst, keyTTL.
- Неактивные ключи должны удаляться (lazy cleanup), чтобы карта бакетов не росла бесконечно.
- Потокобезопасность обязательна.
- Нельзя использовать time.Ticker; расчёт пополнения делается через time.Now().

Алгоритм решения:
1) Для каждого key храним bucket:
   - tokens,
   - lastFill (последний момент пополнения),
   - lastSeen (последняя активность по ключу).
2) Под одним mutex:
   - cleanup просроченных ключей по keyTTL,
   - get/create bucket,
   - refill: tokens += elapsed*rate, clamp до burst.
3) Allow:
   - после refill, если tokens >= 1 — уменьшаем и возвращаем true,
     иначе false.
4) Acquire:
   - циклически проверяем наличие токена,
   - если токена нет, вычисляем сколько ждать до 1 токена,
   - ждём через time.NewTimer(wait),
   - выходим по ctx.Done().
*/

type Limiter interface {
	Allow(key string) bool
	Acquire(ctx context.Context, key string) error
}

type keyBucket struct {
	tokens   float64
	lastFill time.Time
	lastSeen time.Time
}

type perKeyLimiter struct {
	mu sync.Mutex

	rate   float64
	burst  float64
	keyTTL time.Duration

	buckets map[string]*keyBucket
}

func NewPerKeyLimiter(rate float64, burst int, keyTTL time.Duration) Limiter {
	if rate < 0 {
		rate = 0
	}
	if burst < 0 {
		burst = 0
	}
	if keyTTL < 0 {
		keyTTL = 0
	}
	return &perKeyLimiter{
		rate:    rate,
		burst:   float64(burst),
		keyTTL:  keyTTL,
		buckets: make(map[string]*keyBucket),
	}
}

func (l *perKeyLimiter) Allow(key string) bool {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.cleanup(now)
	b := l.getBucket(key, now)
	l.refill(b, now)
	b.lastSeen = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (l *perKeyLimiter) Acquire(ctx context.Context, key string) error {
	for {
		now := time.Now()

		l.mu.Lock()
		l.cleanup(now)
		b := l.getBucket(key, now)
		l.refill(b, now)
		b.lastSeen = now

		if b.tokens >= 1 {
			b.tokens--
			l.mu.Unlock()
			return nil
		}

		waitFor := l.nextWaitDuration(b)
		l.mu.Unlock()

		t := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (l *perKeyLimiter) nextWaitDuration(b *keyBucket) time.Duration {
	if l.rate <= 0 {
		return time.Second
	}
	missing := 1 - b.tokens
	if missing < 0 {
		missing = 0
	}
	seconds := missing / l.rate
	if seconds <= 0 {
		seconds = 0.001
	}
	return time.Duration(seconds * float64(time.Second))
}

func (l *perKeyLimiter) getBucket(key string, now time.Time) *keyBucket {
	if b, ok := l.buckets[key]; ok {
		return b
	}
	b := &keyBucket{tokens: l.burst, lastFill: now, lastSeen: now}
	l.buckets[key] = b
	return b
}

func (l *perKeyLimiter) refill(b *keyBucket, now time.Time) {
	if l.rate <= 0 {
		b.lastFill = now
		return
	}
	elapsed := now.Sub(b.lastFill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.lastFill = now
}

func (l *perKeyLimiter) cleanup(now time.Time) {
	if l.keyTTL <= 0 {
		return
	}
	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) > l.keyTTL {
			delete(l.buckets, key)
		}
	}
}
