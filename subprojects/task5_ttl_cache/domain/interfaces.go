package domain

import (
	"context"
	"time"
)

type Cache interface {
	Get(key string) (any, bool)
	Set(key string, value any, ttl time.Duration)
	GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error)
}
