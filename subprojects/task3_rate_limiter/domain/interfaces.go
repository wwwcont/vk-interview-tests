package domain

import "context"

type Limiter interface {
	Allow(key string) bool
	Acquire(ctx context.Context, key string) error
}

const DefaultCleanupBudget = 32
