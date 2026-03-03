package interview

import (
	"context"
	"sync"
)

type Event struct {
	Key   string
	Value int64
}

type AggRepo interface {
	SaveBatch(ctx context.Context, data map[string]int64) error
}

type Aggregator interface {
	Add(e Event)
	Flush(ctx context.Context) error
}

type aggregator struct {
	repo AggRepo

	mu   sync.Mutex
	data map[string]int64
}

func NewAggregator(repo AggRepo) Aggregator {
	return &aggregator{
		repo: repo,
		data: make(map[string]int64),
	}
}

func (a *aggregator) Add(e Event) {
	a.mu.Lock()
	a.data[e.Key] += e.Value
	a.mu.Unlock()
}

func (a *aggregator) Flush(ctx context.Context) error {
	a.mu.Lock()
	batch := a.data
	a.data = make(map[string]int64)
	a.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	if err := a.repo.SaveBatch(ctx, batch); err != nil {
		a.mu.Lock()
		for k, v := range batch {
			a.data[k] += v
		}
		a.mu.Unlock()
		return err
	}

	return nil
}
