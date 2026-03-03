package interview

import (
	"context"
	"sync"
)

// Задача 2: реализовать Aggregator для накопления событий в памяти,
// агрегации по ключу и отправки батчем через репозиторий.
// Flush должен быть потокобезопасным и не удерживать lock во время SaveBatch.
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
	batch := a.takeSnapshot()
	if len(batch) == 0 {
		return nil
	}

	// Передаём в repo копию, чтобы внешняя реализация не могла случайно
	// изменить наш snapshot (и потенциально сломать повторную постановку при ошибке).
	forRepo := cloneInt64Map(batch)
	if err := a.repo.SaveBatch(ctx, forRepo); err != nil {
		a.restore(batch)
		return err
	}

	return nil
}

func (a *aggregator) takeSnapshot() map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.data) == 0 {
		return nil
	}

	batch := a.data
	a.data = make(map[string]int64)
	return batch
}

func (a *aggregator) restore(batch map[string]int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, v := range batch {
		a.data[k] += v
	}
}

func cloneInt64Map(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}
