package aggregator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockRepo struct {
	mu      sync.Mutex
	batches [][]Aggregate
	savedCh chan struct{}
	failN   int
}

func (m *mockRepo) SaveBatch(_ context.Context, aggs []Aggregate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failN > 0 {
		m.failN--
		return errors.New("boom")
	}
	m.batches = append(m.batches, append([]Aggregate(nil), aggs...))
	if m.savedCh != nil {
		select {
		case m.savedCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func TestService_Smoke_AutoFlushRetryAndClose(t *testing.T) {
	repo := &mockRepo{savedCh: make(chan struct{}, 8)}
	svc, err := New(repo, Config{
		WindowSize:    time.Minute,
		QueueCap:      4,
		FlushInterval: 0,
		MaxKeys:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 1, 12, 0, 10, 0, time.UTC)

	if err := svc.Add(Event{Key: "a", Value: 2, Ts: base}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Add(Event{Key: "b", Value: 3, Ts: base}); err != nil {
		t.Fatal(err)
	}
	// После второго ключа должен сработать auto-flush по MaxKeys.
	<-repo.savedCh

	repo.mu.Lock()
	if len(repo.batches) != 1 {
		t.Fatalf("want 1 batch after auto-flush, got %d", len(repo.batches))
	}
	repo.mu.Unlock()

	// Ошибка в репозитории не должна потерять данные: первый Flush падает, второй успешен.
	repo.failN = 1
	if err := svc.Add(Event{Key: "a", Value: 5, Ts: base.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Flush(context.Background()); err == nil {
		t.Fatal("expected flush error")
	}
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := svc.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Add(Event{Key: "z", Value: 1, Ts: base}); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	var sumA0, sumB0, sumA1 int64
	for _, b := range repo.batches {
		for _, a := range b {
			switch {
			case a.WindowStart.Equal(base.Truncate(time.Minute)) && a.Key == "a":
				sumA0 += a.Sum
			case a.WindowStart.Equal(base.Truncate(time.Minute)) && a.Key == "b":
				sumB0 += a.Sum
			case a.WindowStart.Equal(base.Add(time.Minute).Truncate(time.Minute)) && a.Key == "a":
				sumA1 += a.Sum
			}
		}
	}
	if sumA0 != 2 || sumB0 != 3 || sumA1 != 5 {
		t.Fatalf("unexpected sums: a0=%d b0=%d a1=%d", sumA0, sumB0, sumA1)
	}
}
