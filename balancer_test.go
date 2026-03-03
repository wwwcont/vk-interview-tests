package interview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type backendMock struct {
	id      string
	healthy bool
	calls   int32
}

func (b *backendMock) ID() string    { return b.id }
func (b *backendMock) Healthy() bool { return b.healthy }
func (b *backendMock) Do(context.Context, Request) (Response, error) {
	atomic.AddInt32(&b.calls, 1)
	return Response{}, nil
}

func TestLeastLoadDistribution(t *testing.T) {
	b1 := &backendMock{id: "b1", healthy: true}
	b2 := &backendMock{id: "b2", healthy: true}
	bal := NewLeastLoadBalancer([]Backend{b1, b2})

	beA, doneA, err := bal.Pick()
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	beB, doneB, err := bal.Pick()
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if beA.ID() == beB.ID() {
		t.Fatalf("expected different backends for first two picks, got same %s", beA.ID())
	}

	doneA.Done(nil, 10*time.Millisecond)
	doneB.Done(nil, 10*time.Millisecond)
}

func TestLeastLoadSkipsUnhealthy(t *testing.T) {
	b1 := &backendMock{id: "bad", healthy: false}
	b2 := &backendMock{id: "good", healthy: true}
	bal := NewLeastLoadBalancer([]Backend{b1, b2})

	for i := 0; i < 5; i++ {
		be, done, err := bal.Pick()
		if err != nil {
			t.Fatalf("Pick() error = %v", err)
		}
		if be.ID() != "good" {
			t.Fatalf("Pick() = %s, want good", be.ID())
		}
		done.Done(nil, 2*time.Millisecond)
	}
}

func TestLeastLoadConcurrentPick(t *testing.T) {
	backends := []Backend{
		&backendMock{id: "a", healthy: true},
		&backendMock{id: "b", healthy: true},
		&backendMock{id: "c", healthy: true},
	}
	bal := NewLeastLoadBalancer(backends)

	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, done, err := bal.Pick()
			if err != nil {
				t.Errorf("Pick() error = %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			done.Done(nil, time.Millisecond)
		}()
	}
	wg.Wait()
}

func TestLeastLoadLatencyPenaltyAffectsChoice(t *testing.T) {
	fast := &backendMock{id: "fast", healthy: true}
	slow := &backendMock{id: "slow", healthy: true}
	bal := NewLeastLoadBalancer([]Backend{slow, fast})

	be, done, err := bal.Pick()
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if be.ID() != "slow" {
		t.Fatalf("first Pick() = %s, want slow", be.ID())
	}
	done.Done(nil, 500*time.Millisecond)

	be, done, err = bal.Pick()
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if be.ID() != "fast" {
		t.Fatalf("second Pick() = %s, want fast due to penalty", be.ID())
	}
	done.Done(nil, 5*time.Millisecond)
}
