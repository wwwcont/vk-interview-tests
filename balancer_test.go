package interview

import (
	"context"
	"sync"
	"testing"
)

type backendMock struct {
	id      string
	healthy bool
}

func (b *backendMock) ID() string                                    { return b.id }
func (b *backendMock) Healthy() bool                                 { return b.healthy }
func (b *backendMock) Do(context.Context, Request) (Response, error) { return Response{}, nil }

func TestBalancerRoundRobinOrder(t *testing.T) {
	b1 := &backendMock{id: "b1", healthy: true}
	b2 := &backendMock{id: "b2", healthy: true}
	b3 := &backendMock{id: "b3", healthy: true}

	bal := NewRoundRobinBalancer([]Backend{b1, b2, b3})

	got := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		be, err := bal.Next()
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		got = append(got, be.ID())
	}

	want := []string{"b1", "b2", "b3", "b1", "b2", "b3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestBalancerSkipsUnhealthy(t *testing.T) {
	b1 := &backendMock{id: "b1", healthy: false}
	b2 := &backendMock{id: "b2", healthy: true}
	bal := NewRoundRobinBalancer([]Backend{b1, b2})

	for i := 0; i < 4; i++ {
		be, err := bal.Next()
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if be.ID() != "b2" {
			t.Fatalf("got backend %s, want b2", be.ID())
		}
	}
}

func TestBalancerConcurrentNext(t *testing.T) {
	b1 := &backendMock{id: "b1", healthy: true}
	b2 := &backendMock{id: "b2", healthy: true}
	bal := NewRoundRobinBalancer([]Backend{b1, b2})

	const calls = 1000
	var wg sync.WaitGroup
	var mu sync.Mutex
	counts := map[string]int{}

	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			be, err := bal.Next()
			if err != nil {
				t.Errorf("Next() error = %v", err)
				return
			}
			mu.Lock()
			counts[be.ID()]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if counts["b1"]+counts["b2"] != calls {
		t.Fatalf("total calls = %d, want %d", counts["b1"]+counts["b2"], calls)
	}
}

func TestBalancerUpdateReplacesBackends(t *testing.T) {
	b1 := &backendMock{id: "b1", healthy: true}
	bal := NewRoundRobinBalancer([]Backend{b1})

	be, err := bal.Next()
	if err != nil || be.ID() != "b1" {
		t.Fatalf("initial Next() = (%v, %v), want b1,nil", be, err)
	}

	b2 := &backendMock{id: "b2", healthy: true}
	bal.Update([]Backend{b2})

	for i := 0; i < 3; i++ {
		be, err = bal.Next()
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if be.ID() != "b2" {
			t.Fatalf("got backend %s, want b2", be.ID())
		}
	}
}
