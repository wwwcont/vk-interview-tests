package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vk-interview-tests/subprojects/task1_balancer/domain"
)

type be struct {
	id string
	h  bool
}

func (b *be) ID() string    { return b.id }
func (b *be) Healthy() bool { return b.h }
func (b *be) Do(context.Context, domain.Request) (domain.Response, error) {
	return domain.Response{}, nil
}

func TestPickAndPenalty(t *testing.T) {
	a, b := &be{"a", true}, &be{"b", true}
	s := New([]domain.Backend{a, b})
	x, d, _ := s.Pick()
	d.Done(errors.New("x"), 100*time.Millisecond)
	y, _, _ := s.Pick()
	if x.ID() == y.ID() {
		t.Fatal("penalty should affect choice")
	}
}
func TestSkipUnhealthy(t *testing.T) {
	s := New([]domain.Backend{&be{"a", false}})
	if _, _, err := s.Pick(); err == nil {
		t.Fatal("want err")
	}
}
func TestConcurrentPick(t *testing.T) {
	s := New([]domain.Backend{&be{"a", true}, &be{"b", true}})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, d, err := s.Pick()
			if err == nil {
				d.Done(nil, time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
