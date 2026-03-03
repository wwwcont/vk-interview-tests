package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"vk-interview-tests/subprojects/task4_circuit_breaker/domain"
)

func TestTransitions(t *testing.T) {
	b := New(domain.Config{WindowSize: 4, MinRequestsToTrip: 4, ErrorThreshold: .5, ResetTimeout: 30 * time.Millisecond, MaxProbe: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("x") })
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("x") })
	_ = b.Execute(context.Background(), func(context.Context) error { return nil })
	_ = b.Execute(context.Background(), func(context.Context) error { return nil })
	if b.State() != domain.Open {
		t.Fatal("want open")
	}
	time.Sleep(40 * time.Millisecond)
	if b.State() != domain.HalfOpen {
		t.Fatal("want half")
	}
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if b.State() != domain.Closed {
		t.Fatal("want closed")
	}
}

func TestPredicate(t *testing.T) {
	b := New(domain.Config{WindowSize: 2, MinRequestsToTrip: 2, ErrorThreshold: .5, IsFailure: func(err error) bool { return err != nil && err.Error() == "fatal" }})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("soft") })
	_ = b.Execute(context.Background(), func(context.Context) error { return nil })
	if b.State() != domain.Closed {
		t.Fatal("predicate broken")
	}
}
