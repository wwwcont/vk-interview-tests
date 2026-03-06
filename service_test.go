package order

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// mockTx хранит staged-заказ до Commit и позволяет симулировать ошибку коммита.
type mockTx struct {
	repo      *mockRepo
	pending   *Order
	committed bool
	rolled    bool
}

func (t *mockTx) Commit() error {
	if t.repo.commitErr != nil {
		return t.repo.commitErr
	}
	t.committed = true
	if t.pending != nil {
		t.repo.ordersByID[t.pending.ID] = *t.pending
		t.repo.byKey[t.pending.IdempotencyKey] = t.pending.ID
	}
	return nil
}

func (t *mockTx) Rollback() error {
	t.rolled = true
	t.pending = nil
	return nil
}

// mockRepo — in-memory mock с минимальной логикой.
type mockRepo struct {
	mu sync.Mutex

	beginErr  error
	commitErr error

	ordersByID map[string]Order
	byKey      map[string]string
	lastErr    map[string]string
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		ordersByID: make(map[string]Order),
		byKey:      make(map[string]string),
		lastErr:    make(map[string]string),
	}
}

func (r *mockRepo) BeginTx(_ context.Context) (Tx, error) {
	if r.beginErr != nil {
		return nil, r.beginErr
	}
	return &mockTx{repo: r}, nil
}

func (r *mockRepo) GetByIdempotencyKey(_ context.Context, key string) (Order, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.byKey[key]
	if !ok {
		return Order{}, false, nil
	}
	ord, exists := r.ordersByID[id]
	if !exists {
		return Order{}, false, nil
	}
	return ord, true, nil
}

func (r *mockRepo) CreatePending(_ context.Context, tx Tx, o Order) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	mtx, ok := tx.(*mockTx)
	if !ok {
		return errors.New("unexpected tx type")
	}
	if _, exists := r.byKey[o.IdempotencyKey]; exists {
		return errors.New("duplicate idempotency key")
	}
	copyOrder := o
	mtx.pending = &copyOrder
	return nil
}

func (r *mockRepo) UpdateStatus(_ context.Context, id string, st Status, lastErr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ord, ok := r.ordersByID[id]
	if !ok {
		return errors.New("order not found")
	}
	ord.Status = st
	r.ordersByID[id] = ord
	r.lastErr[id] = lastErr
	return nil
}

// mockClient считает количество вызовов Reserve и умеет возвращать ошибку.
type mockClient struct {
	mu        sync.Mutex
	calls     int
	reserveErr error
}

func (c *mockClient) Reserve(_ context.Context, _ string, _ int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.reserveErr
}

func (c *mockClient) CallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestCreateOrder_SuccessAndIdempotent(t *testing.T) {
	repo := newMockRepo()
	client := &mockClient{}
	svc := &orderService{
		repo:   repo,
		client: client,
		idGen: func() (string, error) {
			return "ord-1", nil
		},
	}

	ctx := context.Background()
	req := CreateOrderRequest{IdempotencyKey: "k-1", Amount: 100}

	first, err := svc.CreateOrder(ctx, req)
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	if first.Status != StatusConfirmed {
		t.Fatalf("expected CONFIRMED, got %s", first.Status)
	}
	if got := client.CallCount(); got != 1 {
		t.Fatalf("expected one Reserve call, got %d", got)
	}

	second, err := svc.CreateOrder(ctx, req)
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("expected same order for idempotent request, got %q and %q", first.ID, second.ID)
	}
	if got := client.CallCount(); got != 1 {
		t.Fatalf("expected still one Reserve call, got %d", got)
	}
}

func TestCreateOrder_CommitErrorDoesNotCallExternal(t *testing.T) {
	repo := newMockRepo()
	repo.commitErr = errors.New("commit boom")
	client := &mockClient{}
	svc := &orderService{
		repo:   repo,
		client: client,
		idGen: func() (string, error) {
			return "ord-2", nil
		},
	}

	_, err := svc.CreateOrder(context.Background(), CreateOrderRequest{
		IdempotencyKey: "k-2",
		Amount:         42,
	})
	if err == nil {
		t.Fatal("expected error on commit")
	}
	if got := client.CallCount(); got != 0 {
		t.Fatalf("reserve must not be called after commit error, got %d", got)
	}
	if _, ok := repo.byKey["k-2"]; ok {
		t.Fatal("order should not be visible on failed commit")
	}
}
