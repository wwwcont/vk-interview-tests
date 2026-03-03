package interview

import (
	"context"
	"errors"
	"testing"
)

type txMock struct {
	commitErr   error
	rollbackErr error

	commitCalled   int
	rollbackCalled int
}

func (m *txMock) Commit() error {
	m.commitCalled++
	return m.commitErr
}

func (m *txMock) Rollback() error {
	m.rollbackCalled++
	return m.rollbackErr
}

type repoMock struct {
	tx         *txMock
	beginErr   error
	createErr  error
	beginCalls int
	createCall int
}

func (m *repoMock) BeginTx(context.Context) (Tx, error) {
	m.beginCalls++
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return m.tx, nil
}

func (m *repoMock) Create(context.Context, Tx, Order) error {
	m.createCall++
	return m.createErr
}

type externalMock struct {
	reserveErr   error
	reserveCalls int
}

func (m *externalMock) Reserve(context.Context, Order) error {
	m.reserveCalls++
	return m.reserveErr
}

func TestOrderServiceCreateOrderBeginTxError(t *testing.T) {
	repo := &repoMock{beginErr: errors.New("begin")}
	svc := NewOrderService(repo, &externalMock{})

	err := svc.CreateOrder(context.Background(), Order{ID: "1"})
	if err == nil || err.Error() != "begin" {
		t.Fatalf("CreateOrder() err = %v, want begin", err)
	}
}

func TestOrderServiceCreateOrderCreateError(t *testing.T) {
	tx := &txMock{}
	repo := &repoMock{tx: tx, createErr: errors.New("create")}
	ext := &externalMock{}
	svc := NewOrderService(repo, ext)

	err := svc.CreateOrder(context.Background(), Order{ID: "1"})
	if err == nil || err.Error() != "create" {
		t.Fatalf("CreateOrder() err = %v, want create", err)
	}
	if tx.rollbackCalled != 1 {
		t.Fatalf("rollbackCalled = %d, want 1", tx.rollbackCalled)
	}
	if tx.commitCalled != 0 {
		t.Fatalf("commitCalled = %d, want 0", tx.commitCalled)
	}
	if ext.reserveCalls != 0 {
		t.Fatalf("reserveCalls = %d, want 0", ext.reserveCalls)
	}
}

func TestOrderServiceCreateOrderReserveError(t *testing.T) {
	tx := &txMock{}
	repo := &repoMock{tx: tx}
	ext := &externalMock{reserveErr: errors.New("reserve")}
	svc := NewOrderService(repo, ext)

	err := svc.CreateOrder(context.Background(), Order{ID: "1"})
	if err == nil || err.Error() != "reserve" {
		t.Fatalf("CreateOrder() err = %v, want reserve", err)
	}
	if tx.rollbackCalled != 1 {
		t.Fatalf("rollbackCalled = %d, want 1", tx.rollbackCalled)
	}
	if tx.commitCalled != 0 {
		t.Fatalf("commitCalled = %d, want 0", tx.commitCalled)
	}
}

func TestOrderServiceCreateOrderCommitError(t *testing.T) {
	tx := &txMock{commitErr: errors.New("commit")}
	repo := &repoMock{tx: tx}
	svc := NewOrderService(repo, &externalMock{})

	err := svc.CreateOrder(context.Background(), Order{ID: "1"})
	if err == nil || err.Error() != "commit" {
		t.Fatalf("CreateOrder() err = %v, want commit", err)
	}
	if tx.commitCalled != 1 {
		t.Fatalf("commitCalled = %d, want 1", tx.commitCalled)
	}
	if tx.rollbackCalled != 1 {
		t.Fatalf("rollbackCalled = %d, want 1", tx.rollbackCalled)
	}
}

func TestOrderServiceCreateOrderSuccess(t *testing.T) {
	tx := &txMock{}
	repo := &repoMock{tx: tx}
	ext := &externalMock{}
	svc := NewOrderService(repo, ext)

	if err := svc.CreateOrder(context.Background(), Order{ID: "1"}); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	if repo.beginCalls != 1 || repo.createCall != 1 || ext.reserveCalls != 1 {
		t.Fatalf("unexpected calls: begin=%d create=%d reserve=%d", repo.beginCalls, repo.createCall, ext.reserveCalls)
	}
	if tx.commitCalled != 1 {
		t.Fatalf("commitCalled = %d, want 1", tx.commitCalled)
	}
	if tx.rollbackCalled != 0 {
		t.Fatalf("rollbackCalled = %d, want 0", tx.rollbackCalled)
	}
}
