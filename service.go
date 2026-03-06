package order

// Пакет реализует минимальный сервис создания заказов для сценария "БД + внешний сервис".
// Сервис нужен, чтобы безопасно провести заказ через два шага: сначала сохранить PENDING
// в транзакции (для консистентности и идемпотентности), а затем уже вне транзакции
// вызвать внешний Reserve и финализировать статус в CONFIRMED/FAILED.
// Такой порядок не держит транзакцию во время сетевого вызова и снижает риск блокировок.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// Доменные статусы заказа.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusConfirmed Status = "CONFIRMED"
	StatusFailed    Status = "FAILED"
)

// Заказ в системе.
type Order struct {
	ID             string
	IdempotencyKey string
	Amount         int64
	Status         Status
}

// Входные данные на создание заказа.
type CreateOrderRequest struct {
	IdempotencyKey string
	Amount         int64
}

// Ошибки бизнес-уровня.
var ErrInvalidRequest = errors.New("invalid request")
var ErrAlreadyFailed = errors.New("order already failed")

// Минимальный интерфейс транзакции.
type Tx interface {
	Commit() error
	Rollback() error
}

// Репозиторий хранения заказов.
type Repo interface {
	BeginTx(ctx context.Context) (Tx, error)
	GetByIdempotencyKey(ctx context.Context, key string) (Order, bool, error)
	CreatePending(ctx context.Context, tx Tx, o Order) error
	UpdateStatus(ctx context.Context, id string, st Status, lastErr string) error
}

// Внешний клиент (резерв/оплата).
type ExternalClient interface {
	Reserve(ctx context.Context, orderID string, amount int64) error
}

// Сервис работы с заказами.
type Service interface {
	CreateOrder(ctx context.Context, req CreateOrderRequest) (Order, error)
}

// orderService — простая реализация use-case.
type orderService struct {
	repo   Repo
	client ExternalClient
	idGen  func() (string, error)
}

// NewService создает сервис с дефолтным генератором ID.
func NewService(repo Repo, client ExternalClient) Service {
	return &orderService{
		repo:   repo,
		client: client,
		idGen:  generateID,
	}
}

// CreateOrder реализует сценарий:
// 1) валидация,
// 2) идемпотентная проверка,
// 3) создание PENDING в транзакции,
// 4) внешний Reserve ВНЕ транзакции,
// 5) финализация статуса.
func (s *orderService) CreateOrder(ctx context.Context, req CreateOrderRequest) (Order, error) {
	// Базовая валидация входа.
	if req.Amount <= 0 || req.IdempotencyKey == "" {
		return Order{}, ErrInvalidRequest
	}

	// Идемпотентность: если заказ уже есть, возвращаем его без повторного внешнего вызова.
	existing, ok, err := s.repo.GetByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return Order{}, fmt.Errorf("get by idempotency key: %w", err)
	}
	if ok {
		switch existing.Status {
		case StatusPending, StatusConfirmed:
			return existing, nil
		case StatusFailed:
			return existing, ErrAlreadyFailed
		default:
			return Order{}, fmt.Errorf("unknown status: %s", existing.Status)
		}
	}

	// Транзакция только для создания PENDING.
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return Order{}, fmt.Errorf("begin tx: %w", err)
	}

	orderID, err := s.idGen()
	if err != nil {
		_ = tx.Rollback()
		return Order{}, fmt.Errorf("generate id: %w", err)
	}
	created := Order{
		ID:             orderID,
		IdempotencyKey: req.IdempotencyKey,
		Amount:         req.Amount,
		Status:         StatusPending,
	}

	if err := s.repo.CreatePending(ctx, tx, created); err != nil {
		_ = tx.Rollback()
		return Order{}, fmt.Errorf("create pending: %w", err)
	}
	if err := tx.Commit(); err != nil {
		// Важно: при ошибке коммита не идем во внешний сервис.
		_ = tx.Rollback()
		return Order{}, fmt.Errorf("commit tx: %w", err)
	}

	// Внешний вызов делаем после коммита (транзакция уже закрыта).
	if err := s.client.Reserve(ctx, created.ID, created.Amount); err != nil {
		created.Status = StatusFailed
		if updErr := s.repo.UpdateStatus(ctx, created.ID, StatusFailed, err.Error()); updErr != nil {
			return created, fmt.Errorf("reserve failed: %v; update status failed: %w", err, updErr)
		}
		return created, fmt.Errorf("reserve: %w", err)
	}

	created.Status = StatusConfirmed
	if err := s.repo.UpdateStatus(ctx, created.ID, StatusConfirmed, ""); err != nil {
		return created, fmt.Errorf("update confirmed status: %w", err)
	}

	return created, nil
}

// generateID делает короткий случайный ID без сторонних библиотек.
func generateID() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
