package interview

import "context"

type Tx interface {
	Commit() error
	Rollback() error
}

type Repo interface {
	BeginTx(ctx context.Context) (Tx, error)
	Create(ctx context.Context, tx Tx, order Order) error
}

type ExternalClient interface {
	Reserve(ctx context.Context, order Order) error
}

type Service interface {
	CreateOrder(ctx context.Context, order Order) error
}

type Order struct {
	ID string
}

type orderService struct {
	repo   Repo
	client ExternalClient
}

func NewOrderService(repo Repo, client ExternalClient) Service {
	return &orderService{repo: repo, client: client}
}

func (s *orderService) CreateOrder(ctx context.Context, order Order) (err error) {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		if rbErr := tx.Rollback(); rbErr != nil && err == nil {
			err = rbErr
		}
	}()

	if err = s.repo.Create(ctx, tx, order); err != nil {
		return err
	}

	if err = s.client.Reserve(ctx, order); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	committed = true
	return nil
}
