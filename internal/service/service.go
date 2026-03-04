package service

import (
	"context"
	"errors"

	"vk-interview-tests/internal/domain"
)

var ErrAllAttemptsFailed = errors.New("all attempts failed")

type Service struct{ balancer *domain.Balancer }

func New(b *domain.Balancer) *Service { return &Service{balancer: b} }

func (s *Service) UpsertBackend(id string, healthy bool, delayMS int, fail bool) {
	s.balancer.Upsert(id, healthy, delayMS, fail)
}

func (s *Service) UpsertBackendWithInvoker(id string, healthy bool, invoker domain.Invoker) {
	s.balancer.UpsertWithInvoker(id, healthy, invoker)
}

func (s *Service) SetBackendHealth(id string, healthy bool) bool {
	return s.balancer.SetHealth(id, healthy)
}

func (s *Service) Stats() []domain.Snapshot { return s.balancer.Stats() }

func (s *Service) DoRequest(ctx context.Context) (string, error) {
	first, ok := s.balancer.Pick("")
	if !ok {
		return "", domain.ErrNoHealthyBackend
	}
	if err := domain.RunBackend(ctx, first); err == nil {
		return first.ID, nil
	}
	second, ok := s.balancer.Pick(first.ID)
	if !ok {
		return first.ID, ErrAllAttemptsFailed
	}
	if err := domain.RunBackend(ctx, second); err == nil {
		return second.ID, nil
	}
	return second.ID, ErrAllAttemptsFailed
}
