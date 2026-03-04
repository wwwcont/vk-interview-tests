package service

import (
	"context"
	"errors"

	"vk-interview-tests/internal/domain"
)

type Service struct{ balancer *domain.Balancer }

func New(b *domain.Balancer) *Service { return &Service{balancer: b} }

func (s *Service) UpsertBackend(id string, healthy bool, delayMS int, fail bool) {
	s.balancer.Upsert(id, healthy, delayMS, fail)
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
		return first.ID, errors.New("all attempts failed")
	}
	if err := domain.RunBackend(ctx, second); err == nil {
		return second.ID, nil
	}
	return second.ID, errors.New("all attempts failed")
}
