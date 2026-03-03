package domain

import (
	"context"
	"errors"
	"time"
)

type Backend interface {
	ID() string
	Do(ctx context.Context, req Request) (Response, error)
	Healthy() bool
}

type Request struct{}
type Response struct{}

type Picked interface {
	Done(err error, latency time.Duration)
}

type Balancer interface {
	Pick() (Backend, Picked, error)
	Update(backends []Backend)
}

var ErrNoHealthyBackend = errors.New("no healthy backend")

const (
	DefaultLatencyAlpha    = 0.2
	DefaultPenaltyOnError  = 1.0
	DefaultPenaltyRecovery = 0.15
)
