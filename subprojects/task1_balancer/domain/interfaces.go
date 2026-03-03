package domain

import (
	"context"
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
