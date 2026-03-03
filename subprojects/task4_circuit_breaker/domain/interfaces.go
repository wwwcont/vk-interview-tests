package domain

import (
	"context"
	"errors"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

type Breaker interface {
	Execute(ctx context.Context, fn func(context.Context) error) error
	State() State
}

var ErrBreakerOpen = errors.New("breaker open")

type IsFailure func(error) bool

type Config struct {
	WindowSize        int
	ErrorThreshold    float64
	ResetTimeout      time.Duration
	MaxProbe          int
	MinRequestsToTrip int
	IsFailure         IsFailure
}
