package domain

import "context"

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
