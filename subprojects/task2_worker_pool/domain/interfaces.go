package domain

import (
	"context"
	"errors"
)

type Priority int

const (
	High Priority = iota
	Normal
)

type Task func(ctx context.Context) error

type Pool interface {
	Submit(ctx context.Context, p Priority, task Task) error
	Resize(n int)
	Close(ctx context.Context) error
}

var ErrPoolClosed = errors.New("pool closed")

const DefaultQueueCapacity = 256
