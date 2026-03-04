package domain

import (
	"errors"
	"time"
)

type JobType string

type Priority string

type Status string

const (
	Sleep     JobType  = "sleep"
	High      Priority = "high"
	Normal    Priority = "normal"
	Queued    Status   = "queued"
	Running   Status   = "running"
	Succeeded Status   = "succeeded"
	Failed    Status   = "failed"
	Canceled  Status   = "canceled"
)

var (
	ErrInvalidJobType  = errors.New("invalid job type")
	ErrInvalidPayload  = errors.New("invalid payload")
	ErrInvalidPriority = errors.New("invalid priority")
	ErrQueueFull       = errors.New("queue full")
	ErrShuttingDown    = errors.New("service is shutting down")
	ErrNotFound        = errors.New("job not found")
)

type SleepPayload struct {
	MS           int  `json:"ms"`
	Fail         bool `json:"fail,omitempty"`
	FailAttempts int  `json:"fail_attempts,omitempty"`
}

type Job struct {
	ID, LastError, IdempotencyKey string
	Type                          JobType
	Priority                      Priority
	Payload                       SleepPayload
	Status                        Status
	CreatedAt                     time.Time
	StartedAt, FinishedAt         *time.Time
	Attempts                      int
}

type Repo interface {
	Save(*Job)
	ByID(string) (*Job, error)
	ByKey(string) (*Job, error)
	Bind(string, string)
}

func NewJob(id string, t JobType, p SleepPayload, pr Priority, key string) (*Job, error) {
	if t != Sleep {
		return nil, ErrInvalidJobType
	}
	if pr != High && pr != Normal {
		return nil, ErrInvalidPriority
	}
	if p.MS < 0 {
		return nil, ErrInvalidPayload
	}
	return &Job{ID: id, Type: t, Payload: p, Priority: pr, Status: Queued, CreatedAt: time.Now().UTC(), IdempotencyKey: key}, nil
}
