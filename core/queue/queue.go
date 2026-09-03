package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrIntakeClosed    = errors.New("queue intake is closed")
	ErrDeliverySettled = errors.New("delivery is already settled")
)

type EnqueueOptions struct {
	ID        string
	NotBefore time.Time
}

type EnqueueOption func(*EnqueueOptions)

func WithID(id string) EnqueueOption {
	return func(options *EnqueueOptions) { options.ID = id }
}

func WithDelay(delay time.Duration) EnqueueOption {
	return func(options *EnqueueOptions) { options.NotBefore = time.Now().Add(delay) }
}

func WithNotBefore(notBefore time.Time) EnqueueOption {
	return func(options *EnqueueOptions) { options.NotBefore = notBefore }
}

func ApplyEnqueueOptions(options ...EnqueueOption) EnqueueOptions {
	var result EnqueueOptions
	for _, option := range options {
		option(&result)
	}
	return result
}

type Metadata struct {
	ID          string
	EnqueuedAt  time.Time
	DeliveredAt time.Time
	Attempt     uint64
}

type Delivery[T any] interface {
	Value() T
	Metadata() Metadata
	Ack(context.Context) error
	Requeue(context.Context, time.Duration) error
	Reject(context.Context) error
	Touch(context.Context) error
	Settled() bool
}

type Stats struct {
	Ready    int64
	Deferred int64
	InFlight int64
}

func (s Stats) Queued() int64 {
	return s.Ready + s.Deferred
}

type Backend[T any] interface {
	Enqueue(context.Context, T, EnqueueOptions) error
	Dequeue(context.Context) (Delivery[T], error)
	Stats(context.Context) (Stats, error)
	Close(context.Context) error
}

type Queue[T any] struct {
	id      int
	backend Backend[T]
	ctx     context.Context
	cancel  context.CancelCauseFunc

	mu           sync.RWMutex
	intakeClosed bool
	lastEnqueued time.Time
	closeOnce    sync.Once
	closeErr     error
}

func New[T any](id int, backend Backend[T]) *Queue[T] {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Queue[T]{id: id, backend: backend, ctx: ctx, cancel: cancel}
}

func (q *Queue[T]) ID() int {
	return q.id
}

func (q *Queue[T]) Context() context.Context {
	return q.ctx
}

func (q *Queue[T]) LastEnqueueTime() time.Time {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastEnqueued
}

func (q *Queue[T]) Enqueue(ctx context.Context, item T, options ...EnqueueOption) error {
	q.mu.RLock()
	if q.intakeClosed {
		q.mu.RUnlock()
		return ErrIntakeClosed
	}
	if err := q.ctx.Err(); err != nil {
		q.mu.RUnlock()
		return context.Cause(q.ctx)
	}
	if err := q.backend.Enqueue(ctx, item, ApplyEnqueueOptions(options...)); err != nil {
		q.mu.RUnlock()
		return err
	}
	q.mu.RUnlock()
	q.mu.Lock()
	q.lastEnqueued = time.Now()
	q.mu.Unlock()
	return nil
}

func (q *Queue[T]) Dequeue(ctx context.Context) (Delivery[T], error) {
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(q.ctx, func() { cancel(context.Cause(q.ctx)) })
	defer stop()
	defer cancel(nil)
	return q.backend.Dequeue(ctx)
}

func (q *Queue[T]) Stats(ctx context.Context) (Stats, error) {
	return q.backend.Stats(ctx)
}

func (q *Queue[T]) CloseIntake() {
	q.mu.Lock()
	q.intakeClosed = true
	q.mu.Unlock()
}

func (q *Queue[T]) Close(ctx context.Context) error {
	q.closeOnce.Do(func() {
		q.cancel(context.Canceled)
		q.closeErr = q.backend.Close(ctx)
	})
	return q.closeErr
}
