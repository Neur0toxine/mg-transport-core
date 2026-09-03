package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrIntakeClosed is returned by Enqueue after CloseIntake was called. Existing items can still be drained.
	ErrIntakeClosed = errors.New("queue intake is closed")
	// ErrDeliverySettled is returned when a delivery is acknowledged, requeued, or rejected a second time.
	ErrDeliverySettled = errors.New("delivery is already settled")
)

// EnqueueOptions controls how an item is enqueued by a Backend.
type EnqueueOptions struct {
	// ID is the caller-provided delivery identity. Backends that support deduplication use it as the
	// message ID; when empty a backend-generated ID is used.
	ID string
	// NotBefore defers the item until the given time. A zero value makes the item immediately ready.
	// Backends without native scheduling emulate it with delayed delivery.
	NotBefore time.Time
}

// EnqueueOption mutates EnqueueOptions during Enqueue.
type EnqueueOption func(*EnqueueOptions)

// WithID assigns a stable delivery ID, enabling deduplication in supporting backends.
func WithID(id string) EnqueueOption {
	return func(options *EnqueueOptions) { options.ID = id }
}

// WithDelay defers the item by a fixed duration counted from the current time.
func WithDelay(delay time.Duration) EnqueueOption {
	return func(options *EnqueueOptions) { options.NotBefore = time.Now().Add(delay) }
}

// WithNotBefore defers the item until the given absolute time.
func WithNotBefore(notBefore time.Time) EnqueueOption {
	return func(options *EnqueueOptions) { options.NotBefore = notBefore }
}

// ApplyEnqueueOptions folds the given options into an EnqueueOptions value. Backends call it internally;
// it is exported mostly for tests and custom Backend implementations.
func ApplyEnqueueOptions(options ...EnqueueOption) EnqueueOptions {
	var result EnqueueOptions
	for _, option := range options {
		option(&result)
	}
	return result
}

// Metadata describes a single delivery attempt. Attempt usually starts at 1 on the first handout and
// grows with every requeue-driven redelivery.
type Metadata struct {
	ID          string
	EnqueuedAt  time.Time
	DeliveredAt time.Time
	Attempt     uint64
}

// Delivery is a single dequeued item handed to a Processor. Exactly one of Ack, Requeue, or Reject must
// eventually be called; every method returns ErrDeliverySettled after the first successful settlement.
// Touch extends the backend acknowledgment lease and does not settle the delivery.
type Delivery[T any] interface {
	// Value returns the decoded item.
	Value() T
	// Metadata returns delivery identity and timing information.
	Metadata() Metadata
	// Ack marks the item as successfully processed.
	Ack(context.Context) error
	// Requeue schedules a retry after the given delay.
	Requeue(context.Context, time.Duration) error
	// Reject discards the item without a retry.
	Reject(context.Context) error
	// Touch renews the acknowledgment lease for long-running processing.
	Touch(context.Context) error
	// Settled reports whether the delivery was already settled.
	Settled() bool
}

// Stats is a snapshot of the queue workload counters.
type Stats struct {
	// Ready is the number of items available for immediate delivery.
	Ready int64
	// Deferred is the number of items waiting for their NotBefore time.
	Deferred int64
	// InFlight is the number of delivered items not yet settled.
	InFlight int64
}

// Queued returns the total number of items not yet handed to workers (Ready plus Deferred).
func (s Stats) Queued() int64 {
	return s.Ready + s.Deferred
}

// Backend stores items and hands out deliveries. The interface is intentionally small so that radically
// different storages (in-memory, beanstalkd, NATS JetStream) can implement it; implementations live in
// the memory, beanstalk, and nats subpackages. Dequeue blocks until a delivery is available, the context
// is canceled, or the backend is closed.
type Backend[T any] interface {
	Enqueue(context.Context, T, EnqueueOptions) error
	Dequeue(context.Context) (Delivery[T], error)
	Stats(context.Context) (Stats, error)
	Close(context.Context) error
}

// Queue pairs a Backend with lifecycle management for a single queue ID: it tracks the last enqueue
// time, can close the intake for graceful shutdown, and cancels in-flight dequeues once closed.
// Queues are normally not created directly but owned by an Executor, which in turn is managed by a Store.
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

// New creates a Queue with the given numeric ID and backend.
func New[T any](id int, backend Backend[T]) *Queue[T] {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Queue[T]{id: id, backend: backend, ctx: ctx, cancel: cancel}
}

// ID returns the queue identifier passed to New.
func (q *Queue[T]) ID() int {
	return q.id
}

// Context returns the queue lifetime context. It is canceled when the queue is closed and is used as the
// parent context of the queue workers.
func (q *Queue[T]) Context() context.Context {
	return q.ctx
}

// LastEnqueueTime returns the time of the last successful enqueue, or the zero time if nothing was
// enqueued yet. Scaling policies may use it to retire idle workers.
func (q *Queue[T]) LastEnqueueTime() time.Time {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastEnqueued
}

// Enqueue adds an item to the backend. It returns ErrIntakeClosed after CloseIntake, and the queue
// cancellation cause after Close. Options are applied before reaching the backend.
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

// Dequeue waits for the next backend delivery. The call is aborted when either the passed context or the
// queue itself is closed, so workers stop promptly during shutdown.
func (q *Queue[T]) Dequeue(ctx context.Context) (Delivery[T], error) {
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(q.ctx, func() { cancel(context.Cause(q.ctx)) })
	defer stop()
	defer cancel(nil)
	return q.backend.Dequeue(ctx)
}

// Stats returns the backend workload counters.
func (q *Queue[T]) Stats(ctx context.Context) (Stats, error) {
	return q.backend.Stats(ctx)
}

// CloseIntake rejects further Enqueue calls while allowing workers to drain already enqueued items.
// It is the first phase of a graceful shutdown.
func (q *Queue[T]) CloseIntake() {
	q.mu.Lock()
	q.intakeClosed = true
	q.mu.Unlock()
}

// Close cancels the queue context (aborting in-flight dequeues) and closes the backend. It is
// idempotent: subsequent calls return the first error without repeating the work.
func (q *Queue[T]) Close(ctx context.Context) error {
	q.closeOnce.Do(func() {
		q.cancel(context.Canceled)
		q.closeErr = q.backend.Close(ctx)
	})
	return q.closeErr
}
