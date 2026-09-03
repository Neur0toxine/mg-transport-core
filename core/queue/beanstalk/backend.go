package beanstalk

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
	"github.com/retailcrm/mg-transport-core/v2/core/queue"
)

type Options struct {
	Priority    uint32
	TTR         time.Duration
	PollTimeout time.Duration
}

type Backend[T any] struct {
	manager ManagerInterface
	codec   queue.Codec[T]
	options Options
	closed  atomic.Bool
}

type envelope struct {
	ID         string    `json:"id"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
	Payload    []byte    `json:"payload"`
}

func New[T any](manager ManagerInterface, codec queue.Codec[T], options Options) *Backend[T] {
	if options.TTR <= 0 {
		options.TTR = time.Minute
	}
	if options.PollTimeout <= 0 {
		options.PollTimeout = time.Second
	}
	return &Backend[T]{manager: manager, codec: codec, options: options}
}

func (b *Backend[T]) Enqueue(ctx context.Context, value T, options queue.EnqueueOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := b.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode beanstalk delivery: %w", err)
	}
	now := time.Now()
	id := options.ID
	if id == "" {
		id = fmt.Sprintf("beanstalk-%d", now.UnixNano())
	}
	body, err := json.Marshal(envelope{ID: id, EnqueuedAt: now, Payload: payload})
	if err != nil {
		return fmt.Errorf("encode beanstalk envelope: %w", err)
	}
	delay := max(time.Until(options.NotBefore), 0)
	_, err = b.manager.Put(body, b.options.Priority, delay, b.options.TTR)
	return err
}

func (b *Backend[T]) Dequeue(ctx context.Context) (queue.Delivery[T], error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id, body, err := b.manager.Reserve(b.options.PollTimeout)
		if errors.Is(err, beanstalk.ErrTimeout) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var message envelope
		if err := json.Unmarshal(body, &message); err != nil {
			_ = b.manager.Delete(id)
			return nil, fmt.Errorf("decode beanstalk envelope: %w", err)
		}
		value, err := b.codec.Decode(message.Payload)
		if err != nil {
			_ = b.manager.Delete(id)
			return nil, fmt.Errorf("decode beanstalk delivery: %w", err)
		}
		attempt, err := b.manager.Attempts(id)
		if err != nil {
			_ = b.manager.Release(id, b.options.Priority, 0)
			return nil, fmt.Errorf("read beanstalk delivery metadata: %w", err)
		}
		return &delivery[T]{backend: b, jobID: id, value: value, metadata: queue.Metadata{ID: message.ID, EnqueuedAt: message.EnqueuedAt, DeliveredAt: time.Now(), Attempt: attempt}}, nil
	}
}

func (b *Backend[T]) Stats(ctx context.Context) (queue.Stats, error) {
	if err := ctx.Err(); err != nil {
		return queue.Stats{}, err
	}
	stats, err := b.manager.Stats()
	return queue.Stats{Ready: stats.Ready, Deferred: stats.Delayed, InFlight: stats.Reserved}, err
}
func (b *Backend[T]) Close(context.Context) error {
	b.closed.Store(true)
	return b.manager.Close()
}

type delivery[T any] struct {
	backend  *Backend[T]
	jobID    uint64
	value    T
	metadata queue.Metadata
	settled  atomic.Bool
}

func (d *delivery[T]) Value() T {
	return d.value
}

func (d *delivery[T]) Metadata() queue.Metadata {
	return d.metadata
}

func (d *delivery[T]) Settled() bool {
	return d.settled.Load()
}
func (d *delivery[T]) terminal(operation func() error) error {
	if !d.settled.CompareAndSwap(false, true) {
		return queue.ErrDeliverySettled
	}
	if err := operation(); err != nil {
		d.settled.Store(false)
		return err
	}
	return nil
}
func (d *delivery[T]) Ack(context.Context) error {
	return d.terminal(func() error { return d.backend.manager.Delete(d.jobID) })
}
func (d *delivery[T]) Reject(ctx context.Context) error {
	return d.Ack(ctx)
}
func (d *delivery[T]) Requeue(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.terminal(func() error { return d.backend.manager.Release(d.jobID, d.backend.options.Priority, delay) })
}
func (d *delivery[T]) Touch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Settled() {
		return queue.ErrDeliverySettled
	}
	return d.backend.manager.Touch(d.jobID)
}

var _ queue.Backend[int] = (*Backend[int])(nil)
