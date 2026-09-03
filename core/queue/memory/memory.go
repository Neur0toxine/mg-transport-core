package memory

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/retailcrm/mg-transport-core/v2/core/queue"
)

type Options struct {
	AckWait time.Duration
}

type item[T any] struct {
	id         string
	value      T
	enqueuedAt time.Time
	notBefore  time.Time
	attempt    uint64
	index      int
}

type delayedHeap[T any] []*item[T]

func (h delayedHeap[T]) Len() int           { return len(h) }
func (h delayedHeap[T]) Less(i, j int) bool { return h[i].notBefore.Before(h[j].notBefore) }
func (h delayedHeap[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index, h[j].index = i, j }
func (h *delayedHeap[T]) Push(value any)    { *h = append(*h, value.(*item[T])) }
func (h *delayedHeap[T]) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

type Memory[T any] struct {
	mu        sync.Mutex
	notify    chan struct{}
	closedCh  chan struct{}
	ready     []*item[T]
	delayed   delayedHeap[T]
	inFlight  map[string]*memoryDelivery[T]
	ackWait   time.Duration
	closed    bool
	closeOnce sync.Once
	sequence  atomic.Uint64
}

func New[T any](options Options) *Memory[T] {
	if options.AckWait <= 0 {
		options.AckWait = 30 * time.Second
	}
	b := &Memory[T]{notify: make(chan struct{}, 1), closedCh: make(chan struct{}), ackWait: options.AckWait, inFlight: make(map[string]*memoryDelivery[T])}
	heap.Init(&b.delayed)
	return b
}

func (b *Memory[T]) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *Memory[T]) Enqueue(ctx context.Context, value T, options queue.EnqueueOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	entry := &item[T]{id: fmt.Sprintf("memory-%d", b.sequence.Add(1)), value: value, enqueuedAt: now, notBefore: options.NotBefore}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return context.Canceled
	}
	if !entry.notBefore.IsZero() && entry.notBefore.After(now) {
		heap.Push(&b.delayed, entry)
	} else {
		b.ready = append(b.ready, entry)
	}
	b.signal()
	return nil
}

func (b *Memory[T]) promote(now time.Time) {
	for len(b.delayed) > 0 && !b.delayed[0].notBefore.After(now) {
		entry := heap.Pop(&b.delayed).(*item[T])
		b.ready = append(b.ready, entry)
	}
}

func (b *Memory[T]) nextDelay(now time.Time) time.Duration {
	if len(b.delayed) == 0 {
		return time.Hour
	}
	return max(time.Until(b.delayed[0].notBefore), time.Millisecond)
}

func (b *Memory[T]) Dequeue(ctx context.Context) (queue.Delivery[T], error) {
	for {
		b.mu.Lock()
		now := time.Now()
		b.promote(now)
		if len(b.ready) > 0 {
			entry := b.ready[0]
			b.ready = b.ready[1:]
			entry.attempt++
			delivery := &memoryDelivery[T]{backend: b, entry: entry, deliveredAt: now}
			initialized := make(chan struct{})
			delivery.timer = time.AfterFunc(b.ackWait, func() {
				<-initialized
				delivery.expire()
			})
			close(initialized)
			b.inFlight[entry.id] = delivery
			b.mu.Unlock()
			return delivery, nil
		}
		if b.closed {
			b.mu.Unlock()
			return nil, context.Canceled
		}
		wait := b.nextDelay(now)
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-b.notify:
			if !timer.Stop() {
				<-timer.C
			}
		case <-b.closedCh:
			if !timer.Stop() {
				<-timer.C
			}
			return nil, context.Canceled
		case <-timer.C:
		}
	}
}

func (b *Memory[T]) Stats(ctx context.Context) (queue.Stats, error) {
	if err := ctx.Err(); err != nil {
		return queue.Stats{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.promote(time.Now())
	return queue.Stats{Ready: int64(len(b.ready)), Deferred: int64(len(b.delayed)), InFlight: int64(len(b.inFlight))}, nil
}

func (b *Memory[T]) Close(context.Context) error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		for _, delivery := range b.inFlight {
			delivery.timer.Stop()
		}
		b.mu.Unlock()
		close(b.closedCh)
	})
	return nil
}

type memoryDelivery[T any] struct {
	backend     *Memory[T]
	entry       *item[T]
	deliveredAt time.Time
	timer       *time.Timer
	settled     atomic.Bool
}

func (d *memoryDelivery[T]) Value() T { return d.entry.value }
func (d *memoryDelivery[T]) Metadata() queue.Metadata {
	return queue.Metadata{ID: d.entry.id, EnqueuedAt: d.entry.enqueuedAt, DeliveredAt: d.deliveredAt, Attempt: d.entry.attempt}
}
func (d *memoryDelivery[T]) Settled() bool { return d.settled.Load() }

func (d *memoryDelivery[T]) terminal(fn func()) error {
	if !d.settled.CompareAndSwap(false, true) {
		return queue.ErrDeliverySettled
	}
	d.timer.Stop()
	d.backend.mu.Lock()
	delete(d.backend.inFlight, d.entry.id)
	fn()
	d.backend.mu.Unlock()
	d.backend.signal()
	return nil
}

func (d *memoryDelivery[T]) Ack(context.Context) error    { return d.terminal(func() {}) }
func (d *memoryDelivery[T]) Reject(context.Context) error { return d.terminal(func() {}) }
func (d *memoryDelivery[T]) Requeue(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.terminal(func() {
		d.entry.notBefore = time.Now().Add(delay)
		if delay > 0 {
			heap.Push(&d.backend.delayed, d.entry)
		} else {
			d.backend.ready = append(d.backend.ready, d.entry)
		}
	})
}
func (d *memoryDelivery[T]) Touch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Settled() {
		return queue.ErrDeliverySettled
	}
	d.timer.Reset(d.backend.ackWait)
	return nil
}
func (d *memoryDelivery[T]) expire() {
	_ = d.terminal(func() { d.backend.ready = append(d.backend.ready, d.entry) })
}

var _ queue.Backend[int] = (*Memory[int])(nil)
