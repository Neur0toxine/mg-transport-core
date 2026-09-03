package queue

import (
	"context"
	"errors"
	"time"
)

type Processor[T any] func(context.Context, int, Delivery[T])
type PanicHandler[T any] func(context.Context, int, Delivery[T], any)

type UnsettledKind uint8

const (
	UnsettledReturned UnsettledKind = iota + 1
	UnsettledPanicked
)

type UnsettledCause struct {
	Kind  UnsettledKind
	Panic any
}

type UnsettledProcessor[T any] func(context.Context, int, Delivery[T], UnsettledCause)

type WorkerResult uint8

const (
	WorkerIdle WorkerResult = iota
	WorkerStopped
)

// Worker consumes deliveries until it becomes idle or cannot continue.
type Worker interface {
	Run(context.Context) WorkerResult
}

type WorkerConfig[T any] struct {
	Queue              *Queue[T]
	Processor          Processor[T]
	PanicHandler       PanicHandler[T]
	UnsettledProcessor UnsettledProcessor[T]
	IdleTimeout        time.Duration
}

type WorkerFactory[T any] func(WorkerConfig[T]) Worker

type defaultWorker[T any] struct {
	config WorkerConfig[T]
}

func defaultWorkerFactory[T any](config WorkerConfig[T]) Worker {
	return &defaultWorker[T]{config: config}
}

func (w *defaultWorker[T]) Run(ctx context.Context) WorkerResult {
	for {
		dequeueCtx, cancel := context.WithTimeout(ctx, w.config.IdleTimeout)
		delivery, err := w.config.Queue.Dequeue(dequeueCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return WorkerIdle
		}
		if err != nil {
			return WorkerStopped
		}
		w.process(ctx, delivery)
	}
}

func (w *defaultWorker[T]) process(ctx context.Context, delivery Delivery[T]) {
	cause := UnsettledCause{Kind: UnsettledReturned}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				cause = UnsettledCause{Kind: UnsettledPanicked, Panic: recovered}
				callPanicHandler(w.config.PanicHandler, ctx, w.config.Queue.ID(), delivery, recovered)
			}
		}()
		w.config.Processor(ctx, w.config.Queue.ID(), delivery)
	}()
	if !delivery.Settled() && w.config.UnsettledProcessor != nil {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					callPanicHandler(w.config.PanicHandler, ctx, w.config.Queue.ID(), delivery, recovered)
				}
			}()
			w.config.UnsettledProcessor(ctx, w.config.Queue.ID(), delivery, cause)
		}()
	}
}

func callPanicHandler[T any](handler PanicHandler[T], ctx context.Context, id int, delivery Delivery[T], recovered any) {
	if handler == nil {
		return
	}
	defer func() { _ = recover() }()
	handler(ctx, id, delivery, recovered)
}
