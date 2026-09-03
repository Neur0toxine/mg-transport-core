package queue

import "context"

type Worker[T any] func(*Queue[T])
type WorkerConstructor[T any] func(context.Context, int) Worker[T]
type Processor[T any] func(context.Context, Delivery[T])
type PanicHandler[T any] func(context.Context, Delivery[T], any)

type UnsettledKind uint8

const (
	UnsettledReturned UnsettledKind = iota + 1
	UnsettledPanicked
)

type UnsettledCause struct {
	Kind  UnsettledKind
	Panic any
}

type UnsettledProcessor[T any] func(context.Context, Delivery[T], UnsettledCause)

type workerOptions[T any] struct {
	panicHandler       PanicHandler[T]
	unsettledProcessor UnsettledProcessor[T]
	cancelCallbacks    []func()
}

type WorkerOption[T any] func(*workerOptions[T])

func WithPanicHandler[T any](handler PanicHandler[T]) WorkerOption[T] {
	return func(options *workerOptions[T]) { options.panicHandler = handler }
}

func WithUnsettledProcessor[T any](processor UnsettledProcessor[T]) WorkerOption[T] {
	return func(options *workerOptions[T]) { options.unsettledProcessor = processor }
}

func WithCancelCallbacks[T any](callbacks ...func()) WorkerOption[T] {
	return func(options *workerOptions[T]) {
		options.cancelCallbacks = append(options.cancelCallbacks, callbacks...)
	}
}

func NewWorker[T any](ctx context.Context, processor Processor[T], options ...WorkerOption[T]) Worker[T] {
	configuration := workerOptions[T]{}
	for _, option := range options {
		option(&configuration)
	}

	return func(q *Queue[T]) {
		defer func() {
			for _, callback := range configuration.cancelCallbacks {
				callback()
			}
		}()
		for {
			delivery, err := q.Dequeue(ctx)
			if err != nil {
				return
			}
			cause := UnsettledCause{Kind: UnsettledReturned}
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						cause = UnsettledCause{Kind: UnsettledPanicked, Panic: recovered}
						callPanicHandler(configuration.panicHandler, ctx, delivery, recovered)
					}
				}()
				processor(ctx, delivery)
			}()
			if !delivery.Settled() && configuration.unsettledProcessor != nil {
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							callPanicHandler(configuration.panicHandler, ctx, delivery, recovered)
						}
					}()
					configuration.unsettledProcessor(ctx, delivery, cause)
				}()
			}
		}
	}
}

func callPanicHandler[T any](handler PanicHandler[T], ctx context.Context, delivery Delivery[T], recovered any) {
	if handler == nil {
		return
	}
	defer func() { _ = recover() }()
	handler(ctx, delivery, recovered)
}

func DummyWorker[T any]() WorkerConstructor[T] {
	return func(context.Context, int) Worker[T] { return func(*Queue[T]) {} }
}
