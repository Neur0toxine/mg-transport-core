package queue

import (
	"context"
	"errors"
	"time"
)

type ExecutorInfo struct {
	ID              int
	LastEnqueueTime time.Time
	Stats           Stats
	ActiveWorkers   int
}

type Executor[T any] struct {
	queue   *Queue[T]
	workers *workerGroup[T]
}

func newExecutor[T any](id int, backend Backend[T], processor Processor[T], policy WorkerPolicy,
	panicHandler PanicHandler[T], unsettled UnsettledProcessor[T], factory WorkerFactory[T],
) *Executor[T] {
	q := New(id, backend)
	executor := &Executor[T]{queue: q}
	executor.workers = newWorkerGroup(q, processor, policy, panicHandler, unsettled, factory)
	executor.workers.Start()
	return executor
}

func (e *Executor[T]) ID() int { return e.queue.ID() }

func (e *Executor[T]) Enqueue(ctx context.Context, value T, options ...EnqueueOption) error {
	if err := e.queue.Enqueue(ctx, value, options...); err != nil {
		return err
	}
	e.workers.Notify()
	return nil
}

func (e *Executor[T]) Info(ctx context.Context) (ExecutorInfo, error) {
	stats, err := e.queue.Stats(ctx)
	return ExecutorInfo{
		ID: e.ID(), LastEnqueueTime: e.queue.LastEnqueueTime(), Stats: stats,
		ActiveWorkers: e.workers.ActiveWorkers(),
	}, err
}

func (e *Executor[T]) CloseIntake() { e.queue.CloseIntake() }

func (e *Executor[T]) Drain(ctx context.Context) error {
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()
	for {
		stats, err := e.queue.Stats(ctx)
		if err != nil {
			return err
		}
		if stats.Queued() == 0 && stats.InFlight == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Executor[T]) Close(ctx context.Context) error {
	closeErr := e.shutdown(ctx)
	return errors.Join(closeErr, e.workers.Wait(ctx))
}

func (e *Executor[T]) shutdown(ctx context.Context) error {
	e.workers.Cancel()
	return e.queue.Close(ctx)
}
