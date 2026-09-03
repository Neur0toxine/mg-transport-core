// Package queue provides a generic, storage-agnostic job queue with typed deliveries, autoscaling worker
// pools, and a multi-queue store.
//
// # Architecture
//
// The package is organized around five collaborating concepts:
//
//   - Backend stores items and hands out deliveries. Implementations live in subpackages: memory
//     (process-local), beanstalk (beanstalkd tubes), and nats (JetStream streams). Backends are
//     responsible for persistence, delivery leases, and statistics.
//   - Queue wraps a single backend and guards its lifecycle: it records the last enqueue time, closes the
//     intake for graceful shutdown, and cancels in-flight dequeues when the queue is closed.
//   - workerGroup runs workers over a Queue: it invokes Processor callbacks, restarts workers after errors
//     or panics, and scales the pool between MinWorkers and MaxWorkers according to a WorkerPolicy.
//   - Executor couples one Queue with its workerGroup and exposes enqueue, info, drain, and close
//     operations for a single numeric queue ID.
//   - Store owns executors keyed by numeric IDs (typically transport account IDs). It creates executors
//     lazily through a BackendConstructor, aggregates statistics, and reconciles the executor set against
//     a desired list of IDs.
//
// Every dequeued item is delivered as a Delivery which must be explicitly settled by the processor:
// Ack confirms successful processing, Requeue schedules a retry, and Reject discards the delivery.
// Touch renews the backend acknowledgment lease for long-running work. An unsettled delivery remains
// pending in the backend; use WithUnsettledProcessor to observe unsettled deliveries, including the
// recovered value when a processor panicked.
//
// The following diagram shows how a Store wires these parts together for one queue ID:
//
//	                    Store
//	┌────────────────────────────────────────────────────────────┐
//	│ id ──► Executor ──► Queue ──► Backend (memory/beanstalk/nats)
//	│           │            │
//	│           │            └─ cancels in-flight Dequeue on Close
//	│           └─ workerGroup ──► Worker ──► Processor(Delivery)
//	│                     └─ scales using WorkerPolicy + Stats
//	└────────────────────────────────────────────────────────────┘
//
// # Usage
//
// A Store is created from a backend constructor, a processor, and a worker policy:
//
//	jobs, err := queue.NewStore(
//	    func(ctx context.Context, accountID int) (queue.Backend[Job], error) {
//	        return memory.New[Job](memory.Options{AckWait: 30 * time.Second}), nil
//	    },
//	    func(ctx context.Context, accountID int, delivery queue.Delivery[Job]) {
//	        if err := handle(ctx, accountID, delivery.Value()); err != nil {
//	            _ = delivery.Requeue(ctx, time.Second)
//	            return
//	        }
//	        _ = delivery.Ack(ctx)
//	    },
//	    queue.WorkerPolicy{
//	        MinWorkers: 1, MaxWorkers: 10, JobsPerWorker: 10,
//	        IdleTimeout: time.Minute, ScaleInterval: time.Second,
//	    },
//	)
//	if err != nil {
//	    return err
//	}
//	if err := jobs.Enqueue(ctx, accountID, job, queue.WithID(job.ID), queue.WithDelay(time.Minute)); err != nil {
//	    return err
//	}
//	return jobs.Stop(ctx)
//
// Persistent backends (beanstalk, nats) serialize values with a Codec; JSONCodec covers most cases, and
// FuncCodec restores runtime-only dependencies after decoding. See the backend subpackages for details.
package queue
