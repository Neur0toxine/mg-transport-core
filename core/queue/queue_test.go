package queue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/retailcrm/mg-transport-core/v2/core/queue"
	"github.com/retailcrm/mg-transport-core/v2/core/queue/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueDeliveryLifecycle(t *testing.T) {
	driver := memory.New[int](memory.Options{AckWait: 50 * time.Millisecond})
	q := queue.New(7, driver)
	require.NoError(t, q.Enqueue(t.Context(), 1, queue.WithID("caller-id")))
	require.NoError(t, q.Enqueue(t.Context(), 2, queue.WithDelay(30*time.Millisecond)))

	stats, err := q.Stats(t.Context())
	require.NoError(t, err)
	assert.Equal(t, queue.Stats{Ready: 1, Deferred: 1}, stats)

	delivery, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, delivery.Value())
	assert.Equal(t, "caller-id", delivery.Metadata().ID)
	assert.Equal(t, uint64(1), delivery.Metadata().Attempt)
	require.NoError(t, delivery.Requeue(t.Context(), 0))
	require.ErrorIs(t, delivery.Ack(t.Context()), queue.ErrDeliverySettled)

	delivery, err = q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), delivery.Metadata().Attempt)
	require.NoError(t, delivery.Touch(t.Context()))
	require.NoError(t, delivery.Ack(t.Context()))

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	delivery, err = q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, delivery.Value())
	require.NoError(t, delivery.Reject(t.Context()))
}

func TestMemoryRedeliversExpiredDelivery(t *testing.T) {
	q := queue.New(1, memory.New[string](memory.Options{AckWait: 20 * time.Millisecond}))
	require.NoError(t, q.Enqueue(t.Context(), "job"))
	first, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.False(t, first.Settled())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	second, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "job", second.Value())
	assert.Equal(t, uint64(2), second.Metadata().Attempt)
	require.NoError(t, second.Ack(t.Context()))
}

func TestWorkerUsesUnsettledProcessor(t *testing.T) {
	var called atomic.Int64
	store, err := queue.NewStore(
		func(context.Context, int) (queue.Driver[int], error) {
			return memory.New[int](memory.Options{}), nil
		},
		func(context.Context, int, queue.Delivery[int]) {},
		queue.WorkerPolicy{MinWorkers: 1, MaxWorkers: 1, JobsPerWorker: 1, IdleTimeout: time.Second, ScaleInterval: time.Second},
		queue.WithUnsettledProcessor(func(ctx context.Context, _ int, delivery queue.Delivery[int], cause queue.UnsettledCause) {
			called.Add(1)
			assert.Equal(t, queue.UnsettledReturned, cause.Kind)
			require.NoError(t, delivery.Ack(ctx))
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Stop(context.Background())) })
	require.NoError(t, store.Enqueue(t.Context(), 1, 42))
	require.Eventually(t, func() bool { return called.Load() == 1 }, time.Second, time.Millisecond)
}

func TestStoreConstructsAndDrainsQueues(t *testing.T) {
	store, err := queue.NewStore(
		func(context.Context, int) (queue.Driver[int], error) {
			return memory.New[int](memory.Options{}), nil
		},
		func(ctx context.Context, _ int, delivery queue.Delivery[int]) {
			require.NoError(t, delivery.Ack(ctx))
		},
		queue.WorkerPolicy{MinWorkers: 1, MaxWorkers: 1, JobsPerWorker: 1, IdleTimeout: time.Second, ScaleInterval: time.Second},
	)
	require.NoError(t, err)
	q, err := store.Get(t.Context(), 5)
	require.NoError(t, err)
	require.NoError(t, q.Enqueue(t.Context(), 1))
	store.CloseIntake()
	require.ErrorIs(t, q.Enqueue(t.Context(), 2), queue.ErrIntakeClosed)
	require.NoError(t, store.Drain(t.Context()))
	require.NoError(t, store.Stop(t.Context()))
}
