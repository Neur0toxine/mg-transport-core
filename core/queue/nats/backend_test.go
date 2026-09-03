package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/retailcrm/mg-transport-core/v2/core/logger"
	corenats "github.com/retailcrm/mg-transport-core/v2/core/nats"
	"github.com/retailcrm/mg-transport-core/v2/core/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendLifecycleAndScheduling(t *testing.T) {
	srv, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1})
	require.NoError(t, err)
	srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	client, err := corenats.Connect(t.Context(), corenats.Config{URLs: []string{srv.ClientURL()}}, logger.NewNil())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	backend, err := New(t.Context(), client, queue.JSONCodec[string]{}, Config{
		Subject: "jobs.ready", ScheduleSubject: "jobs.schedule", Provision: Ensure,
		Stream:       jetstream.StreamConfig{Name: "JOBS", Storage: jetstream.MemoryStorage, Retention: jetstream.WorkQueuePolicy},
		Consumer:     jetstream.ConsumerConfig{Name: "workers", AckWait: 100 * time.Millisecond},
		FetchMaxWait: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	q := queue.New(1, backend)
	require.NoError(t, q.Enqueue(t.Context(), "now"))
	require.NoError(t, q.Enqueue(t.Context(), "later", queue.WithDelay(100*time.Millisecond)))
	require.Eventually(t, func() bool {
		stats, statsErr := q.Stats(t.Context())
		return statsErr == nil && stats.Deferred == 1
	}, time.Second, 10*time.Millisecond)

	delivery, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "now", delivery.Value())
	require.NoError(t, delivery.Requeue(t.Context(), 20*time.Millisecond))
	delivery, err = q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "now", delivery.Value())
	assert.GreaterOrEqual(t, delivery.Metadata().Attempt, uint64(2))
	require.NoError(t, delivery.Ack(t.Context()))

	delivery, err = q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "later", delivery.Value())
	require.NoError(t, delivery.Reject(t.Context()))
	require.NoError(t, backend.Close(t.Context()))

	_, err = New(t.Context(), client, queue.JSONCodec[string]{}, Config{
		Subject: "jobs.ready", ScheduleSubject: "jobs.schedule", Provision: BindExisting,
		Stream: jetstream.StreamConfig{Name: "JOBS"}, Consumer: jetstream.ConsumerConfig{Name: "workers"},
	})
	require.NoError(t, err)
}
