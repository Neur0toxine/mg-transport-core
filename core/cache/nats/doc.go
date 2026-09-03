// Package nats provides a distributed cache.Backend implementation backed by NATS JetStream key-value
// buckets.
//
// # Architecture
//
// Each backend owns exactly one JetStream KV bucket and shares a core/nats.Client connection with the
// rest of the transport. Keys are converted to bucket keys with a cache.KeyEncoder and values are
// serialized with a cache.Codec, so the stored form is fully controlled by the caller. Delete uses a
// purge so per-key history does not accumulate in the underlying stream.
//
// Entry expiry is a bucket-wide property: the TTL configured in jetstream.KeyValueConfig is applied by
// the server to every entry, and clients cannot override it per key. Because of that, BindExisting
// validates that the existing bucket's TTL matches the configured one and refuses to bind otherwise.
// Use Provision mode Ensure to create or update the bucket (and its TTL) from the application.
//
// The bucket content is shared by every process using it, which makes the backend a building block for
// cross-replica caches. Backends do not watch for updates: reads hit the server, so changes made by
// another process are visible on the next operation.
//
// # Usage
//
//	client, err := corenats.Connect(ctx, corenats.Config{URLs: []string{"nats://localhost:4222"}}, log)
//	if err != nil {
//	    return err
//	}
//	backend, err := nats.New[int, Account](
//	    ctx, client,
//	    cache.JSONKeyEncoder[int]{},
//	    cache.JSONCodec[Account]{},
//	    nats.Config{
//	        Bucket:    jetstream.KeyValueConfig{Bucket: "accounts", TTL: time.Hour},
//	        Provision: nats.Ensure,
//	    },
//	)
//
// Closing the backend only marks it closed: neither the shared client nor the bucket is touched.
package nats
