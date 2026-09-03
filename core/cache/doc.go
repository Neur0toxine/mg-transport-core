// Package cache provides a typed, storage-agnostic cache facade.
//
// # Architecture
//
// The package defines two contracts. Backend is the storage interface (get, set, has, delete, clear,
// length, close) implemented by the memory and nats subpackages. Cache is the user-facing facade that
// simply forwards to a backend, giving transports a stable, domain-typed API:
//
//	                 ┌──────────────┐
//	transport code ─►│ Cache[K, V]  │ get / set / has / delete / clear / len
//	                 └──────┬───────┘
//	                        ▼
//	                 Backend[K, V] (interface)
//	                  ┌─────┴─────┐
//	          memory  │           │  nats (JetStream KV)
//	        otter, TTL-bounded  bucket, shared across processes
//
// Persistent backends exchange values with storage as bytes, so they also need a Codec for values and
// a KeyEncoder for keys (both defined in this package). JSONCodec, BytesCodec, StringKeyEncoder, and
// JSONKeyEncoder cover the common cases; transports with domain-specific encodings can plug in their
// own implementations. Backends with fixed server-side TTLs (such as JetStream KV buckets) validate
// the configuration at construction time.
//
// # Usage
//
//	backend, err := memory.New[int, Account](memory.Options{Capacity: 1_000, TTL: time.Hour})
//	if err != nil {
//	    return err
//	}
//	accounts := cache.New(backend)
//
//	if err := accounts.Set(ctx, account.ID, account); err != nil {
//	    return err
//	}
//	account, found, err := accounts.Get(ctx, accountID)
//
// All methods accept a context so network backends can honor deadlines and cancellation. After Close,
// every method returns ErrClosed.
package cache
