# Cache

The `core/cache` package (with the `core/cache/memory` and `core/cache/nats` subpackages) provides a
typed cache facade over interchangeable backends. Values and keys stay typed in transport code;
storage details (encoding, TTL semantics, sharing) are a backend concern.

## The facade

```go
type Cache[K comparable, V any] // get / set / has / delete / clear / len / close
type VersionedCache[K comparable, V any] // Cache plus revisions / CAS / typed keys
```

```go
backend, err := memory.New[int, Account](memory.Options{
    Capacity: 1_000,
    TTL:      time.Hour,
})
if err != nil {
    return err
}
accounts := cache.New(backend)

_ = accounts.Set(ctx, account.ID, account)
account, found, err := accounts.Get(ctx, accountID)
if !found && err == nil {
    account, err = loadAccount(ctx, accountID) // cache miss: recompute and store
    if err == nil {
        _ = accounts.Set(ctx, account.ID, account)
    }
}
```

All operations take a `context.Context`, and every operation returns `cache.ErrClosed` after
`Close`. `Get` distinguishes a miss (`found == false`, `err == nil`) from a failure (`err != nil`).

## Backends

### memory — process-local

| Option | Meaning |
|---|---|
| `Capacity` | Hard entry limit; must be positive. Excess writes evict via the otter admission policy. |
| `TTL` | Entries expire this long *after being written*. Zero disables expiry. |

The backend is built on the [otter] cache (S3-FIFO), is lock-free, and never blocks on I/O. Entries do
not survive restarts and are invisible to other replicas — use it for re-computable, per-instance data
(connection objects, resolved tokens, idempotent API responses).

```mermaid
flowchart LR
    subgraph process["Transport replica"]
        C1["Cache[int, Account]"] --> M1["memory.Backend<br/>otter, capacity, write TTL"]
    end
```

### nats — JetStream KV bucket, shared

The NATS backend stores entries in a JetStream key-value bucket. Every process using the same bucket
and client sees the same data, which makes it a building block for cross-replica caches.

```go
client, err := corenats.Connect(ctx, corenats.Config{URLs: cfg.NATSURLs}, log)
if err != nil {
    return err
}

backend, err := nats.New[int, Account](
    ctx, client,
    cache.JSONKeyEncoder[int]{},   // int keys -> base64(JSON) bucket keys
    cache.JSONCodec[Account]{},    // Account -> JSON bytes
    nats.Config{
        Bucket:    jetstream.KeyValueConfig{Bucket: "accounts", TTL: time.Hour},
        Provision: nats.Ensure,
    },
)
```

Properties to be aware of:

- **TTL is bucket-wide.** The server expires every entry after the bucket's TTL; per-entry TTL is not
  possible. `Provision: BindExisting` validates that the existing bucket's TTL matches the configured
  one and refuses to bind otherwise; `Ensure` creates or updates the bucket (and its TTL).
- **Reads hit the server.** The backend does not watch for updates; a value written by another
  process becomes visible on the next operation.
- **Delete purges** the key so per-key history does not accumulate in the underlying stream.
- **Closing is local.** `Close` only marks the backend closed; the shared NATS client and the bucket
  itself are untouched.

For shared mutable state, wrap the same backend with `cache.NewVersioned`. `Create` reserves an absent
key, `GetEntry` returns its revision, and `Update` / `DeleteRevision` perform optimistic concurrency:

```go
state := cache.NewVersioned[string, DeliveryState](backend)
revision, err := state.Create(ctx, deliveryID, initial)
if errors.Is(err, cache.ErrConflict) {
    current, found, err := state.GetEntry(ctx, deliveryID)
    // Resolve the conflict or retry an Update with current.Revision.
}
_, err = state.Update(ctx, deliveryID, completed, revision)
keys, err := state.Keys(ctx)
```

`BindExisting` validates TTL, history, replicas, and storage. `Keys` requires a key converter that
also implements `cache.KeyDecoder`; both built-in key encoders do.

```mermaid
flowchart LR
    subgraph replica1["Replica A"]
        CA["Cache[int, Account]"] --> NA["cache/nats.Backend"]
    end
    subgraph replica2["Replica B"]
        CB["Cache[int, Account]"] --> NB["cache/nats.Backend"]
    end
    NA --> KV["JetStream KV bucket<br/>TTL: 1h"]
    NB --> KV
```

## Keys and values on persistent backends

Bucket keys are strings and stored values are bytes, so the NATS backend takes two converters:

| Converter | Behavior |
|---|---|
| `cache.StringKeyEncoder{}` | Passes `string` keys through unchanged. |
| `cache.JSONKeyEncoder[K]{}` | Serializes any comparable key to JSON and base64-encodes it, keeping it safe for the key space. |
| `cache.JSONCodec[V]{}` | Values as encoding/json/v2. |
| `cache.BytesCodec{}` | Pass `[]byte` values through (already-encoded payloads). |

Custom encodings plug in by implementing `cache.KeyEncoder[K]` or `cache.Codec[V]`. Implement
`cache.KeyCodec[K]` when typed key listing is required.

## Choosing a backend

| Need | Backend |
|---|---|
| Cheap per-instance cache, misses re-computable | `memory` |
| Shared across replicas / survive restarts | `nats` |
| Per-entry TTL | `memory` |
| Server-enforced uniform TTL | `nats` |

[otter]: https://github.com/maypok86/otter
