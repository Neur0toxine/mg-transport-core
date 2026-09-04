package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/retailcrm/mg-transport-core/v2/core/cache"
	corenats "github.com/retailcrm/mg-transport-core/v2/core/nats"
)

// ProvisionMode selects how the backend obtains its JetStream KV bucket.
type ProvisionMode uint8

const (
	// BindExisting binds to an already provisioned bucket and validates its TTL against the configured
	// one instead of creating anything.
	BindExisting ProvisionMode = iota
	// Ensure creates or updates the bucket, including its TTL.
	Ensure
)

// Config configures a JetStream KV cache backend.
type Config struct {
	// Bucket is the JetStream key-value configuration; Bucket.Bucket (the name) and the TTL are used
	// by both provision modes.
	Bucket jetstream.KeyValueConfig
	// Provision selects between creating or updating the bucket (Ensure) and binding to an existing
	// one with a matching TTL (BindExisting).
	Provision ProvisionMode
}

// Backend is a cache.Backend over one JetStream KV bucket shared through a core/nats.Client. It is
// safe for concurrent use.
type Backend[K comparable, V any] struct {
	keyValue  jetstream.KeyValue
	keyCodec  cache.KeyEncoder[K]
	codec     cache.Codec[V]
	closed    atomic.Bool
	closeOnce sync.Once
}

// New builds a JetStream KV cache backend from a connected core NATS client, a key encoder, a value
// codec, and a configuration. Depending on Config.Provision it creates or updates the bucket (Ensure)
// or binds to an existing bucket whose TTL must match the configured one (BindExisting).
func New[K comparable, V any](
	ctx context.Context,
	client *corenats.Client,
	keyCodec cache.KeyEncoder[K],
	codec cache.Codec[V],
	config Config,
) (*Backend[K, V], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil || client.JetStream == nil {
		return nil, errors.New("NATS JetStream client is required")
	}
	if keyCodec == nil {
		return nil, errors.New("NATS cache key encoder is required")
	}
	if codec == nil {
		return nil, errors.New("NATS cache codec is required")
	}
	if config.Bucket.Bucket == "" {
		return nil, errors.New("NATS cache bucket name is required")
	}

	var (
		keyValue jetstream.KeyValue
		err      error
	)
	if config.Provision == Ensure {
		keyValue, err = client.JetStream.CreateOrUpdateKeyValue(ctx, config.Bucket)
		if err != nil {
			return nil, fmt.Errorf("ensure NATS cache bucket %q: %w", config.Bucket.Bucket, err)
		}
	} else {
		keyValue, err = client.JetStream.KeyValue(ctx, config.Bucket.Bucket)
		if err != nil {
			return nil, fmt.Errorf("bind NATS cache bucket %q: %w", config.Bucket.Bucket, err)
		}
		if err := validateBucket(ctx, keyValue, config.Bucket); err != nil {
			return nil, err
		}
	}

	return &Backend[K, V]{keyValue: keyValue, keyCodec: keyCodec, codec: codec}, nil
}

func validateBucket(ctx context.Context, keyValue jetstream.KeyValue, expected jetstream.KeyValueConfig) error {
	status, err := keyValue.Status(ctx)
	if err != nil {
		return fmt.Errorf("inspect NATS cache bucket %q: %w", expected.Bucket, err)
	}
	actual := status.Config()
	expected = normalizeBucketConfig(expected)
	actual = normalizeBucketConfig(actual)
	if actual.TTL != expected.TTL || actual.History != expected.History ||
		actual.Replicas != expected.Replicas || actual.Storage != expected.Storage {
		return fmt.Errorf(
			"NATS cache bucket %q configuration mismatch: got TTL=%s history=%d replicas=%d storage=%s, "+
				"expected TTL=%s history=%d replicas=%d storage=%s",
			expected.Bucket,
			actual.TTL, actual.History, actual.Replicas, actual.Storage,
			expected.TTL, expected.History, expected.Replicas, expected.Storage,
		)
	}
	return nil
}

func normalizeBucketConfig(config jetstream.KeyValueConfig) jetstream.KeyValueConfig {
	if config.History == 0 {
		config.History = 1
	}
	if config.Replicas == 0 {
		config.Replicas = 1
	}
	if config.Storage == 0 {
		config.Storage = jetstream.FileStorage
	}
	return config
}

func (b *Backend[K, V]) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed.Load() {
		return cache.ErrClosed
	}
	return nil
}

func (b *Backend[K, V]) encodeKey(key K) (string, error) {
	encoded, err := b.keyCodec.EncodeKey(key)
	if err != nil {
		return "", fmt.Errorf("encode NATS cache key: %w", err)
	}
	return encoded, nil
}

// Get fetches and decodes the value for the key. A missing key yields a zero value, false, and a nil
// error.
func (b *Backend[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	entry, found, err := b.GetEntry(ctx, key)
	return entry.Value, found, err
}

// GetEntry fetches and decodes a value together with its JetStream revision metadata.
func (b *Backend[K, V]) GetEntry(ctx context.Context, key K) (cache.Entry[V], bool, error) {
	if err := b.check(ctx); err != nil {
		return cache.Entry[V]{}, false, err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return cache.Entry[V]{}, false, err
	}
	entry, err := b.keyValue.Get(ctx, encodedKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return cache.Entry[V]{}, false, nil
	}
	if err != nil {
		return cache.Entry[V]{}, false, fmt.Errorf("get NATS cache key %q: %w", encodedKey, err)
	}
	value, err := b.codec.Decode(entry.Value())
	if err != nil {
		return cache.Entry[V]{}, false, fmt.Errorf("decode NATS cache value for key %q: %w", encodedKey, err)
	}
	return cache.Entry[V]{Value: value, Revision: entry.Revision(), CreatedAt: entry.Created()}, true, nil
}

// Set encodes the value and stores it under the key, replacing any previous entry.
func (b *Backend[K, V]) Set(ctx context.Context, key K, value V) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return err
	}
	encodedValue, err := b.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode NATS cache value for key %q: %w", encodedKey, err)
	}
	if _, err := b.keyValue.Put(ctx, encodedKey, encodedValue); err != nil {
		return fmt.Errorf("set NATS cache key %q: %w", encodedKey, err)
	}
	return nil
}

// Create stores a value only when the key does not currently exist.
func (b *Backend[K, V]) Create(ctx context.Context, key K, value V) (uint64, error) {
	encodedKey, encodedValue, err := b.encode(ctx, key, value)
	if err != nil {
		return 0, err
	}
	revision, err := b.keyValue.Create(ctx, encodedKey, encodedValue)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return 0, fmt.Errorf("create NATS cache key %q: %w", encodedKey, cache.ErrConflict)
	}
	if err != nil {
		return 0, fmt.Errorf("create NATS cache key %q: %w", encodedKey, err)
	}
	return revision, nil
}

// Update replaces a value only when revision is still current.
func (b *Backend[K, V]) Update(ctx context.Context, key K, value V, revision uint64) (uint64, error) {
	encodedKey, encodedValue, err := b.encode(ctx, key, value)
	if err != nil {
		return 0, err
	}
	nextRevision, err := b.keyValue.Update(ctx, encodedKey, encodedValue, revision)
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, fmt.Errorf("update NATS cache key %q: %w", encodedKey, cache.ErrConflict)
	}
	if err != nil {
		return 0, fmt.Errorf("update NATS cache key %q: %w", encodedKey, err)
	}
	return nextRevision, nil
}

func (b *Backend[K, V]) encode(ctx context.Context, key K, value V) (string, []byte, error) {
	if err := b.check(ctx); err != nil {
		return "", nil, err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return "", nil, err
	}
	encodedValue, err := b.codec.Encode(value)
	if err != nil {
		return "", nil, fmt.Errorf("encode NATS cache value for key %q: %w", encodedKey, err)
	}
	return encodedKey, encodedValue, nil
}

// Has reports whether the key is present without decoding the value.
func (b *Backend[K, V]) Has(ctx context.Context, key K) (bool, error) {
	if err := b.check(ctx); err != nil {
		return false, err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return false, err
	}
	_, err = b.keyValue.Get(ctx, encodedKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check NATS cache key %q: %w", encodedKey, err)
	}
	return true, nil
}

// Delete purges the key so no per-key history accumulates. Deleting a missing key is not an error.
func (b *Backend[K, V]) Delete(ctx context.Context, key K) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return err
	}
	if _, err := b.keyValue.Get(ctx, encodedKey); errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("check NATS cache key %q before deletion: %w", encodedKey, err)
	}
	if err := b.keyValue.Purge(ctx, encodedKey); err != nil {
		return fmt.Errorf("delete NATS cache key %q: %w", encodedKey, err)
	}
	return nil
}

// DeleteRevision places a delete marker only when revision is still current.
func (b *Backend[K, V]) DeleteRevision(ctx context.Context, key K, revision uint64) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return err
	}
	err = b.keyValue.Delete(ctx, encodedKey, jetstream.LastRevision(revision))
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("delete NATS cache key %q: %w", encodedKey, cache.ErrConflict)
	}
	if err != nil {
		return fmt.Errorf("delete NATS cache key %q: %w", encodedKey, err)
	}
	return nil
}

// Keys returns all current keys decoded to their typed form.
func (b *Backend[K, V]) Keys(ctx context.Context) ([]K, error) {
	if err := b.check(ctx); err != nil {
		return nil, err
	}
	decoder, ok := b.keyCodec.(cache.KeyDecoder[K])
	if !ok {
		return nil, cache.ErrKeyDecodingUnsupported
	}
	encodedKeys, err := b.keyValue.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return []K{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list NATS cache keys: %w", err)
	}
	keys := make([]K, 0, len(encodedKeys))
	for _, encodedKey := range encodedKeys {
		key, decodeErr := decoder.DecodeKey(encodedKey)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode NATS cache key %q: %w", encodedKey, decodeErr)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Clear purges every key in the bucket. Individual purge failures are joined into the result.
func (b *Backend[K, V]) Clear(ctx context.Context) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	keys, err := b.keyValue.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("list NATS cache keys: %w", err)
	}
	var errs []error
	for _, key := range keys {
		if err := b.keyValue.Purge(ctx, key); err != nil {
			errs = append(errs, fmt.Errorf("delete NATS cache key %q: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// Len counts the keys currently present in the bucket.
func (b *Backend[K, V]) Len(ctx context.Context) (int, error) {
	if err := b.check(ctx); err != nil {
		return 0, err
	}
	keys, err := b.keyValue.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list NATS cache keys: %w", err)
	}
	return len(keys), nil
}

// Close marks the backend closed; subsequent operations return cache.ErrClosed. It does not close the
// shared NATS client and does not delete the bucket. Close is idempotent.
func (b *Backend[K, V]) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.closeOnce.Do(func() {
		b.closed.Store(true)
	})
	return nil
}

var _ cache.Backend[int, int] = (*Backend[int, int])(nil)
var _ cache.VersionedBackend[int, int] = (*Backend[int, int])(nil)
