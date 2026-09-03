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
	if status.TTL() != expected.TTL {
		return fmt.Errorf(
			"NATS cache bucket %q TTL is %s, expected %s",
			expected.Bucket,
			status.TTL(),
			expected.TTL,
		)
	}
	return nil
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
	var zero V
	if err := b.check(ctx); err != nil {
		return zero, false, err
	}
	encodedKey, err := b.encodeKey(key)
	if err != nil {
		return zero, false, err
	}
	entry, err := b.keyValue.Get(ctx, encodedKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("get NATS cache key %q: %w", encodedKey, err)
	}
	value, err := b.codec.Decode(entry.Value())
	if err != nil {
		return zero, false, fmt.Errorf("decode NATS cache value for key %q: %w", encodedKey, err)
	}
	return value, true, nil
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
