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

type ProvisionMode uint8

const (
	BindExisting ProvisionMode = iota
	Ensure
)

type Config struct {
	Bucket    jetstream.KeyValueConfig
	Provision ProvisionMode
}

type Backend[K comparable, V any] struct {
	keyValue  jetstream.KeyValue
	keyCodec  cache.KeyEncoder[K]
	codec     cache.Codec[V]
	closed    atomic.Bool
	closeOnce sync.Once
}

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
