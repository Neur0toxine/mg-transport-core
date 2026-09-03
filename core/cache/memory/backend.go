package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/retailcrm/mg-transport-core/v2/core/cache"
)

type Options struct {
	Capacity int
	TTL      time.Duration
}

type Backend[K comparable, V any] struct {
	cache     *otter.Cache[K, V]
	closed    atomic.Bool
	closeOnce sync.Once
}

func New[K comparable, V any](options Options) (*Backend[K, V], error) {
	if options.Capacity <= 0 {
		return nil, errors.New("memory cache capacity must be positive")
	}
	if options.TTL < 0 {
		return nil, errors.New("memory cache TTL must not be negative")
	}

	otterOptions := &otter.Options[K, V]{MaximumSize: options.Capacity}
	if options.TTL > 0 {
		otterOptions.ExpiryCalculator = otter.ExpiryWriting[K, V](options.TTL)
	}
	storage, err := otter.New(otterOptions)
	if err != nil {
		return nil, fmt.Errorf("create memory cache: %w", err)
	}
	return &Backend[K, V]{cache: storage}, nil
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

func (b *Backend[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	if err := b.check(ctx); err != nil {
		var zero V
		return zero, false, err
	}
	value, found := b.cache.GetIfPresent(key)
	return value, found, nil
}

func (b *Backend[K, V]) Set(ctx context.Context, key K, value V) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	b.cache.Set(key, value)
	return nil
}

func (b *Backend[K, V]) Has(ctx context.Context, key K) (bool, error) {
	if err := b.check(ctx); err != nil {
		return false, err
	}
	_, found := b.cache.GetIfPresent(key)
	return found, nil
}

func (b *Backend[K, V]) Delete(ctx context.Context, key K) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	b.cache.Invalidate(key)
	return nil
}

func (b *Backend[K, V]) Clear(ctx context.Context) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	b.cache.InvalidateAll()
	return nil
}

func (b *Backend[K, V]) Len(ctx context.Context) (int, error) {
	if err := b.check(ctx); err != nil {
		return 0, err
	}
	b.cache.CleanUp()
	return b.cache.EstimatedSize(), nil
}

func (b *Backend[K, V]) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		b.cache.InvalidateAll()
		b.cache.StopAllGoroutines()
	})
	return nil
}

var _ cache.Backend[int, int] = (*Backend[int, int])(nil)
