package cache

import (
	"context"
	"errors"
)

var ErrClosed = errors.New("cache is closed")

type Backend[K comparable, V any] interface {
	Get(context.Context, K) (V, bool, error)
	Set(context.Context, K, V) error
	Has(context.Context, K) (bool, error)
	Delete(context.Context, K) error
	Clear(context.Context) error
	Len(context.Context) (int, error)
	Close(context.Context) error
}

type Cache[K comparable, V any] struct {
	backend Backend[K, V]
}

func New[K comparable, V any](backend Backend[K, V]) *Cache[K, V] {
	return &Cache[K, V]{backend: backend}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return c.backend.Get(ctx, key)
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V) error {
	return c.backend.Set(ctx, key, value)
}

func (c *Cache[K, V]) Has(ctx context.Context, key K) (bool, error) {
	return c.backend.Has(ctx, key)
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	return c.backend.Delete(ctx, key)
}

func (c *Cache[K, V]) Clear(ctx context.Context) error {
	return c.backend.Clear(ctx)
}

func (c *Cache[K, V]) Len(ctx context.Context) (int, error) {
	return c.backend.Len(ctx)
}

func (c *Cache[K, V]) Close(ctx context.Context) error {
	return c.backend.Close(ctx)
}
