package cache

import (
	"context"
	"errors"
)

// ErrClosed is returned by every cache operation after Close.
var ErrClosed = errors.New("cache is closed")

// Backend is the storage contract behind Cache. Implementations live in the memory and nats
// subpackages. Get reports a miss with a false second result instead of an error; Len counts the
// entries currently stored.
type Backend[K comparable, V any] interface {
	Get(context.Context, K) (V, bool, error)
	Set(context.Context, K, V) error
	Has(context.Context, K) (bool, error)
	Delete(context.Context, K) error
	Clear(context.Context) error
	Len(context.Context) (int, error)
	Close(context.Context) error
}

// Cache is a typed facade over a Backend. Construct it with New and share it freely: the cache adds no
// state of its own and is safe for concurrent use as long as the backend is.
type Cache[K comparable, V any] struct {
	backend Backend[K, V]
}

// New wraps a backend into the user-facing cache facade.
func New[K comparable, V any](backend Backend[K, V]) *Cache[K, V] {
	return &Cache[K, V]{backend: backend}
}

// Get returns the cached value for the key. A missing key yields a zero value, false, and a nil error.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return c.backend.Get(ctx, key)
}

// Set stores the value under the key, replacing any previous entry.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V) error {
	return c.backend.Set(ctx, key, value)
}

// Has reports whether the key is present without decoding the value.
func (c *Cache[K, V]) Has(ctx context.Context, key K) (bool, error) {
	return c.backend.Has(ctx, key)
}

// Delete removes the key. Deleting a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	return c.backend.Delete(ctx, key)
}

// Clear removes every entry from the backend.
func (c *Cache[K, V]) Clear(ctx context.Context) error {
	return c.backend.Clear(ctx)
}

// Len returns the number of entries currently stored.
func (c *Cache[K, V]) Len(ctx context.Context) (int, error) {
	return c.backend.Len(ctx)
}

// Close releases backend resources. Subsequent operations return ErrClosed.
func (c *Cache[K, V]) Close(ctx context.Context) error {
	return c.backend.Close(ctx)
}
