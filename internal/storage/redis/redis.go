//revive:disable:package-comments
package redis

import (
	"context"
	"fmt"
	"iter"

	"github.com/cenkalti/backoff/v7"
	"github.com/redis/go-redis/v9"

	"github.com/grpcd/server/internal/storage"
)

// BackOffFactory makes the schedule the subscription's reconnection attempts
// follow while the backend cannot be reached.
type BackOffFactory func() backoff.BackOff

// Store implements Store using Redis as the backend
type Store struct {
	client     *redis.Client
	additions  *storage.Additions
	conditions *storage.Conditions
	newBackOff BackOffFactory
}

// NewRedisStore creates a Redis store at the configured address, reconnecting
// its subscription on the schedule newBackOff makes. Redis is reached at an
// address, so a configuration without one is refused. Performs pure
// construction with no network I/O.
func NewRedisStore(cfg storage.Configuration, newBackOff BackOffFactory) (*Store, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("%w: STORAGE_ADDRESS is required for redis", storage.ErrStorageNotConfigured)
	}

	client := redis.NewClient(&redis.Options{Addr: cfg.Address})

	return &Store{
		client:     client,
		additions:  storage.NewAdditions(),
		conditions: storage.NewConditions(),
		newBackOff: newBackOff,
	}, nil
}

// Latest answers with the most recent addition announced to this instance.
func (r *Store) Latest() *storage.Addition {
	return r.additions.Latest()
}

// Condition answers with the store's reachability.
func (r *Store) Condition() *storage.Condition {
	return r.conditions.Current()
}

// Changes yields the store's reachability as it changes.
func (r *Store) Changes(ctx context.Context) iter.Seq[*storage.Condition] {
	return r.conditions.Changes(ctx)
}

// Close closes the Redis connection
func (r *Store) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// Name of the store
func (*Store) Name() string {
	return "redis"
}

// Address of the store
func (r *Store) Address() string {
	return r.client.Options().Addr
}
