//revive:disable:package-comments
package redis

import (
	"context"
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

// NewRedisStore creates a new Redis store, reconnecting its subscription on
// the schedule newBackOff makes. Performs pure construction with no network
// I/O.
func NewRedisStore(addr string, newBackOff BackOffFactory) *Store {
	client := redis.NewClient(&redis.Options{Addr: addr})

	return &Store{
		client:     client,
		additions:  storage.NewAdditions(),
		conditions: storage.NewConditions(),
		newBackOff: newBackOff,
	}
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
