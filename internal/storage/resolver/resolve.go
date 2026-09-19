//revive:disable:package-comments
package resolver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/caarlos0/env/v11"
	"github.com/cenkalti/backoff/v7"

	"github.com/grpcd/server/internal/storage"
	"github.com/grpcd/server/internal/storage/mock"
	"github.com/grpcd/server/internal/storage/redis"
)

// Resolve a storage Store from the store's configuration in the environment.
//
// An unreachable backend is returned as an error rather than ended here, so the
// caller's shutdown still runs and the log explaining the failure is exported.
func Resolve(ctx context.Context, log *slog.Logger) (storage.Store, error) {
	configured, err := env.ParseAs[storage.Configuration]()
	if err != nil {
		return nil, err
	}

	switch configured.Backend {
	case storage.BackendRedis:
		store, err := redis.NewRedisStore(configured, func() backoff.BackOff { return backoff.NewExponentialBackOff() })
		if err != nil {
			return nil, err
		}

		if err := store.Ping(ctx); err != nil {
			return nil, fmt.Errorf("%w: %w", storage.ErrStorageNotReachable, err)
		}

		if err := store.Listen(ctx); err != nil {
			return nil, fmt.Errorf("%w: %w", storage.ErrStorageNotReachable, err)
		}

		log.InfoContext(ctx, "Using redis storage", "address", configured.Address)

		return store, nil
	default:
		// The parse refused every name but the known ones, so what is left is
		// the volatile store.
		log.InfoContext(ctx, "Using volatile storage")

		return mock.NewStore(), nil
	}
}
