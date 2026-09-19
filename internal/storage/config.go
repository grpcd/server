package storage

import "fmt"

// Backend names a storage implementation.
type Backend string

const (
	// BackendVolatile is the in-memory store: no dependency, nothing shared
	// between instances, nothing kept across a restart.
	BackendVolatile Backend = ""

	// BackendRedis is the Redis store, shared by every instance in a region.
	BackendRedis Backend = "redis"
)

// UnmarshalText reads a backend name, refusing one no implementation has, so
// configuration naming an unknown backend fails where it is parsed.
func (b *Backend) UnmarshalText(text []byte) error {
	switch named := Backend(text); named {
	case BackendVolatile, BackendRedis:
		*b = named

		return nil
	default:
		return fmt.Errorf("%w: unknown backend %q", ErrStorageNotConfigured, named)
	}
}

// Configuration is the store's, whichever backend is behind it: which one,
// and where it is reached. A backend that reaches nothing ignores Address; one
// that does refuses an empty one.
type Configuration struct {
	Backend Backend `env:"STORAGE_BACKEND"`
	Address string  `env:"STORAGE_ADDRESS"`
}
