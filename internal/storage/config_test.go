package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/caarlos0/env/v11"
)

func TestConfiguration(t *testing.T) {
	t.Run("reads the volatile backend when nothing is set", func(t *testing.T) {
		configured, err := env.ParseAs[Configuration]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if configured.Backend != BackendVolatile {
			t.Errorf("backend = %q, want volatile", configured.Backend)
		}
	})

	t.Run("reads the redis backend and its address", func(t *testing.T) {
		t.Setenv("STORAGE_BACKEND", "redis")
		t.Setenv("STORAGE_ADDRESS", "redis:6379")

		configured, err := env.ParseAs[Configuration]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if configured.Backend != BackendRedis || configured.Address != "redis:6379" {
			t.Errorf("configuration = %+v, want redis at redis:6379", configured)
		}
	})

	t.Run("refuses a backend no implementation has", func(t *testing.T) {
		var backend Backend

		if err := backend.UnmarshalText([]byte("etcd")); !errors.Is(err, ErrStorageNotConfigured) {
			t.Fatalf("error = %v, want %v", err, ErrStorageNotConfigured)
		}

		// The library reports the refusal as its own parse error, naming the
		// field and the backend.
		t.Setenv("STORAGE_BACKEND", "etcd")

		_, err := env.ParseAs[Configuration]()
		if err == nil || !strings.Contains(err.Error(), `unknown backend "etcd"`) {
			t.Fatalf("error = %v, want the unknown backend named", err)
		}
	})
}
