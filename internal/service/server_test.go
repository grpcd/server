package service

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/pbrpc/otel-testing/mocks/meter"

	"github.com/grpcd/server/internal/storage/mock"
)

func TestNewGRPCDServer(t *testing.T) {
	t.Run("builds a server with its metrics", func(t *testing.T) {
		server := NewGRPCDServer(
			slog.New(slog.DiscardHandler), mock.NewStore(), meter.New(), testAnchor,
		)

		if server.log == nil {
			t.Error("expected logger to be set")
		}
		if server.registrationCount == nil {
			t.Error("expected registrationCount to be set")
		}
		if server.removalCount == nil {
			t.Error("expected removalCount to be set")
		}
		if server.methodsDiscovered == nil {
			t.Error("expected methodsDiscovered to be set")
		}
		if server.revertedRemovals == nil {
			t.Error("expected revertedRemovals to be set")
		}
		if server.rebalanceCount == nil {
			t.Error("expected rebalanceCount to be set")
		}
		if server.roll == nil {
			t.Error("expected roll to be set")
		}
		if server.anchor != testAnchor {
			t.Errorf("expected anchor %q, got %q", testAnchor, server.anchor)
		}
	})

	t.Run("draws one in n", func(t *testing.T) {
		if oneIn(0) {
			t.Error("expected nobody to move when nothing serves the method")
		}

		if !oneIn(1) {
			t.Error("expected the only holder to move when the new address is the only one")
		}

		// Uniform, so unasserted beyond answering; the shape of the draw is
		// what the two cases above pin.
		_ = oneIn(3)
	})

	t.Run("substitutes a logger when given none", func(t *testing.T) {
		server := NewGRPCDServer(nil, mock.NewStore(), meter.New(), testAnchor)

		if server.log == nil {
			t.Fatal("expected logger to be set even when nil passed")
		}
	})

	t.Run("substitutes a store when given none", func(t *testing.T) {
		server := NewGRPCDServer(slog.New(slog.DiscardHandler), nil, meter.New(), testAnchor)

		if server.store == nil {
			t.Fatal("expected store to be set even when nil passed")
		}
	})

	t.Run("serves without metrics when they cannot be created", func(t *testing.T) {
		failing := meter.New()
		failing.SetInt64CounterError(errors.New("metric creation failed"))

		server := NewGRPCDServer(
			slog.New(slog.DiscardHandler), mock.NewStore(), failing, testAnchor,
		)

		if server.log == nil {
			t.Fatal("expected logger to be set")
		}
	})
}
