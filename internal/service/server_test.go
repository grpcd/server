package service

import (
	"log/slog"
	"testing"

	"github.com/grpcd/server/internal/storage/mock"
)

func TestNewGRPCDServer(t *testing.T) {
	t.Run("builds a server", func(t *testing.T) {
		server := NewGRPCDServer(slog.New(slog.DiscardHandler), mock.NewStore(), testAnchor)

		if server.log == nil {
			t.Error("expected logger to be set")
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
		server := NewGRPCDServer(nil, mock.NewStore(), testAnchor)

		if server.log == nil {
			t.Fatal("expected logger to be set even when nil passed")
		}
	})

	t.Run("substitutes a store when given none", func(t *testing.T) {
		server := NewGRPCDServer(slog.New(slog.DiscardHandler), nil, testAnchor)

		if server.store == nil {
			t.Fatal("expected store to be set even when nil passed")
		}
	})
}
