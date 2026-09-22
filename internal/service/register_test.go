package service

import (
	"context"
	"errors"
	"slices"
	"testing"

	"connectrpc.com/connect/v2"
	"go.opentelemetry.io/otel/codes"
)

// peer is the connection a registering service arrives on: its IP and the
// ephemeral port it dialed from.
const peer = "192.168.1.100:41234"

func TestRegister(t *testing.T) {
	methods := []string{
		"/package.Service/Method",
		"/package.Service/Other",
	}

	t.Run("holds the rows for as long as the stream", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		if _, err := register(ctx, h.client(peer), registration(methods...)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		// The address pairs the IP off the connection with the port the caller
		// reported, rather than the ephemeral port it dialed from.
		for _, method := range methods {
			if got := h.store.Addresses(method); !slices.Equal(got, []string{"192.168.1.100:50054"}) {
				t.Errorf("method %s holds %v", method, got)
			}
		}

		if got := h.store.Anchor("192.168.1.100:50054"); got != testAnchor {
			t.Errorf("expected anchor %q, got %q", testAnchor, got)
		}

		disconnect()

		if err := await(t, returned, "handler did not return when the stream ended"); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		for _, method := range methods {
			if got := h.store.Addresses(method); len(got) != 0 {
				t.Errorf("method %s still holds %v after the stream ended", method, got)
			}
		}

		h.assertSpans(t, "register", codes.Unset)
		h.assertSpans(t, "remove", codes.Unset)
	})

	t.Run("leaves other addresses serving the method", func(t *testing.T) {
		h := newHarness(t)

		first, disconnectFirst := context.WithCancel(t.Context())
		defer disconnectFirst()

		second, disconnectSecond := context.WithCancel(t.Context())
		defer disconnectSecond()

		if _, err := register(first, h.client("10.0.0.1:41234"), registration(methods[:1]...)); err != nil {
			t.Fatalf("first registration was refused: %v", err)
		}

		returned := h.registering()

		if _, err := register(second, h.client("10.0.0.2:41234"), registration(methods[:1]...)); err != nil {
			t.Fatalf("second registration was refused: %v", err)
		}

		disconnectSecond()
		await(t, returned, "second handler did not return")

		if got := h.store.Addresses(methods[0]); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the first address to survive, got %v", got)
		}
	})

	t.Run("refuses a request with no methods", func(t *testing.T) {
		h := newHarness(t)

		_, err := register(t.Context(), h.client(peer), registration())

		assertCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("refuses a request with no port", func(t *testing.T) {
		h := newHarness(t)

		request := registration(methods...)
		request.Port = 0

		_, err := register(t.Context(), h.client(peer), request)

		assertCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("refuses a port outside the range", func(t *testing.T) {
		h := newHarness(t)

		request := registration(methods...)
		request.Port = 70000

		_, err := register(t.Context(), h.client(peer), request)

		assertCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("refuses a connection carrying no peer", func(t *testing.T) {
		h := newHarness(t)

		_, err := register(t.Context(), h.client(""), registration(methods...))

		assertCode(t, err, connect.CodeInternal)
	})

	t.Run("refuses a call carrying no connection info", func(t *testing.T) {
		h := newHarness(t)

		// A bare context is a call the dispatcher never saw.
		_, err := h.server.address(t.Context(), 50054)

		assertCode(t, err, connect.CodeInternal)
	})

	t.Run("composes an address from a peer with no port", func(t *testing.T) {
		h := newHarness(t)

		if _, err := register(t.Context(), h.client("/tmp/grpcd.sock"), registration(methods[:1]...)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		if got := h.store.Addresses(methods[0]); !slices.Equal(got, []string{"/tmp/grpcd.sock:50054"}) {
			t.Fatalf("expected the peer joined to the port, got %v", got)
		}
	})

	t.Run("refuses when the rows cannot be written", func(t *testing.T) {
		h := newHarness(t)

		h.store.SetAddError(errors.New("storage unavailable"))

		_, err := register(t.Context(), h.client(peer), registration(methods...))

		assertCode(t, err, connect.CodeInternal)
		h.assertSpans(t, "register", codes.Error)
	})

	t.Run("removes the rows when the acknowledgement cannot be sent", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		added := h.store.Added()
		returned := h.registering()

		if _, err := h.client(peer).Register(ctx, registration(methods...)); err != nil {
			t.Fatalf("failed to open the registration: %v", err)
		}

		// The rows are written and the acknowledgement is next. The caller
		// leaves without ever taking it, so the send fails.
		await(t, added, "handler never wrote the rows")
		disconnect()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the send failure to be returned")
		}

		if got := h.store.Addresses(methods[0]); len(got) != 0 {
			t.Errorf("expected the rows to be removed, got %v", got)
		}

		h.assertSpans(t, "register", codes.Error)
		h.assertSpans(t, "remove", codes.Unset)
	})

	t.Run("returns when the rows cannot be removed", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		if _, err := register(ctx, h.client(peer), registration(methods...)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		h.store.SetRemoveError(errors.New("storage unavailable"))

		disconnect()

		if err := await(t, returned, "handler did not return"); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		h.assertSpans(t, "remove", codes.Error)
	})

	t.Run("holds the registration through a lost store and writes it once the store returns", func(t *testing.T) {
		h := newHarness(t)

		h.store.SetAddError(errors.New("storage unavailable"))
		h.store.Lose()

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		waiting := h.store.Waiting()
		returned := h.registering()

		stream, err := h.client(peer).Register(ctx, registration(methods...))
		if err != nil {
			t.Fatalf("failed to open the registration: %v", err)
		}

		// The write failed against a lost store, so the handler is waiting
		// rather than refusing, and nothing is written.
		await(t, waiting, "handler never judged the failed write")

		if got := h.store.Addresses(methods[0]); len(got) != 0 {
			t.Fatalf("expected nothing written while the store is lost, got %v", got)
		}

		h.store.SetAddError(nil)
		h.store.Recover()

		if _, err := stream.Receive(); err != nil {
			t.Fatalf("registration was never acknowledged after the store returned: %v", err)
		}

		if got := h.store.Addresses(methods[0]); !slices.Equal(got, []string{"192.168.1.100:50054"}) {
			t.Errorf("expected the rows to be written, got %v", got)
		}

		disconnect()
		await(t, returned, "handler did not return")
	})

	t.Run("returns when the caller leaves while the store is lost", func(t *testing.T) {
		h := newHarness(t)

		h.store.SetAddError(errors.New("storage unavailable"))
		h.store.Lose()

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		waiting := h.store.Waiting()
		returned := h.registering()

		if _, err := h.client(peer).Register(ctx, registration(methods...)); err != nil {
			t.Fatalf("failed to open the registration: %v", err)
		}

		await(t, waiting, "handler never judged the failed write")

		disconnect()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("rewrites the rows when the store returns while holding", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		if _, err := register(ctx, h.client(peer), registration(methods...)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		// Losing the store while holding is only something to wait out: the
		// handler looks, sees it lost, and sleeps again without writing.
		conditioned := h.store.Conditioned()
		h.store.Lose()
		await(t, conditioned, "handler did not look at the store after it was lost")

		// The store coming back may have come back empty, so the rows are
		// written again from the request the handler holds.
		added := h.store.Added()
		h.store.Recover()
		await(t, added, "handler did not rewrite the rows after the store returned")

		if got := h.store.Adds(); got != 2 {
			t.Errorf("expected the rows to be written twice, got %d", got)
		}

		disconnect()
		await(t, returned, "handler did not return")
	})

	t.Run("keeps holding when the rewrite fails", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		if _, err := register(ctx, h.client(peer), registration(methods...)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		h.store.SetAddError(errors.New("storage unavailable"))

		failed := h.store.Failed()
		h.store.Recover()
		await(t, failed, "handler did not try to rewrite the rows")

		// A later recovery is tried again.
		h.store.SetAddError(nil)

		added := h.store.Added()
		h.store.Recover()
		await(t, added, "handler did not rewrite the rows on the next recovery")

		disconnect()
		await(t, returned, "handler did not return")
	})

	t.Run("acknowledges once", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		stream, err := register(ctx, h.client(peer), registration(methods...))
		if err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		disconnect()
		await(t, returned, "handler did not return")

		// The stream ended with the handler, so there is no second
		// acknowledgement to take off it.
		if _, err := stream.Receive(); err == nil {
			t.Error("expected no second acknowledgement")
		}
	})
}
