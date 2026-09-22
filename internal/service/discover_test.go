package service

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"connectrpc.com/connect/v2"
	"go.opentelemetry.io/otel/codes"

	grpcd "github.com/grpcd/protos"

	"github.com/grpcd/server/internal/storage"
)

const method = "/package.Service/Method"

func TestDiscover(t *testing.T) {
	t.Run("offers a registered address", func(t *testing.T) {
		h := newHarness(t)

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		candidates, err := discover(t.Context(), h.client(""), method)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !slices.Equal(candidates, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected one candidate, got %v", candidates)
		}
	})

	t.Run("offers the next address after one is reported dead", func(t *testing.T) {
		h := newHarness(t)

		seedTwo(t, h.store)

		candidates, err := discover(t.Context(), h.client(""), method, "10.0.0.1:50054")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		want := []string{"10.0.0.1:50054", "10.0.0.2:50054"}
		if !slices.Equal(candidates, want) {
			t.Errorf("expected %v, got %v", want, candidates)
		}

		if got := h.store.Addresses(method); !slices.Equal(got, []string{"10.0.0.2:50054"}) {
			t.Errorf("expected the dead address to be removed, got %v", got)
		}

		h.assertSpans(t, "remove", codes.Unset)
	})

	t.Run("reinstates a removal it is told about", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		if _, err := register(ctx, h.client("10.0.0.1:41234"), registration(method)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		removals, err := h.store.Watch(t.Context(), testAnchor)
		if err != nil {
			t.Fatalf("failed to watch: %v", err)
		}

		// A client that cannot reach the address reports it, even though the
		// service is up and holding its stream. The report is delivered to the
		// anchor only once this test takes it, so the client runs alongside.
		go func() {
			_, _ = discover(t.Context(), h.client(""), method, "10.0.0.1:50054")
		}()

		removal := await(t, removals, "the anchor was never told about the removal")

		if removal.Method != method || removal.Address != "10.0.0.1:50054" {
			t.Fatalf("expected the removed row to be named, got %+v", removal)
		}

		// The holder writes its registration again; the write is its next
		// addition.
		written := h.store.Latest()

		h.server.Reinstate(t.Context(), removal)

		await(t, written.Done, "the registration was never written again")

		if got := h.store.Addresses(method); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the address to be written back, got %v", got)
		}

		// The span ends after the write; the handler returning is what says it
		// has.
		disconnect()
		await(t, returned, "handler did not return")

		h.assertSpans(t, "revert", codes.Unset)
	})

	t.Run("leaves a removed row that the holder never registered", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		// The address is held for one method, and a row for another was left
		// on it by an earlier occupant.
		if _, err := register(ctx, h.client("10.0.0.1:41234"), registration(method)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		const stale = "/earlier.Service/Method"

		if err := h.store.Add(ctx, "10.0.0.1:50054", testAnchor, []string{stale}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		removals, err := h.store.Watch(t.Context(), testAnchor)
		if err != nil {
			t.Fatalf("failed to watch: %v", err)
		}

		go func() {
			_, _ = discover(t.Context(), h.client(""), stale, "10.0.0.1:50054")
		}()

		removal := await(t, removals, "the anchor was never told about the removal")

		written := h.store.Latest()

		h.server.Reinstate(t.Context(), removal)

		await(t, written.Done, "the registration was never written again")

		if got := h.store.Addresses(stale); len(got) != 0 {
			t.Errorf("expected the stale row to stay removed, got %v", got)
		}
		if got := h.store.Addresses(method); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the registration to stand, got %v", got)
		}
	})

	t.Run("leaves a removal at an address no stream holds", func(t *testing.T) {
		server, store := newServer()

		server.Reinstate(t.Context(), storage.Removal{Method: method, Address: "10.0.0.1:50054"})

		if got := store.Adds(); got != 0 {
			t.Errorf("expected nothing to be written, got %d writes", got)
		}
	})

	t.Run("reports when the registration cannot be written again", func(t *testing.T) {
		h := newHarness(t)

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		if _, err := register(ctx, h.client("10.0.0.1:41234"), registration(method)); err != nil {
			t.Fatalf("registration was refused: %v", err)
		}

		h.store.SetAddError(errors.New("storage unavailable"))

		failed := h.store.Failed()

		h.server.Reinstate(t.Context(), storage.Removal{Method: method, Address: "10.0.0.1:50054"})

		await(t, failed, "the holder never tried to write")

		disconnect()
		await(t, returned, "handler did not return")

		h.assertSpans(t, "revert", codes.Error)
	})

	t.Run("waits for a method that is not registered and offers the first to arrive", func(t *testing.T) {
		h := newHarness(t)

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		exhausted := h.store.Exhausted(method)

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		// Nothing is registered, so the handler is waiting. Only now does a
		// service register, and the handler is expected to be woken by it.
		await(t, exhausted, "handler never ran out of candidates")

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		candidate, err := stream.Receive()
		if err != nil {
			t.Fatalf("handler did not offer the registration: %v", err)
		}

		if got := candidate.GetAddress(); got != "10.0.0.1:50054" {
			t.Errorf("expected the registered address, got %q", got)
		}

		if err := stream.CloseSend(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}

		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected the stream to end cleanly, got %v", err)
		}
	})

	t.Run("waits after every address is reported dead and offers the next to arrive", func(t *testing.T) {
		h := newHarness(t)

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		exhausted := h.store.Exhausted(method)

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		first, err := stream.Receive()
		if err != nil {
			t.Fatalf("handler did not offer the seeded address: %v", err)
		}

		if err := stream.Send(reporting(first.GetAddress())); err != nil {
			t.Fatalf("failed to report: %v", err)
		}

		await(t, exhausted, "handler never ran out of candidates")

		if got := h.store.Addresses(method); len(got) != 0 {
			t.Errorf("expected the dead address to be removed before waiting, got %v", got)
		}

		if err := h.store.Add(t.Context(), "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		second, err := stream.Receive()
		if err != nil {
			t.Fatalf("handler did not offer the registration: %v", err)
		}

		want := []string{"10.0.0.1:50054", "10.0.0.2:50054"}
		if got := []string{first.GetAddress(), second.GetAddress()}; !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("answers not found instead of waiting when asked not to wait", func(t *testing.T) {
		h := newHarness(t)

		candidates, err := discoverWithoutWaiting(t.Context(), h.client(""), method)

		assertCode(t, err, connect.CodeNotFound)

		if len(candidates) != 0 {
			t.Errorf("expected no candidates, got %v", candidates)
		}
	})

	t.Run("answers not found once every address is reported dead when asked not to wait", func(t *testing.T) {
		h := newHarness(t)

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		candidates, err := discoverWithoutWaiting(t.Context(), h.client(""), method, "10.0.0.1:50054")

		assertCode(t, err, connect.CodeNotFound)

		if !slices.Equal(candidates, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the one candidate offered first, got %v", candidates)
		}
		if got := h.store.Addresses(method); len(got) != 0 {
			t.Errorf("expected the dead address removed, got %v", got)
		}
	})

	t.Run("returns when the caller goes away while waiting", func(t *testing.T) {
		h := newHarness(t)

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		stream, err := h.client("").Discover(ctx)
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}

		exhausted := h.store.Exhausted(method)
		returned := h.discovering()

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		await(t, exhausted, "handler never ran out of candidates")

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("holds the discovery through a lost store and draws once it returns", func(t *testing.T) {
		h := newHarness(t)

		h.store.SetAddressesForError(errors.New("storage unavailable"))
		h.store.Lose()

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		waiting := h.store.Waiting()

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		await(t, waiting, "handler never judged the failed draw")

		h.store.SetAddressesForError(nil)

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		h.store.Recover()

		candidate, err := stream.Receive()
		if err != nil {
			t.Fatalf("handler did not offer after the store returned: %v", err)
		}

		if got := candidate.GetAddress(); got != "10.0.0.1:50054" {
			t.Errorf("expected the address, got %q", got)
		}
	})

	t.Run("returns when the caller goes away while the store is lost", func(t *testing.T) {
		h := newHarness(t)

		h.store.SetAddressesForError(errors.New("storage unavailable"))
		h.store.Lose()

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		stream, err := h.client("").Discover(ctx)
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}

		waiting := h.store.Waiting()
		returned := h.discovering()

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		await(t, waiting, "handler never judged the failed draw")

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("draws again when the store returns while waiting", func(t *testing.T) {
		h := newHarness(t)

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		exhausted := h.store.Exhausted(method)

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		await(t, exhausted, "handler never ran out of candidates")

		// A recovery wakes the wait: the store may hold registrations this
		// instance was deaf to. Here it holds nothing, so the handler draws,
		// finds nothing, and waits again.
		exhausted = h.store.Exhausted(method)
		h.store.Recover()
		await(t, exhausted, "handler did not draw again after the store returned")

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if _, err := stream.Receive(); err != nil {
			t.Fatalf("handler did not offer the registration: %v", err)
		}
	})

	t.Run("goes back to sleep when another method registers", func(t *testing.T) {
		h := newHarness(t)

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		exhausted := h.store.Exhausted(method)

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		await(t, exhausted, "handler never ran out of candidates")

		// Every registration wakes every waiter. This one is for a method the
		// handler does not want, so it is expected to draw, find nothing, and
		// wait again.
		exhausted = h.store.Exhausted(method)

		if err := h.store.Add(t.Context(), "10.0.0.9:50054", testAnchor, []string{"/other.Service/Method"}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		await(t, exhausted, "handler did not draw again after the wake")

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		candidate, err := stream.Receive()
		if err != nil {
			t.Fatalf("handler did not offer the registration: %v", err)
		}

		if got := candidate.GetAddress(); got != "10.0.0.1:50054" {
			t.Errorf("expected the registered address, got %q", got)
		}
	})

	t.Run("refuses an invalid method name", func(t *testing.T) {
		h := newHarness(t)

		_, err := discover(t.Context(), h.client(""), "not-a-method")

		assertCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("returns when the request cannot be read", func(t *testing.T) {
		h := newHarness(t)

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		// Closed before asking anything: the handler's first receive finds the
		// stream already over.
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}

		if _, err := stream.Receive(); err == nil {
			t.Fatal("expected the receive failure to be returned")
		}
	})

	t.Run("returns when the store cannot be read", func(t *testing.T) {
		h := newHarness(t)

		h.store.SetAddressesForError(errors.New("storage unavailable"))

		_, err := discover(t.Context(), h.client(""), method)

		assertCode(t, err, connect.CodeInternal)
	})

	t.Run("returns when a candidate cannot be sent", func(t *testing.T) {
		h := newHarness(t)

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		stream, err := h.client("").Discover(ctx)
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}

		returned := h.discovering()

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		// The candidate is the next thing the handler sends, and the caller
		// leaves without ever taking it, so the send fails.
		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the send failure to be returned")
		}
	})

	t.Run("returns when the report cannot be read", func(t *testing.T) {
		h := newHarness(t)

		if err := h.store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		stream, err := h.client("").Discover(ctx)
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}

		returned := h.discovering()

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		if _, err := stream.Receive(); err != nil {
			t.Fatalf("handler did not offer the seeded address: %v", err)
		}

		// The handler is waiting for the verdict, and the caller leaves
		// instead of giving one.
		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the receive failure to be returned")
		}
	})

	t.Run("ignores a report naming no address", func(t *testing.T) {
		h := newHarness(t)

		seedTwo(t, h.store)

		stream, err := h.client("").Discover(t.Context())
		if err != nil {
			t.Fatalf("failed to open the discovery: %v", err)
		}
		defer stream.Close()

		if err := stream.Send(asking(method)); err != nil {
			t.Fatalf("failed to ask: %v", err)
		}

		if _, err := stream.Receive(); err != nil {
			t.Fatalf("handler did not offer a candidate: %v", err)
		}

		if err := stream.Send(&grpcd.DiscoverRequest{}); err != nil {
			t.Fatalf("failed to report: %v", err)
		}

		if _, err := stream.Receive(); err != nil {
			t.Fatalf("handler did not offer the next candidate: %v", err)
		}

		if err := stream.CloseSend(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}

		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected the stream to end cleanly, got %v", err)
		}

		want := []string{"10.0.0.1:50054", "10.0.0.2:50054"}
		if got := h.store.Addresses(method); !slices.Equal(got, want) {
			t.Errorf("expected both addresses to survive, got %v", got)
		}
	})

	t.Run("keeps going when a removal fails", func(t *testing.T) {
		h := newHarness(t)

		seedTwo(t, h.store)
		h.store.SetRemoveFromMethodError(errors.New("storage unavailable"))

		candidates, err := discover(t.Context(), h.client(""), method, "10.0.0.1:50054")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// The address was not removed, so the next draw is over the same set and
		// offers it again. A store that keeps failing is handled by the server
		// leaving, not by this handler.
		want := []string{"10.0.0.1:50054", "10.0.0.1:50054"}
		if !slices.Equal(candidates, want) {
			t.Errorf("expected %v after the failed removal, got %v", want, candidates)
		}

		h.assertSpans(t, "remove", codes.Error)
	})

	t.Run("keeps going when the anchor cannot be told", func(t *testing.T) {
		h := newHarness(t)

		seedTwo(t, h.store)
		h.store.SetNotifyError(errors.New("storage unavailable"))

		candidates, err := discover(t.Context(), h.client(""), method, "10.0.0.1:50054")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(candidates) != 2 {
			t.Errorf("expected the next candidate after the failed notify, got %v", candidates)
		}
	})

	t.Run("tells nobody about an address with no anchor", func(t *testing.T) {
		h := newHarness(t)

		for _, address := range []string{"10.0.0.1:50054", "10.0.0.2:50054"} {
			if err := h.store.Add(t.Context(), address, "", []string{method}); err != nil {
				t.Fatalf("failed to seed: %v", err)
			}
		}

		candidates, err := discover(t.Context(), h.client(""), method, "10.0.0.1:50054")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(candidates) != 2 {
			t.Errorf("expected the next candidate, got %v", candidates)
		}
	})
}
