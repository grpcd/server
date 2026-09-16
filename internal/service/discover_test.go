package service

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"

	grpcd "github.com/grpcd/protos"

	"github.com/grpcd/server/internal/storage"
)

const method = "/package.Service/Method"

func TestDiscover(t *testing.T) {
	t.Run("offers a registered address", func(t *testing.T) {
		server, store := newServer()

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		stream := satisfiedAfter(t.Context(), method)

		if err := server.Discover(stream); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if got := stream.candidates(); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected one candidate, got %v", got)
		}
	})

	t.Run("offers the next address after one is reported dead", func(t *testing.T) {
		server, store := newServer()

		for _, address := range []string{"10.0.0.1:50054", "10.0.0.2:50054"} {
			if err := store.Add(t.Context(), address, testAnchor, []string{method}); err != nil {
				t.Fatalf("failed to seed: %v", err)
			}
		}

		stream := satisfiedAfter(t.Context(), method, "10.0.0.1:50054")

		if err := server.Discover(stream); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		want := []string{"10.0.0.1:50054", "10.0.0.2:50054"}
		if got := stream.candidates(); !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}

		if got := store.Addresses(method); !slices.Equal(got, []string{"10.0.0.2:50054"}) {
			t.Errorf("expected the dead address to be removed, got %v", got)
		}
	})

	t.Run("reinstates a removal it is told about", func(t *testing.T) {
		server, store := newServer()

		ctx, disconnect := context.WithCancel(peerContext(t.Context(), "10.0.0.1:41234"))
		defer disconnect()

		registration := newRegisterStream(ctx)

		held(server, &grpcd.RegisterRequest{Methods: []string{method}, Port: 50054}, registration)
		await(t, registration.acknowledged(), "registration was never acknowledged")

		removals, err := store.Watch(t.Context(), testAnchor)
		if err != nil {
			t.Fatalf("failed to watch: %v", err)
		}

		// A client that cannot reach the address reports it, even though the
		// service is up and holding its stream.
		go func() {
			_ = server.Discover(satisfiedAfter(t.Context(), method, "10.0.0.1:50054"))
		}()

		removal := await(t, removals, "the anchor was never told about the removal")

		if removal.Method != method || removal.Address != "10.0.0.1:50054" {
			t.Fatalf("expected the removed row to be named, got %+v", removal)
		}

		server.Reinstate(t.Context(), removal)

		if got := store.Addresses(method); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the address to be written back, got %v", got)
		}
	})

	t.Run("reports when reinstating fails", func(t *testing.T) {
		server, store := newServer()

		store.SetAddError(errors.New("storage unavailable"))

		server.Reinstate(t.Context(), storage.Removal{Method: method, Address: "10.0.0.1:50054"})

		if got := store.Addresses(method); len(got) != 0 {
			t.Errorf("expected nothing to be written, got %v", got)
		}
	})

	t.Run("waits for a method that is not registered and offers the first to arrive", func(t *testing.T) {
		server, store := newServer()

		stream := satisfiedAfter(t.Context(), method)

		exhausted := store.Exhausted(method)
		returned := discovering(server, stream)

		// Nothing is registered, so the handler is waiting. Only now does a
		// service register, and the handler is expected to be woken by it.
		await(t, exhausted, "handler never ran out of candidates")

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if err := await(t, returned, "handler did not offer the registration"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := stream.candidates(); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the registered address, got %v", got)
		}
	})

	t.Run("waits after every address is reported dead and offers the next to arrive", func(t *testing.T) {
		server, store := newServer()

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		stream := satisfiedAfter(t.Context(), method, "10.0.0.1:50054")

		exhausted := store.Exhausted(method)
		returned := discovering(server, stream)

		await(t, exhausted, "handler never ran out of candidates")

		if got := store.Addresses(method); len(got) != 0 {
			t.Errorf("expected the dead address to be removed before waiting, got %v", got)
		}

		if err := store.Add(t.Context(), "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if err := await(t, returned, "handler did not offer the registration"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{"10.0.0.1:50054", "10.0.0.2:50054"}
		if got := stream.candidates(); !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("answers not found instead of waiting when asked not to wait", func(t *testing.T) {
		server, _ := newServer()

		stream := asksWithoutWaiting(t.Context(), method)

		assertCode(t, server.Discover(stream), codes.NotFound)

		if got := stream.candidates(); len(got) != 0 {
			t.Errorf("expected no candidates, got %v", got)
		}
	})

	t.Run("answers not found once every address is reported dead when asked not to wait", func(t *testing.T) {
		server, store := newServer()

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		stream := asksWithoutWaiting(t.Context(), method, "10.0.0.1:50054")

		assertCode(t, server.Discover(stream), codes.NotFound)

		if got := stream.candidates(); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the one candidate offered first, got %v", got)
		}
		if got := store.Addresses(method); len(got) != 0 {
			t.Errorf("expected the dead address removed, got %v", got)
		}
	})

	t.Run("returns when the caller goes away while waiting", func(t *testing.T) {
		server, store := newServer()

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		exhausted := store.Exhausted(method)
		returned := discovering(server, satisfiedAfter(ctx, method))

		await(t, exhausted, "handler never ran out of candidates")

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("holds the discovery through a lost store and draws once it returns", func(t *testing.T) {
		server, store := newServer()

		store.SetAddressesForError(errors.New("storage unavailable"))
		store.Lose()

		stream := satisfiedAfter(t.Context(), method)

		failed := store.Failed()
		returned := discovering(server, stream)

		await(t, failed, "handler never tried to draw")

		store.SetAddressesForError(nil)

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		store.Recover()

		if err := await(t, returned, "handler did not offer after the store returned"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := stream.candidates(); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the address, got %v", got)
		}
	})

	t.Run("returns when the caller goes away while the store is lost", func(t *testing.T) {
		server, store := newServer()

		store.SetAddressesForError(errors.New("storage unavailable"))
		store.Lose()

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		failed := store.Failed()
		returned := discovering(server, satisfiedAfter(ctx, method))

		await(t, failed, "handler never tried to draw")

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("draws again when the store returns while waiting", func(t *testing.T) {
		server, store := newServer()

		stream := satisfiedAfter(t.Context(), method)

		exhausted := store.Exhausted(method)
		returned := discovering(server, stream)

		await(t, exhausted, "handler never ran out of candidates")

		// A recovery wakes the wait: the store may hold registrations this
		// instance was deaf to. Here it holds nothing, so the handler draws,
		// finds nothing, and waits again.
		exhausted = store.Exhausted(method)
		store.Recover()
		await(t, exhausted, "handler did not draw again after the store returned")

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if err := await(t, returned, "handler did not offer the registration"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("goes back to sleep when another method registers", func(t *testing.T) {
		server, store := newServer()

		stream := satisfiedAfter(t.Context(), method)

		exhausted := store.Exhausted(method)
		returned := discovering(server, stream)

		await(t, exhausted, "handler never ran out of candidates")

		// Every registration wakes every waiter. This one is for a method the
		// handler does not want, so it is expected to draw, find nothing, and
		// wait again.
		exhausted = store.Exhausted(method)

		if err := store.Add(t.Context(), "10.0.0.9:50054", testAnchor, []string{"/other.Service/Method"}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		await(t, exhausted, "handler did not draw again after the wake")

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if err := await(t, returned, "handler did not offer the registration"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := stream.candidates(); !slices.Equal(got, []string{"10.0.0.1:50054"}) {
			t.Errorf("expected the registered address, got %v", got)
		}
	})

	t.Run("refuses an invalid method name", func(t *testing.T) {
		server, _ := newServer()

		err := server.Discover(satisfiedAfter(t.Context(), "not-a-method"))

		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("returns when the request cannot be read", func(t *testing.T) {
		server, _ := newServer()

		stream := &discoverStream{
			ctx:      t.Context(),
			requests: []recvResult{{err: errors.New("broken transport")}},
		}

		if err := server.Discover(stream); err == nil {
			t.Fatal("expected the receive failure to be returned")
		}
	})

	t.Run("returns when the store cannot be read", func(t *testing.T) {
		server, store := newServer()

		store.SetAddressesForError(errors.New("storage unavailable"))

		err := server.Discover(satisfiedAfter(t.Context(), method))

		assertCode(t, err, codes.Internal)
	})

	t.Run("returns when a candidate cannot be sent", func(t *testing.T) {
		server, store := newServer()

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		stream := satisfiedAfter(t.Context(), method)
		stream.sendErr = errors.New("broken transport")

		if err := server.Discover(stream); err == nil {
			t.Fatal("expected the send failure to be returned")
		}
	})

	t.Run("returns when the report cannot be read", func(t *testing.T) {
		server, store := newServer()

		if err := store.Add(t.Context(), "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		stream := asks(t.Context(), method)
		stream.requests = append(stream.requests, recvResult{err: errors.New("broken transport")})

		if err := server.Discover(stream); err == nil {
			t.Fatal("expected the receive failure to be returned")
		}
	})

	t.Run("ignores a report naming no address", func(t *testing.T) {
		server, store := newServer()

		seedTwo(t, store)

		stream := asks(t.Context(), method)
		stream.requests = append(stream.requests,
			recvResult{request: &grpcd.DiscoverRequest{}},
			recvResult{err: io.EOF},
		)

		if err := server.Discover(stream); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{"10.0.0.1:50054", "10.0.0.2:50054"}
		if got := store.Addresses(method); !slices.Equal(got, want) {
			t.Errorf("expected both addresses to survive, got %v", got)
		}
	})

	t.Run("keeps going when a removal fails", func(t *testing.T) {
		server, store := newServer()

		seedTwo(t, store)
		store.SetRemoveFromMethodError(errors.New("storage unavailable"))

		stream := satisfiedAfter(t.Context(), method, "10.0.0.1:50054")

		if err := server.Discover(stream); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// The address was not removed, so the next draw is over the same set and
		// offers it again. A store that keeps failing is handled by the server
		// leaving, not by this handler.
		want := []string{"10.0.0.1:50054", "10.0.0.1:50054"}
		if got := stream.candidates(); !slices.Equal(got, want) {
			t.Errorf("expected %v after the failed removal, got %v", want, got)
		}
	})

	t.Run("keeps going when the anchor cannot be told", func(t *testing.T) {
		server, store := newServer()

		seedTwo(t, store)
		store.SetNotifyError(errors.New("storage unavailable"))

		stream := satisfiedAfter(t.Context(), method, "10.0.0.1:50054")

		if err := server.Discover(stream); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := stream.candidates(); len(got) != 2 {
			t.Errorf("expected the next candidate after the failed notify, got %v", got)
		}
	})

	t.Run("tells nobody about an address with no anchor", func(t *testing.T) {
		server, store := newServer()

		for _, address := range []string{"10.0.0.1:50054", "10.0.0.2:50054"} {
			if err := store.Add(t.Context(), address, "", []string{method}); err != nil {
				t.Fatalf("failed to seed: %v", err)
			}
		}

		stream := satisfiedAfter(t.Context(), method, "10.0.0.1:50054")

		if err := server.Discover(stream); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := stream.candidates(); len(got) != 2 {
			t.Errorf("expected the next candidate, got %v", got)
		}
	})
}
