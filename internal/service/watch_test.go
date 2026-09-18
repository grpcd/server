package service

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect/v2"
	"google.golang.org/protobuf/proto"

	grpcd "github.com/grpcd/protos"
	"github.com/grpcd/protos/grpcdconnect"
)

// holding is a request holding address for method.
func holding(address string) *grpcd.WatchRequest {
	return &grpcd.WatchRequest{MethodName: method, Address: address}
}

// serverStreamStub is the transport's side of one Watch, handed to the
// dispatcher directly: Receive answers with request once, and Send fails with
// sendErr.
type serverStreamStub struct {
	request *grpcd.WatchRequest
	sendErr error
}

func (s *serverStreamStub) Receive(msg any) error {
	proto.Merge(msg.(proto.Message), s.request)

	return nil
}

func (*serverStreamStub) SendHeaders() error { return nil }

func (s *serverStreamStub) Send(any) error { return s.sendErr }

// always is a draw every holder wins.
func always(int64) bool { return true }

// watch opens a Watch for req over client and takes the empty first message
// that says the watch is held, answering with the stream the moves come on.
func watch(
	ctx context.Context, t *testing.T, client grpcdconnect.GRPCDServiceClient, req *grpcd.WatchRequest,
) grpcdconnect.GRPCDServiceWatchClientStream {
	t.Helper()

	stream, err := client.Watch(ctx, req)
	if err != nil {
		t.Fatalf("failed to open the watch: %v", err)
	}

	held, err := stream.Receive()
	if err != nil {
		t.Fatalf("the watch never opened: %v", err)
	}
	if held.GetAddress() != "" {
		t.Fatalf("first message = %q, want an empty one", held.GetAddress())
	}

	return stream
}

// told answers with the address the watch stream carries next, failing the
// test if it ends instead.
func told(t *testing.T, stream interface {
	Receive() (*grpcd.WatchResponse, error)
}) string {
	t.Helper()

	move, err := stream.Receive()
	if err != nil {
		t.Fatalf("holder was never told: %v", err)
	}

	return move.GetAddress()
}

func TestWatch(t *testing.T) {
	t.Run("refuses an invalid method name", func(t *testing.T) {
		h := newHarness()

		stream, err := h.client("").Watch(
			t.Context(), &grpcd.WatchRequest{MethodName: "not-a-method", Address: "10.0.0.1:50054"},
		)
		if err != nil {
			t.Fatalf("failed to open the watch: %v", err)
		}

		_, err = stream.Receive()

		assertCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("refuses an empty address", func(t *testing.T) {
		h := newHarness()

		stream, err := h.client("").Watch(t.Context(), holding(""))
		if err != nil {
			t.Fatalf("failed to open the watch: %v", err)
		}

		_, err = stream.Receive()

		assertCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("returns when the stream cannot be opened", func(t *testing.T) {
		h := newHarness()

		rpc := connect.NewServer(loggerInterceptor(h.server.log))
		grpcdconnect.RegisterGRPCDServiceHandler(rpc, h.server)

		// The transport's stream refuses the acknowledgement, the one send the
		// handler makes before a move.
		unopenable := &serverStreamStub{request: holding("10.0.0.1:50054"), sendErr: errors.New("connection reset")}

		err := rpc.Call(t.Context(), grpcdconnect.GRPCDServiceWatchProcedure, nil, unopenable)
		if !errors.Is(err, unopenable.sendErr) {
			t.Fatalf("error = %v, want the transport's", err)
		}
	})

	t.Run("returns when the caller goes away", func(t *testing.T) {
		h := newHarness()

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		returned := h.watching()

		watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("tells the holder about a new address when the draw wins", func(t *testing.T) {
		h := newHarness()
		h.server.roll = always

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		loaded := h.store.Loaded()
		returned := h.watching()

		stream := watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		// Registered only once the handler holds the announcement it sleeps
		// on, so the registration reaches it as a wake.
		await(t, loaded, "handler never began watching")

		if err := h.store.Add(ctx, "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if got := told(t, stream); got != "10.0.0.2:50054" {
			t.Errorf("told %q, want 10.0.0.2:50054", got)
		}

		leave()
		await(t, returned, "handler did not return")
	})

	t.Run("says nothing about other methods or the address held", func(t *testing.T) {
		h := newHarness()
		h.server.roll = always

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		loaded := h.store.Loaded()
		returned := h.watching()

		stream := watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		await(t, loaded, "handler never began watching")

		// Each registration is made only once the handler has reloaded after
		// the previous one, so every wake is seen and judged on its own.
		for _, registration := range []struct{ address, method string }{
			{"10.0.0.9:50054", "/other.Service/Method"},
			{"10.0.0.1:50054", method},
		} {
			loaded = h.store.Loaded()

			if err := h.store.Add(ctx, registration.address, testAnchor, []string{registration.method}); err != nil {
				t.Fatalf("failed to register: %v", err)
			}

			await(t, loaded, "handler did not wake")
		}

		if err := h.store.Add(ctx, "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		// Messages arrive in the order they are sent, so the first one being
		// this registration is the earlier ones having said nothing.
		if got := told(t, stream); got != "10.0.0.2:50054" {
			t.Errorf("told %q, want only 10.0.0.2:50054", got)
		}

		leave()
		await(t, returned, "handler did not return")
	})

	t.Run("says nothing when the draw loses", func(t *testing.T) {
		h := newHarness()

		// Wins only once three addresses serve the method, so the second
		// registration is told and the first is not. Every draw is reported,
		// so the next registration is made only once the previous one has
		// been counted and judged.
		draws := make(chan int64, 2)
		h.server.roll = func(n int64) bool {
			draws <- n

			return n == 3
		}

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		if err := h.store.Add(ctx, "10.0.0.1:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}

		loaded := h.store.Loaded()
		returned := h.watching()

		stream := watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		await(t, loaded, "handler never began watching")

		if err := h.store.Add(ctx, "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if n := await(t, draws, "handler did not draw"); n != 2 {
			t.Fatalf("drew against %d addresses, want 2", n)
		}

		if err := h.store.Add(ctx, "10.0.0.3:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if n := await(t, draws, "handler did not draw"); n != 3 {
			t.Fatalf("drew against %d addresses, want 3", n)
		}

		if got := told(t, stream); got != "10.0.0.3:50054" {
			t.Errorf("told %q, want only 10.0.0.3:50054", got)
		}

		leave()
		await(t, returned, "handler did not return")
	})

	t.Run("returns when the count fails", func(t *testing.T) {
		h := newHarness()
		h.server.roll = always

		h.store.SetCountError(errors.New("storage unavailable"))

		loaded := h.store.Loaded()

		stream := watch(t.Context(), t, h.client(""), holding("10.0.0.1:50054"))

		await(t, loaded, "handler never began watching")

		if err := h.store.Add(t.Context(), "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		_, err := stream.Receive()

		assertCode(t, err, connect.CodeInternal)
	})

	t.Run("holds the watch through a lost store", func(t *testing.T) {
		h := newHarness()
		h.server.roll = always

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		h.store.SetCountError(errors.New("storage unavailable"))
		h.store.Lose()

		loaded := h.store.Loaded()
		returned := h.watching()

		stream := watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		await(t, loaded, "handler never began watching")

		waiting := h.store.Waiting()

		if err := h.store.Add(ctx, "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		await(t, waiting, "handler never judged the failed count")

		// The addition that woke the handler is gone with the store; the next
		// one after recovery is told.
		h.store.SetCountError(nil)
		h.store.Recover()

		if err := h.store.Add(ctx, "10.0.0.3:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		if got := told(t, stream); got != "10.0.0.3:50054" {
			t.Errorf("told %q, want 10.0.0.3:50054", got)
		}

		leave()
		await(t, returned, "handler did not return")
	})

	t.Run("returns when the caller goes away while the store is lost", func(t *testing.T) {
		h := newHarness()
		h.server.roll = always

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		h.store.SetCountError(errors.New("storage unavailable"))
		h.store.Lose()

		loaded := h.store.Loaded()
		returned := h.watching()

		watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		await(t, loaded, "handler never began watching")

		waiting := h.store.Waiting()

		if err := h.store.Add(ctx, "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		await(t, waiting, "handler never judged the failed count")

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the cancellation to be returned")
		}
	})

	t.Run("returns when the holder cannot be told", func(t *testing.T) {
		h := newHarness()
		h.server.roll = always

		ctx, leave := context.WithCancel(t.Context())
		defer leave()

		loaded := h.store.Loaded()
		returned := h.watching()

		watch(ctx, t, h.client(""), holding("10.0.0.1:50054"))

		await(t, loaded, "handler never began watching")

		// Woken and past the wait, so the move is the next thing the handler
		// sends. The caller leaves without ever taking it, so the send fails.
		loaded = h.store.Loaded()

		if err := h.store.Add(ctx, "10.0.0.2:50054", testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to register: %v", err)
		}

		await(t, loaded, "handler did not wake")

		leave()

		if err := await(t, returned, "handler did not return"); err == nil {
			t.Fatal("expected the send failure to be returned")
		}
	})
}
