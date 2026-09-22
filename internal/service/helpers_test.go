package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	grpcd "github.com/grpcd/protos"
	"github.com/grpcd/protos/grpcdconnect"
	"github.com/pbrpc/otel-testing/mocks/tracer"

	"github.com/grpcd/server/internal/storage/mock"
)

const testAnchor = "anchor-under-test"

// newServer builds a server on a fresh mock store, which the caller keeps to
// assert on.
func newServer() (*GRPCDServer, *mock.Store) {
	store := mock.NewStore()

	return NewGRPCDServer(slog.New(slog.DiscardHandler), store, testAnchor), store
}

// harness serves a GRPCDServer in-process: plain function calls through the
// generated handler and client, no listener, no network. Messages are handed
// over unbuffered, so a handler's Send returns only once the test has
// received it, and a handler's Receive only once the test has sent or closed.
//
// It also reports each handler's return to a test that asked for it. The
// transport hands that verdict to the client's Receive, which after the caller
// leaves may answer with the cancellation before the handler has returned, so
// a test that acts on the handler having finished waits here instead.
type harness struct {
	server *GRPCDServer
	store  *mock.Store

	// Every call arrives under a span this records, the way the HTTP layer
	// starts one per request, so the spans the handlers start under it are
	// recorded too.
	tracer *tracer.Mock

	// Signals a test registers to be handed the next return of each handler.
	mu         sync.Mutex
	registered chan error
	discovered chan error
	watched    chan error
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	server, store := newServer()

	recorder, _ := tracer.New(t)
	t.Cleanup(func() { recorder.Shutdown(t) })

	return &harness{server: server, store: store, tracer: recorder}
}

// spanStatuses answers with the status of every recorded span named name, in
// the order they ended.
func (h *harness) spanStatuses(name string) []codes.Code {
	var statuses []codes.Code

	for _, span := range h.tracer.GetSpans() {
		if span.Name == name {
			statuses = append(statuses, span.Status.Code)
		}
	}

	return statuses
}

// assertSpans fails the test unless the spans named name were recorded with
// exactly want as their statuses.
func (h *harness) assertSpans(t *testing.T, name string, want ...codes.Code) {
	t.Helper()

	if got := h.spanStatuses(name); !slices.Equal(got, want) {
		t.Errorf("%s spans = %v, want %v", name, got, want)
	}
}

// client answers with a generated client whose calls reach the server
// in-process, each carrying peer as the connection's address. The in-process
// transport has no network peer of its own, so an empty peer is a call that
// arrives with none.
func (h *harness) client(peer string) grpcdconnect.GRPCDServiceClient {
	interceptors := []connect.ServerInterceptor{
		loggerInterceptor(h.server.log),
		spanInterceptor(h.tracer.Tracer("test")),
	}

	if peer != "" {
		interceptors = append(interceptors, peerInterceptor(peer))
	}

	rpc := connect.NewServer(interceptors...)
	grpcdconnect.RegisterGRPCDServiceHandler(rpc, h)

	return grpcdconnect.NewGRPCDServiceClient(connect.NewClient(connectinprocess.New(rpc)))
}

// loggerInterceptor puts log in every call's context, the way the foundation's
// server does, so the handlers log where the server under test does.
func loggerInterceptor(log *slog.Logger) connect.ServerInterceptor {
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			return next(logger.ContextWithLogger(ctx, log), spec, stream)
		}
	}
}

// spanInterceptor starts a span for every call from tr, the way the HTTP
// layer starts one per request.
func spanInterceptor(tr trace.Tracer) connect.ServerInterceptor {
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			ctx, span := tr.Start(ctx, spec.Procedure)
			defer span.End()

			return next(ctx, spec, stream)
		}
	}
}

// peerInterceptor records address as the peer of every call, the way the HTTP
// transport records the socket's remote address.
func peerInterceptor(address string) connect.ServerInterceptor {
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			if info, ok := connect.CallInfoForServerContext(ctx); ok {
				info.PeerAddr = address
			}

			return next(ctx, spec, stream)
		}
	}
}

func (h *harness) Register(
	ctx context.Context, req *grpcd.RegisterRequest, stream grpcdconnect.GRPCDServiceRegisterServerStream,
) error {
	err := h.server.Register(ctx, req, stream)
	h.report(&h.registered, err)

	return err
}

func (h *harness) Discover(ctx context.Context, stream grpcdconnect.GRPCDServiceDiscoverServerStream) error {
	err := h.server.Discover(ctx, stream)
	h.report(&h.discovered, err)

	return err
}

func (h *harness) Watch(
	ctx context.Context, req *grpcd.WatchRequest, stream grpcdconnect.GRPCDServiceWatchServerStream,
) error {
	err := h.server.Watch(ctx, req, stream)
	h.report(&h.watched, err)

	return err
}

// registering answers with a channel carrying the next Register handler's
// return. A test asks before the handler can return; a return nobody asked
// for is dropped.
func (h *harness) registering() <-chan error { return h.expect(&h.registered) }

// discovering answers with a channel carrying the next Discover handler's
// return.
func (h *harness) discovering() <-chan error { return h.expect(&h.discovered) }

// watching answers with a channel carrying the next Watch handler's return.
func (h *harness) watching() <-chan error { return h.expect(&h.watched) }

// expect hands out a fresh signal for slot.
func (h *harness) expect(slot *chan error) <-chan error {
	h.mu.Lock()
	defer h.mu.Unlock()

	signal := make(chan error, 1)
	*slot = signal

	return signal
}

// report hands err to the signal in slot, if one is registered. The signal is
// buffered, so the handler's goroutine never waits on the test.
func (h *harness) report(slot *chan error, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if *slot != nil {
		*slot <- err
		*slot = nil
	}
}

// await blocks until signal fires and answers with what it carried, failing
// the test if the test's own context ends first.
func await[T any](t *testing.T, signal <-chan T, message string) T {
	t.Helper()

	select {
	case value := <-signal:
		return value
	case <-t.Context().Done():
		t.Fatal(message)
	}

	var zero T

	return zero
}

// assertCode fails the test unless err carries want.
func assertCode(t *testing.T, err error, want connect.Code) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected %v, got no error", want)
	}

	if got := connect.CodeOf(err); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

// registration is a request registering methods on port 50054.
func registration(methods ...string) *grpcd.RegisterRequest {
	return &grpcd.RegisterRequest{Methods: methods, Port: 50054}
}

// register opens a registration for req over client and waits for grpcd's
// answer to it: nil once it is acknowledged, otherwise the refusal. An
// acknowledged stream is held until ctx ends.
func register(
	ctx context.Context, client grpcdconnect.GRPCDServiceClient, req *grpcd.RegisterRequest,
) (grpcdconnect.GRPCDServiceRegisterClientStream, error) {
	stream, err := client.Register(ctx, req)
	if err != nil {
		return stream, err
	}

	_, err = stream.Receive()

	return stream, err
}

// asking is the message opening a discovery for method.
func asking(method string) *grpcd.DiscoverRequest {
	return &grpcd.DiscoverRequest{Step: &grpcd.DiscoverRequest_MethodName{MethodName: method}}
}

// askingWithoutWaiting is the message opening a discovery for method by a
// caller that declines to wait for one that nothing serves.
func askingWithoutWaiting(method string) *grpcd.DiscoverRequest {
	request := asking(method)
	request.NoWait = true

	return request
}

// reporting is the message reporting address as unreachable.
func reporting(address string) *grpcd.DiscoverRequest {
	return &grpcd.DiscoverRequest{Step: &grpcd.DiscoverRequest_DeadAddress{DeadAddress: address}}
}

// discover asks for method over client and answers with the candidates
// offered, reporting each of dead as unreachable in turn and closing the
// stream once they are spent, which is how a caller says the last candidate
// worked. The error is how grpcd ended the stream instead: nil when it let
// the caller close it.
func discover(
	ctx context.Context, client grpcdconnect.GRPCDServiceClient, method string, dead ...string,
) ([]string, error) {
	return ask(ctx, client, asking(method), dead...)
}

// discoverWithoutWaiting is discover for a caller that declines to wait.
func discoverWithoutWaiting(
	ctx context.Context, client grpcdconnect.GRPCDServiceClient, method string, dead ...string,
) ([]string, error) {
	return ask(ctx, client, askingWithoutWaiting(method), dead...)
}

// ask drives one Discover stream: request first, then a verdict on each
// candidate as it comes, dead reported in order and the stream closed once
// they are spent.
func ask(
	ctx context.Context,
	client grpcdconnect.GRPCDServiceClient,
	request *grpcd.DiscoverRequest,
	dead ...string,
) ([]string, error) {
	stream, err := client.Discover(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	if err := stream.Send(request); err != nil {
		return nil, err
	}

	var candidates []string

	for {
		candidate, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return candidates, nil
		}

		if err != nil {
			return candidates, err
		}

		candidates = append(candidates, candidate.GetAddress())

		if len(dead) == 0 {
			if err := stream.CloseSend(); err != nil {
				return candidates, err
			}

			continue
		}

		if err := stream.Send(reporting(dead[0])); err != nil {
			return candidates, err
		}

		dead = dead[1:]
	}
}

// seedTwo registers two addresses for method, so a test that reports one dead
// has another to be offered rather than exhausting.
func seedTwo(t *testing.T, store interface {
	Add(context.Context, string, string, []string) error
}) {
	t.Helper()

	for _, address := range []string{"10.0.0.1:50054", "10.0.0.2:50054"} {
		if err := store.Add(t.Context(), address, testAnchor, []string{method}); err != nil {
			t.Fatalf("failed to seed: %v", err)
		}
	}
}

// isValidMethodName checks if a method name is valid according to validation rules
// This must match the validation logic in internal/validate/common.go
// Expects gRPC format: /package.Service/Method
func isValidMethodName(name string) bool {
	if name == "" {
		return false
	}

	// Check for any whitespace
	if strings.ContainsAny(name, " \t\n\r") {
		return false
	}

	// Must start with /
	if !strings.HasPrefix(name, "/") {
		return false
	}

	// Must contain at least two slashes (leading + method separator)
	if strings.Count(name, "/") < 2 {
		return false
	}

	// Cannot contain consecutive slashes
	if strings.Contains(name, "//") {
		return false
	}

	// Cannot end with slash
	if strings.HasSuffix(name, "/") {
		return false
	}

	return true
}
