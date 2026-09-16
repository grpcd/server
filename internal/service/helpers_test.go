package service

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"git.sonicoriginal.software/grpc-testing/mocks/addr"
	"git.sonicoriginal.software/grpc-testing/mocks/meter"
	grpcd "github.com/grpcd/protos"

	"github.com/grpcd/server/internal/storage/mock"
)

const testAnchor = "anchor-under-test"

// discovering runs Discover on its own goroutine and answers with a channel
// carrying its return value, because a handler that runs out of candidates
// blocks until a registration arrives or the caller leaves.
func discovering(server *GRPCDServer, stream *discoverStream) <-chan error {
	returned := make(chan error, 1)

	go func() { returned <- server.Discover(stream) }()

	return returned
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

// newServer builds a server on a fresh mock store, which the caller keeps to
// assert on.
func newServer() (*GRPCDServer, *mock.Store) {
	store := mock.NewStore()

	return NewGRPCDServer(slog.New(slog.DiscardHandler), store, meter.New(), testAnchor), store
}

// peerContext puts a connection address on ctx the way gRPC does, so the
// handler composes the same address a real caller would produce.
func peerContext(ctx context.Context, address string) context.Context {
	return peer.NewContext(ctx, &peer.Peer{Addr: addr.New(address)})
}

// registerStream stands in for a held registration stream. Cancelling the
// context it carries is what a caller going away looks like to the handler.
type registerStream struct {
	grpc.ServerStream

	ctx context.Context

	mu       sync.Mutex
	sent     []*grpcd.RegisterResponse
	sendErr  error
	received chan struct{}
}

func newRegisterStream(ctx context.Context) *registerStream {
	return &registerStream{ctx: ctx, received: make(chan struct{}, 1)}
}

func (s *registerStream) Context() context.Context { return s.ctx }

func (s *registerStream) Send(response *grpcd.RegisterResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sendErr != nil {
		return s.sendErr
	}

	s.sent = append(s.sent, response)

	select {
	case s.received <- struct{}{}:
	default:
	}

	return nil
}

// acknowledged blocks until the handler has confirmed the registration, so a
// test acts on a stream that is actually being held.
func (s *registerStream) acknowledged() <-chan struct{} { return s.received }

func (s *registerStream) sends() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.sent)
}

// discoverStream stands in for a client discovering a method. requests are
// replayed in order, and responses records the candidates the handler offered.
type discoverStream struct {
	grpc.ServerStream

	ctx context.Context

	requests []recvResult
	received int

	mu        sync.Mutex
	responses []string
	sendErr   error
}

// recvResult is one message the fake client sends, or the error ending it.
type recvResult struct {
	request *grpcd.DiscoverRequest
	err     error
}

// asks builds a stream that requests method and then reports each of dead as
// unreachable, ending once they are exhausted.
func asks(ctx context.Context, method string, dead ...string) *discoverStream {
	requests := []recvResult{{
		request: &grpcd.DiscoverRequest{
			Step: &grpcd.DiscoverRequest_MethodName{MethodName: method},
		},
	}}

	for _, address := range dead {
		requests = append(requests, recvResult{
			request: &grpcd.DiscoverRequest{
				Step: &grpcd.DiscoverRequest_DeadAddress{DeadAddress: address},
			},
		})
	}

	return &discoverStream{ctx: ctx, requests: requests}
}

// satisfiedAfter builds a stream that requests method, reports each of dead,
// and then closes — which is how a caller says the last candidate worked.
func satisfiedAfter(ctx context.Context, method string, dead ...string) *discoverStream {
	stream := asks(ctx, method, dead...)
	stream.requests = append(stream.requests, recvResult{err: io.EOF})

	return stream
}

// asksWithoutWaiting builds a stream that requests method with no_wait set
// and then reports each of dead as unreachable.
func asksWithoutWaiting(ctx context.Context, method string, dead ...string) *discoverStream {
	stream := asks(ctx, method, dead...)
	stream.requests[0].request.NoWait = true

	return stream
}

func (s *discoverStream) Context() context.Context { return s.ctx }

func (s *discoverStream) Recv() (*grpcd.DiscoverRequest, error) {
	if s.received >= len(s.requests) {
		return nil, io.EOF
	}

	result := s.requests[s.received]
	s.received++

	return result.request, result.err
}

func (s *discoverStream) Send(response *grpcd.DiscoverResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sendErr != nil {
		return s.sendErr
	}

	s.responses = append(s.responses, response.Address)

	return nil
}

func (s *discoverStream) candidates() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.responses...)
}
