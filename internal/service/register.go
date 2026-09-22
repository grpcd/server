package service

import (
	"context"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"

	"connectrpc.com/connect/v2"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	grpcd "github.com/grpcd/protos"
	"github.com/grpcd/protos/grpcdconnect"
	errors "github.com/pbrpc/connect-errors"

	"github.com/grpcd/server/internal"
	"github.com/grpcd/server/internal/storage"
	"github.com/grpcd/server/internal/validate"
)

const (
	errCodeRegistrationFailed = "REGISTRATION_FAILED"
)

// validateRegisterRequest validates a RegisterRequest
func validateRegisterRequest(req *grpcd.RegisterRequest) []errors.FieldViolation {
	violations := validate.Methods(req.Methods)

	if req.Port == 0 || req.Port > 65535 {
		violations = append(violations, errors.FieldViolation{
			Field:       "port",
			Description: "port must be between 1 and 65535",
		})
	}

	return violations
}

// Register records the caller's methods and holds the stream open.
//
// The stream is the registration: the rows exist while it is held, and this
// handler removes them on its way out. A caller that crashes ends the stream
// the same way a caller that exits cleanly does, so both are the same path.
func (s *GRPCDServer) Register(
	ctx context.Context,
	req *grpcd.RegisterRequest,
	stream grpcdconnect.GRPCDServiceRegisterServerStream,
) error {
	log := logger.FromContext(ctx).With("server_name", req.ServerName)

	violations := validateRegisterRequest(req)
	if len(violations) > 0 {
		return errors.InvalidArgument(ctx, "validation failed", violations...)
	}

	address, err := s.address(ctx, req.Port)
	if err != nil {
		log.ErrorContext(ctx, "Failed to extract peer info from context")

		return err
	}

	log = log.With("peer_address", address, "method_count", len(req.Methods))

	log.InfoContext(ctx, "Registering service instance")
	log.DebugContext(ctx, "Registering methods", "methods", req.Methods)

	// Held before the rows exist, so a removal of any of them reaches this
	// handler; let go after they are released, so one arriving in between is
	// dropped with the stream that would have answered it.
	removals := s.hold(address)
	defer s.unhold(address, removals)

	condition, err := s.register(ctx, log, stream, address, req.Methods)
	if err != nil {
		return err
	}

	log.InfoContext(ctx, "Successfully registered service instance")

	// Holding the stream is the registration. Returning ends it, so this waits
	// for the caller to go away. The rows are written again from the request
	// this handler still holds whenever they may be gone: the store coming
	// back may have come back empty, and a client that could not reach the
	// address has had it removed. The open stream is proof the service is up,
	// so the registration is made again as it was; a row the request never
	// named, left by an earlier occupant of the address, is not among it.
	for {
		select {
		case <-ctx.Done():
			s.release(ctx, log, address, req.Methods)

			return nil
		case <-condition.Changed:
			if condition = s.store.Condition(); condition.Lost {
				continue
			}

			s.rewrite(
				ctx,
				log,
				address,
				req.Methods,
				"Rewrote methods after the store came back")

		case removal := <-removals:
			s.revert(ctx, log.With("method_name", removal.Method), address, req.Methods)
		}
	}
}

// register writes the rows and acknowledges them, under a span of its own:
// the registration is the bounded part of the stream, and the hold that
// follows lasts as long as the caller does. A failure to acknowledge releases
// what was written, since the caller never learns it registered.
func (s *GRPCDServer) register(
	ctx context.Context,
	log *slog.Logger,
	stream grpcdconnect.GRPCDServiceRegisterServerStream,
	address string,
	methods []string,
) (*storage.Condition, error) {
	ctxSpan := trace.SpanFromContext(ctx)
	tracer := ctxSpan.TracerProvider().Tracer(tracerName)
	spanCtx, span := tracer.Start(ctx, "register")
	defer span.End()

	condition, err := s.write(spanCtx, log, address, methods)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	if err := stream.Send(&grpcd.RegisterResponse{}); err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorContext(spanCtx, "Failed to acknowledge registration", "error", err)
		s.release(spanCtx, log, address, methods)

		return nil, err
	}

	return condition, nil
}

// revert writes the registration again after a removal, under a span that
// fails when the write did not land.
func (s *GRPCDServer) revert(
	ctx context.Context,
	log *slog.Logger,
	address string,
	methods []string,
) {
	ctxSpan := trace.SpanFromContext(ctx)
	tracer := ctxSpan.TracerProvider().Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "revert")
	defer span.End()

	if !s.rewrite(ctx, log, address, methods, "Registered again after a removal") {
		span.SetStatus(codes.Error, "not written")
	}
}

// rewrite writes the registration again, reporting whether it landed. A
// failure is logged and nothing more: a lost store is written again when it
// comes back, and any other failure is the store's to have reported.
func (s *GRPCDServer) rewrite(
	ctx context.Context,
	log *slog.Logger,
	address string,
	methods []string,
	message string,
) bool {
	if err := s.store.Add(ctx, address, s.anchor, methods); err != nil {
		log.ErrorContext(ctx, "Failed to write the registration again", "error", err)

		return false
	}

	log.InfoContext(ctx, message)

	return true
}

// hold records the handler holding a stream at address and answers with the
// channel a removal at that address is handed on. One pending removal is
// enough: the registration is written whole either way.
func (s *GRPCDServer) hold(address string) chan storage.Removal {
	removals := make(chan storage.Removal, 1)

	s.heldMu.Lock()
	defer s.heldMu.Unlock()

	if s.held[address] == nil {
		s.held[address] = map[chan storage.Removal]struct{}{}
	}

	s.held[address][removals] = struct{}{}

	return removals
}

// unhold forgets the handler that held removals at address.
func (s *GRPCDServer) unhold(address string, removals chan storage.Removal) {
	s.heldMu.Lock()
	defer s.heldMu.Unlock()

	delete(s.held[address], removals)

	if len(s.held[address]) == 0 {
		delete(s.held, address)
	}
}

// holders answers with the channels of the handlers holding a stream at
// address.
func (s *GRPCDServer) holders(address string) []chan storage.Removal {
	s.heldMu.Lock()
	defer s.heldMu.Unlock()

	return slices.Collect(maps.Keys(s.held[address]))
}

// write records the rows, waiting out a lost store rather than failing on it.
// It answers with the condition the rows were written under, for the holder
// to watch for the next change.
func (s *GRPCDServer) write(
	ctx context.Context, log *slog.Logger, address string, methods []string,
) (*storage.Condition, error) {
	for {
		// Read before the write, so a loss the write itself records closes
		// this one's Changed and the holder wakes to look.
		condition := s.store.Condition()

		err := s.store.Add(ctx, address, s.anchor, methods)
		if err == nil {
			return condition, nil
		}

		lost, waited := storeLost(ctx, s.store)

		if !lost {
			log.ErrorContext(ctx, "Failed to register methods", "error", err)

			return nil, errors.Internal(
				ctx, "failed to register service",
				errCodeRegistrationFailed, internal.ErrDomain,
			)
		}

		if !waited {
			return nil, ctx.Err()
		}

		log.WarnContext(ctx, "Store returned, registering again", "error", err)
	}
}

// release removes the rows this stream was holding.
//
// The context that ended the stream is already cancelled, so the removal runs
// on one detached from it. Nothing else will run this removal: the rows carry
// no expiry, and this instance is the only one watching this stream.
func (s *GRPCDServer) release(
	ctx context.Context, log *slog.Logger, address string, methods []string,
) {
	ctxSpan := trace.SpanFromContext(ctx)
	tracer := ctxSpan.TracerProvider().Tracer(tracerName)
	ctx, span := tracer.Start(context.WithoutCancel(ctx), "remove")
	defer span.End()

	log.InfoContext(ctx, "Removing service instance")

	if err := s.store.Remove(ctx, address, methods); err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorContext(ctx, "Failed to remove methods", "error", err)

		return
	}

	log.InfoContext(ctx, "Removed service instance")
}

// address composes the caller's address from the IP of the peer the transport
// recorded for the call and the port the caller reported.
//
// Neither half is available on its own: a containerized service does not know
// its reachable IP, and the port on the peer socket is the ephemeral one the
// caller dialed from. A call carrying no peer is one the transport has no
// network address for, and there is nothing to register it under.
func (s *GRPCDServer) address(ctx context.Context, port uint32) (string, error) {
	peerAddr := peerAddress(ctx)
	if peerAddr == "" {
		return "", errors.Internal(
			ctx,
			"failed to extract connection info",
			ErrCodePeerInfoUnavailable,
			internal.ErrDomain,
		)
	}

	host, _, err := net.SplitHostPort(peerAddr)
	if err != nil {
		host = peerAddr
	}

	return net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10)), nil
}

// peerAddress answers with the address the transport recorded as the call's
// peer, empty when it recorded none.
func peerAddress(ctx context.Context) string {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		return info.PeerAddr
	}

	return ""
}
