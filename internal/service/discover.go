package service

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	grpcd "github.com/grpcd/protos"
	"github.com/grpcd/protos/grpcdconnect"
	pbrpcerrors "github.com/pbrpc/connect-errors"

	"github.com/grpcd/server/internal"
	"github.com/grpcd/server/internal/storage"
	"github.com/grpcd/server/internal/validate"
)

const (
	errCodeDiscoverFailed = "DISCOVER_FAILED"
)

// Discover answers with the addresses serving a method, one at a time, each
// drawn at random from the method's set.
//
// The caller takes the first it can reach and closes the stream. One it cannot
// reach it reports back, and that address is removed before the next is drawn,
// so the set converges on what is actually reachable without grpcd checking
// anything itself. The draws end when the set is empty; a caller that keeps
// refusing without removing is drawn the same addresses again.
//
// When there is nothing left to offer, the stream is held until something
// registers, and the set is drawn from again. A caller whose backend is
// entirely down blocks on a receive rather than asking again, and is woken by
// the registration. Every registration wakes every waiting handler, whatever
// method it was for; one that finds its own set still empty goes back to sleep.
// A caller that asked with no_wait is answered NotFound at that point instead:
// it is resolving one request and has nothing to wait for.
func (s *GRPCDServer) Discover(
	ctx context.Context,
	stream grpcdconnect.GRPCDServiceDiscoverServerStream,
) error {
	log := logger.FromContext(ctx).With("peer_address", peerAddress(ctx))

	method, noWait, err := methodName(ctx, stream)
	if err != nil {
		return err
	}

	log = log.With("method_name", method)
	log.DebugContext(ctx, "Discovering method")

	sent := 0

	for {
		// Both loaded before the draw, so a registration or a recovery landing
		// after the draw finds nothing closes what this waits on.
		latest := s.store.Latest()
		condition := s.store.Condition()

		done, storeErr, err := s.draw(ctx, log, stream, method, &sent)
		if done || err != nil {
			return err
		}

		if storeErr != nil {
			lost, waited := storeLost(ctx, s.store)

			if !lost {
				log.ErrorContext(ctx, "Failed to discover method", "error", storeErr)

				return pbrpcerrors.Internal(
					ctx, "failed to discover method",
					errCodeDiscoverFailed, internal.ErrDomain,
				)
			}

			if !waited {
				return ctx.Err()
			}

			log.WarnContext(ctx, "Store returned, drawing again", "error", storeErr)

			continue
		}

		if noWait {
			log.InfoContext(ctx, "No addresses for method, not waiting",
				"candidates_sent", sent)

			return pbrpcerrors.NotFound(ctx, "method", method)
		}

		log.DebugContext(ctx, "No addresses for method, waiting",
			"candidates_sent", sent)

		// A recovery wakes this too: the store may have gained addresses this
		// instance was deaf to.
		select {
		case <-latest.Done:
		case <-condition.Changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// draw offers candidates until one works, the set empties, or something
// fails. It answers done once the caller is satisfied. A store failure is
// answered apart from a stream failure, because the caller judges the former
// against the store's condition and returns the latter as is.
func (s *GRPCDServer) draw(
	ctx context.Context,
	log *slog.Logger,
	stream grpcdconnect.GRPCDServiceDiscoverServerStream,
	method string,
	sent *int,
) (done bool, storeErr, err error) {
	for address, err := range s.store.AddressesFor(ctx, method) {
		if err != nil {
			return false, err, nil
		}

		*sent++

		if done, err := s.offer(ctx, log, stream, method, address); done || err != nil {
			return done, nil, err
		}
	}

	return false, nil, nil
}

// offer sends address as a candidate and waits for the caller's verdict. It
// answers done when the caller closed the stream, which is how a caller says
// the candidate worked.
func (s *GRPCDServer) offer(
	ctx context.Context,
	log *slog.Logger,
	stream grpcdconnect.GRPCDServiceDiscoverServerStream,
	method, address string,
) (bool, error) {
	if err := stream.Send(&grpcd.DiscoverResponse{Address: address}); err != nil {
		return false, err
	}

	reported, err := stream.Receive()
	if errors.Is(err, io.EOF) {
		log.InfoContext(ctx, "Discover successful", "method_address", address)

		return true, nil
	}

	if err != nil {
		return false, err
	}

	s.reportedDead(ctx, log, method, reported.GetDeadAddress())

	return false, nil
}

// methodName reads the method off the stream's first message, and whether the
// caller declines to wait for one that nothing serves.
func methodName(
	ctx context.Context, stream grpcdconnect.GRPCDServiceDiscoverServerStream,
) (string, bool, error) {
	request, err := stream.Receive()
	if err != nil {
		return "", false, err
	}

	method := request.GetMethodName()

	if violations := validate.MethodName(method); len(violations) > 0 {
		return "", false, pbrpcerrors.InvalidArgument(ctx, "validation failed",
			violations...)
	}

	return method, request.GetNoWait(), nil
}

// reportedDead removes an address the caller could not reach and tells the
// instance anchoring it, which writes the row back if it still holds that
// address's registration stream.
func (s *GRPCDServer) reportedDead(
	ctx context.Context, log *slog.Logger, method, address string,
) {
	if address == "" {
		return
	}

	log = log.With("method_address", address)

	ctxSpan := trace.SpanFromContext(ctx)
	tracer := ctxSpan.TracerProvider().Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "remove")
	defer span.End()

	anchor, err := s.store.RemoveFromMethod(ctx, method, address)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorContext(ctx, "Failed to remove unreachable address", "error", err)

		return
	}

	log.InfoContext(ctx, "Removed unreachable address")

	if anchor == "" {
		return
	}

	removal := storage.Removal{Method: method, Address: address}

	if err := s.store.Notify(ctx, anchor, removal); err != nil {
		log.ErrorContext(ctx, "Failed to notify the anchoring instance", "error", err)
	}
}

// Reinstate hands a removal from under this instance's anchor to the Register
// handlers holding a stream at the removed address, and each writes its
// registration again. Nothing is written here: what the address serves is
// known only to the stream that registered it.
//
// The store publishes only to the anchor recorded on the row, and the anchor
// is recorded by address, so the row may be one an earlier occupant of the
// address left behind. The handler writes what its request names and nothing
// else, so such a row stays removed. An address no stream holds any more is a
// stream that ended in the meantime, and the removal stands.
func (s *GRPCDServer) Reinstate(ctx context.Context, removal storage.Removal) {
	holders := s.holders(removal.Address)

	if len(holders) == 0 {
		s.log.InfoContext(ctx, "Removed address is held by no stream",
			"peer_address", removal.Address, "method_name", removal.Method)

		return
	}

	for _, holder := range holders {
		// A pending removal already covers this one.
		select {
		case holder <- removal:
		default:
		}
	}
}
