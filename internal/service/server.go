//revive:disable:package-comments
package service

import (
	"log/slog"
	"math/rand/v2"
	"sync"

	"go.opentelemetry.io/otel/metric"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/protos/grpcdconnect"

	"github.com/grpcd/server/internal/storage"
	"github.com/grpcd/server/internal/storage/mock"
)

const (
	// ErrCodePeerInfoUnavailable denotes an error when the peer info is unavailable
	ErrCodePeerInfoUnavailable = "PEER_INFO_UNAVAILABLE"
)

// GRPCDServer implements the GRPCDService
type GRPCDServer struct {
	log    *slog.Logger
	store  storage.Store
	anchor string

	// roll decides whether one holder among n is told to move. Uniform at
	// random in production; a test substitutes a deterministic answer.
	roll func(n int64) bool

	// held is every Register handler holding a stream, by the address it
	// registered: the channel a removal at that address is handed on. What was
	// registered is not kept here; the handler holds its own request.
	heldMu sync.Mutex
	held   map[string]map[chan storage.Removal]struct{}

	// Business metrics
	registrationCount metric.Int64Counter
	removalCount      metric.Int64Counter
	methodsDiscovered metric.Int64Counter
	revertedRemovals  metric.Int64Counter
	rebalanceCount    metric.Int64Counter
}

var _ grpcdconnect.GRPCDServiceHandler = (*GRPCDServer)(nil)

// oneIn answers true with probability 1/n. A count of zero means the set
// emptied between the announcement and the count, and nobody moves.
func oneIn(n int64) bool {
	return n > 0 && rand.IntN(int(n)) == 0
}

// NewGRPCDServer creates a new grpcd server.
//
// anchor identifies this instance for the life of the process. It is recorded
// on every address this server registers, so whoever removes one of those rows
// knows which instance to tell.
func NewGRPCDServer(
	log *slog.Logger, store storage.Store, meter metric.Meter, anchor string,
) *GRPCDServer {
	if log == nil {
		log = logger.NewNullLogger()
	}

	if store == nil {
		store = mock.NewStore()
	}

	// Initialize business metrics
	registrationCount, err := meter.Int64Counter(
		"grpcd.registrations.total",
		metric.WithDescription("Total number of service registrations"),
		metric.WithUnit("{registration}"),
	)
	if err != nil {
		log.Error("Failed to create registrations metric", "error", err)
	}

	removalCount, err := meter.Int64Counter(
		"grpcd.removals.total",
		metric.WithDescription("Total number of address removals"),
		metric.WithUnit("{removal}"),
	)
	if err != nil {
		log.Error("Failed to create removals metric", "error", err)
	}

	methodsDiscovered, err := meter.Int64Counter(
		"grpcd.discoveries.total",
		metric.WithDescription("Total number of method discoveries"),
		metric.WithUnit("{grpcd}"),
	)
	if err != nil {
		log.Error("Failed to create discoveries metric", "error", err)
	}

	revertedRemovals, err := meter.Int64Counter(
		"grpcd.removals.reverted.total",
		metric.WithDescription(
			"Total number of removals written back because this instance still holds the stream",
		),
		metric.WithUnit("{removal}"),
	)
	if err != nil {
		log.Error("Failed to create reverted removals metric", "error", err)
	}

	rebalanceCount, err := meter.Int64Counter(
		"grpcd.rebalances.total",
		metric.WithDescription("Total number of clients told to move to a newly registered address"),
		metric.WithUnit("{move}"),
	)
	if err != nil {
		log.Error("Failed to create rebalances metric", "error", err)
	}

	return &GRPCDServer{
		log:               log,
		store:             store,
		anchor:            anchor,
		roll:              oneIn,
		held:              map[string]map[chan storage.Removal]struct{}{},
		registrationCount: registrationCount,
		removalCount:      removalCount,
		methodsDiscovered: methodsDiscovered,
		revertedRemovals:  revertedRemovals,
		rebalanceCount:    rebalanceCount,
	}
}
