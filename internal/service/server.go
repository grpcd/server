//revive:disable:package-comments
package service

import (
	"log/slog"
	"math/rand/v2"
	"sync"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/protos/grpcdconnect"

	"github.com/grpcd/server/internal/storage"
	"github.com/grpcd/server/internal/storage/mock"
)

const (
	// ErrCodePeerInfoUnavailable denotes an error when the peer info is unavailable
	ErrCodePeerInfoUnavailable = "PEER_INFO_UNAVAILABLE"

	// tracerName is this package's instrumentation scope.
	tracerName = "grpcd/server/internal/service"
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
func NewGRPCDServer(log *slog.Logger, store storage.Store, anchor string) *GRPCDServer {
	if log == nil {
		log = logger.NewNullLogger()
	}

	if store == nil {
		store = mock.NewStore()
	}

	return &GRPCDServer{
		log:    log,
		store:  store,
		anchor: anchor,
		roll:   oneIn,
		held:   map[string]map[chan storage.Removal]struct{}{},
	}
}
