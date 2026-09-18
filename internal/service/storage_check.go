package service

import (
	"context"
	"time"

	diagpb "github.com/pbrpc/connect-protos/diagnostics"
	"github.com/pbrpc/connect-service/diagnostics"
)

// StorageCheckName is the diagnostics name the storage backend reports under
const StorageCheckName = "storage"

// StorageCheck reports the storage backend as a service dependency: reachable
// when it answers a ping, unreachable otherwise, with the failure under the
// "error" detail. A store has no serving status of its own, so none is
// reported.
//
// The method value satisfies diagnostics.Check, so it is registered with the
// diagnostics service rather than being served by this package.
func (s *GRPCDServer) StorageCheck(
	ctx context.Context,
) (*diagpb.ServiceDependency, error) {
	err := s.store.Ping(ctx)

	state := diagnostics.StateReachable
	details := map[string]string{"name": s.store.Name()}

	if err != nil {
		state = diagnostics.StateUnreachable
		details["error"] = err.Error()
	}

	return &diagpb.ServiceDependency{
		Address:     s.store.Address(),
		State:       state,
		Serving:     "",
		LastChecked: time.Now().Unix(),
		Details:     details,
	}, err
}
