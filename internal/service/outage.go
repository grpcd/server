package service

import (
	"context"

	"github.com/pbrpc/connect-service/health"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/protos/grpcdconnect"

	"github.com/grpcd/server/internal/storage"
)

// storeLost reports whether a failed operation is the store being lost, and if
// so blocks until the store's condition next changes, answering false if ctx
// ends first.
//
// The condition is read after the failure, because the failure is what
// records the loss: a condition read before it still says healthy. The
// condition read here is the lost one, so a recovery landing after it is
// read closes the channel waited on and is not slept through.
func storeLost(ctx context.Context, store storage.Store) (lost, waited bool) {
	condition := store.Condition()

	if !condition.Lost {
		return false, false
	}

	select {
	case <-condition.Changed:
		return true, true
	case <-ctx.Done():
		return true, false
	}
}

// ReportHealth keeps the grpcd service's entry on healthSrv matching the
// store's reachability, for as long as ctx lives, and logs each change. The
// service serves exactly while its store can be reached.
func (s *GRPCDServer) ReportHealth(ctx context.Context, healthSrv *health.Server) {
	lost := false

	for condition := range s.store.Changes(ctx) {
		status := health.StatusServing
		if condition.Lost {
			status = health.StatusNotServing
		}

		healthSrv.SetServingStatus(grpcdconnect.GRPCDServiceName, status)

		switch {
		case condition.Lost && !lost:
			logger.FromContext(ctx).ErrorContext(ctx, "Store lost")
		case !condition.Lost && lost:
			logger.FromContext(ctx).InfoContext(ctx, "Store returned")
		}

		lost = condition.Lost
	}
}
