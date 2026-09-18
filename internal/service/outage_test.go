package service

import (
	"context"
	"testing"

	"github.com/pbrpc/connect-service/health"

	"github.com/grpcd/protos/grpcdconnect"
)

func TestReportHealth(t *testing.T) {
	t.Run("keeps the service's status matching the store", func(t *testing.T) {
		server, store := newServer()
		healthSrv := health.NewServer()

		ctx, stop := context.WithCancel(t.Context())
		defer stop()

		// Each condition is reported once the reporter has acted on it.
		reported := store.Conditioned()

		go server.ReportHealth(ctx, healthSrv)

		await(t, reported, "the current condition was never reported")
		assertStatus(t, healthSrv, health.StatusServing)

		reported = store.Conditioned()
		store.Lose()

		await(t, reported, "the loss was never reported")
		assertStatus(t, healthSrv, health.StatusNotServing)

		reported = store.Conditioned()
		store.Recover()

		await(t, reported, "the recovery was never reported")
		assertStatus(t, healthSrv, health.StatusServing)
	})
}

// assertStatus fails the test unless the grpcd service's status is want.
func assertStatus(t *testing.T, healthSrv *health.Server, want health.Status) {
	t.Helper()

	got, found := healthSrv.Status(grpcdconnect.GRPCDServiceName)
	if !found || got != want {
		t.Fatalf("status = %v (found %v), want %v", got, found, want)
	}
}
