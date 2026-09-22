package service

import (
	"errors"
	"testing"

	"github.com/pbrpc/connect-service/diagnostics"

	"github.com/grpcd/server/internal/storage/mock"
)

// newStorageCheckServer builds a server over a mock store whose Ping fails with
// pingErr, or succeeds when pingErr is nil.
func newStorageCheckServer(pingErr error) *GRPCDServer {
	store := mock.NewStore()
	store.SetPingError(pingErr)

	return New(store, testAnchor)
}

func TestStorageCheck(t *testing.T) {
	t.Run("reports the store reachable when it answers", func(t *testing.T) {
		server := newStorageCheckServer(nil)

		got, err := server.StorageCheck(t.Context())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetState() != diagnostics.StateReachable {
			t.Errorf("state = %q, want %q", got.GetState(), diagnostics.StateReachable)
		}
		if got.GetDetails()["name"] != "mock" {
			t.Errorf("details name = %q, want %q", got.GetDetails()["name"], "mock")
		}
		if _, reported := got.GetDetails()["error"]; reported {
			t.Errorf("details carry an error %q for a store that answered", got.GetDetails()["error"])
		}
		if got.GetLastChecked() == 0 {
			t.Error("last checked was not recorded")
		}
	})

	t.Run("reports the store unreachable with the failure", func(t *testing.T) {
		wantErr := errors.New("storage unreachable")
		server := newStorageCheckServer(wantErr)

		got, err := server.StorageCheck(t.Context())
		if !errors.Is(err, wantErr) {
			t.Fatalf("error = %v, want %v", err, wantErr)
		}
		if got.GetState() != diagnostics.StateUnreachable {
			t.Errorf("state = %q, want %q", got.GetState(), diagnostics.StateUnreachable)
		}
		if got.GetDetails()["error"] != wantErr.Error() {
			t.Errorf("details error = %q, want %q", got.GetDetails()["error"], wantErr.Error())
		}
		if got.GetDetails()["name"] != "mock" {
			t.Errorf("details name = %q, want %q", got.GetDetails()["name"], "mock")
		}
	})
}
