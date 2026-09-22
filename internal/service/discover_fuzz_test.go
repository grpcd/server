package service

import (
	"testing"

	"connectrpc.com/connect/v2"
)

// FuzzDiscover_MethodNames validates that Discover properly handles all possible method
// name inputs. This is THE most critical fuzz test for the grpcd service - if Discover
// fails or returns incorrect results, the entire service mesh breaks down. Every method
// name variation must be tested to ensure only valid, registered methods return addresses
// and invalid names return appropriate errors.
func FuzzDiscover_MethodNames(f *testing.F) {
	// Seed corpus with valid and invalid cases
	f.Add("/Service/Method")
	f.Add("/package.service.Service/Method")
	f.Add("/a.b.c.d.Service/Method")
	f.Add("")
	f.Add("NoQualification")
	f.Add("/Service//Method")
	f.Add("Service/Method")
	f.Add("/Service/Method/")
	f.Add(" /Service/Method")
	f.Add("/Service/Method ")
	f.Add("/Service/ Method")
	f.Add("/Service")

	f.Fuzz(func(t *testing.T, methodName string) {
		h := newHarness(t)

		isValid := isValidMethodName(methodName)

		const expectedAddr = "192.168.1.100:50054"

		if isValid {
			if err := h.store.Add(t.Context(), expectedAddr, testAnchor, []string{methodName}); err != nil {
				t.Fatalf("failed to seed valid method: %v", err)
			}
		}

		// The caller takes the first candidate it is offered and closes.
		candidates, err := discover(t.Context(), h.client(""), methodName)

		if !isValid {
			// Invalid method names should return InvalidArgument
			if err == nil {
				t.Errorf("expected error for invalid method name %q, got success", methodName)

				return
			}

			if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
				t.Errorf(
					"expected InvalidArgument for invalid method name %q, got %v",
					methodName, got,
				)
			}

			return
		}

		// Valid method names should offer the registered address
		if err != nil {
			t.Errorf("expected success for valid method name %q, got error: %v", methodName, err)

			return
		}

		if len(candidates) != 1 {
			t.Errorf("expected one candidate for method %q, got %v", methodName, candidates)

			return
		}

		if candidates[0] != expectedAddr {
			t.Errorf(
				"expected address %s for method %q, got %s",
				expectedAddr, methodName, candidates[0],
			)
		}
	})
}
