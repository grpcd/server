package service

import (
	"context"
	"strconv"
	"testing"

	"connectrpc.com/connect/v2"
)

// FuzzRegister_MethodNames validates that Register properly handles all possible method
// name inputs, rejecting invalid names with InvalidArgument and accepting valid ones.
// This is critical for ensuring malformed method names cannot be registered.
func FuzzRegister_MethodNames(f *testing.F) {
	// Seed corpus with known valid and invalid cases
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
		h := newHarness()

		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		// Acknowledged or refused; an acknowledged one runs its whole path,
		// write, acknowledge, remove, once the caller leaves.
		_, err := register(ctx, h.client(peer), registration(methodName))

		isValid := isValidMethodName(methodName)

		if isValid {
			// Valid method names should succeed
			if err != nil {
				t.Errorf("expected valid method name %q to succeed, got error: %v", methodName, err)

				return
			}

			disconnect()

			if err := await(t, returned, "handler did not return"); err != nil {
				t.Errorf("expected method name %q to be released cleanly, got error: %v", methodName, err)
			}

			return
		}

		// Invalid method names should fail with InvalidArgument
		if err == nil {
			t.Errorf("expected invalid method name %q to fail, got success", methodName)

			return
		}

		if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
			t.Errorf(
				"expected InvalidArgument for invalid method name %q, got %v",
				methodName, got,
			)
		}
	})
}

// FuzzRegister_MethodCounts validates that Register properly handles varying numbers of
// methods in a single registration request, from zero methods (should fail validation)
// to large batches. This ensures Register scales correctly and validates array size constraints.
func FuzzRegister_MethodCounts(f *testing.F) {
	// Seed corpus with interesting method counts
	f.Add(0)    // Empty array - should fail
	f.Add(1)    // Single method
	f.Add(3)    // Small batch
	f.Add(10)   // Medium batch
	f.Add(100)  // Large batch
	f.Add(1000) // Very large batch

	f.Fuzz(func(t *testing.T, methodCount int) {
		// Skip negative and unreasonably large counts
		if methodCount < 0 || methodCount > 10000 {
			t.Skip()
		}

		h := newHarness()

		// Generate N valid method names in gRPC format
		methods := make([]string, methodCount)
		for i := range methods {
			methods[i] = "/Service/Method" + strconv.Itoa(i)
		}

		// A stream that is still held, so the rows are there to assert on.
		ctx, disconnect := context.WithCancel(t.Context())
		defer disconnect()

		returned := h.registering()

		_, err := register(ctx, h.client(peer), registration(methods...))

		if methodCount == 0 {
			// Should fail validation
			if err == nil {
				t.Fatal("expected error for zero methods, got nil")
			}

			if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
				t.Errorf("expected InvalidArgument for zero methods, got %v", got)
			}

			return
		}

		if err != nil {
			t.Fatalf("registration was never acknowledged: %v", err)
		}

		// Verify all methods were registered against the composed address
		for _, method := range methods {
			addresses := h.store.Addresses(method)

			if len(addresses) != 1 || addresses[0] != "192.168.1.100:50054" {
				t.Errorf("method %s holds %v", method, addresses)

				break
			}
		}

		disconnect()

		if err := await(t, returned, "handler did not return"); err != nil {
			t.Errorf("expected success for %d methods, got error: %v", methodCount, err)
		}
	})
}
