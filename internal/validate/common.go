//revive:disable:package-comments
package validate

import (
	"strings"

	errors "github.com/pbrpc/connect-errors"
)

// validateSingleMethod performs the core validation logic for a single method name
// fieldName is used for the violation field (e.g., "method_name" or "methods[0]")
// Expects gRPC format: /package.Service/Method
func validateSingleMethod(methodName, fieldName string) []errors.FieldViolation {
	violations := make([]errors.FieldViolation, 0, 5)

	// Check for empty
	if methodName == "" {
		return []errors.FieldViolation{{
			Field:       fieldName,
			Description: "method name cannot be empty",
		}}
	}

	// Check for any whitespace
	if strings.ContainsAny(methodName, " \t\n\r") {
		violations = append(violations, errors.FieldViolation{
			Field:       fieldName,
			Description: "method name cannot contain whitespace",
		})
	}

	// Must start with /
	if !strings.HasPrefix(methodName, "/") {
		violations = append(violations, errors.FieldViolation{
			Field:       fieldName,
			Description: "method must be in gRPC format starting with '/' (e.g., '/package.Service/Method')",
		})
	}

	// Must contain at least one slash after the leading slash (to separate service from method)
	if strings.Count(methodName, "/") < 2 {
		violations = append(violations, errors.FieldViolation{
			Field:       fieldName,
			Description: "method must include service and method separated by '/' (e.g., '/package.Service/Method')",
		})
	}

	// Check for consecutive slashes
	if strings.Contains(methodName, "//") {
		violations = append(violations, errors.FieldViolation{
			Field:       fieldName,
			Description: "method name cannot contain consecutive slashes",
		})
	}

	// Check for trailing slash
	if strings.HasSuffix(methodName, "/") {
		violations = append(violations, errors.FieldViolation{
			Field:       fieldName,
			Description: "method name cannot end with a slash",
		})
	}

	return violations
}
