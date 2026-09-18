package validate

import (
	"fmt"

	errors "github.com/pbrpc/connect-errors"
)

// Methods validates a list of method names
// Each method must be fully qualified (e.g., "service.Method" or "package.service.Method")
func Methods(methods []string) []errors.FieldViolation {
	if len(methods) == 0 {
		return []errors.FieldViolation{{
			Field:       "methods",
			Description: "at least one method required",
		}}
	}

	violations := make([]errors.FieldViolation, 0)

	for i, method := range methods {
		fieldName := fmt.Sprintf("methods[%d]", i)
		methodViolations := validateSingleMethod(method, fieldName)
		violations = append(violations, methodViolations...)
	}

	return violations
}
