package validate

import errors "github.com/pbrpc/connect-errors"

// MethodName validates a single method name
// Must be in gRPC format (e.g., "/package.Service/Method")
func MethodName(methodName string) []errors.FieldViolation {
	return validateSingleMethod(methodName, "method_name")
}
