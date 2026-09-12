package protocol

// ErrorCode is a stable machine-readable category. Clients should branch on Code,
// not on the human-readable Message. HTTP handlers choose the status separately.
type ErrorCode string

const (
	CodeInvalidRequest   ErrorCode = "invalid_request"
	CodeRequestTooLarge  ErrorCode = "request_too_large"
	CodeNotFound         ErrorCode = "not_found"
	CodeMethodNotAllowed ErrorCode = "method_not_allowed"
	CodeConflict         ErrorCode = "conflict"
	CodeUnavailable      ErrorCode = "unavailable"
	CodeJobFailed        ErrorCode = "job_failed"
	CodeInternal         ErrorCode = "internal_error"
)

// ErrorResponse is the shared error body for all /v1 endpoints.
// It carries no Go error value or scheduler state.
type ErrorResponse struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}
