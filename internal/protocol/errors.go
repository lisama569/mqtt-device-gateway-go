package protocol

import "fmt"

// ErrorCode classifies a protocol-level failure.
type ErrorCode string

// Protocol error classifications (see docs/design.md for the full table).
const (
	ErrMalformedPacket     ErrorCode = "MALFORMED_PACKET"
	ErrOversizedPacket     ErrorCode = "OVERSIZED_PACKET"
	ErrUnsupportedProtocol ErrorCode = "UNSUPPORTED_PROTOCOL"
	ErrUnsupportedVersion  ErrorCode = "UNSUPPORTED_PROTOCOL_VERSION"
	ErrInvalidClientID     ErrorCode = "INVALID_CLIENT_ID"
	ErrInvalidTopic        ErrorCode = "INVALID_TOPIC"
	ErrInvalidSubscription ErrorCode = "INVALID_SUBSCRIPTION"
	ErrQoSNotSupported     ErrorCode = "QOS_NOT_SUPPORTED"
	ErrPacketIDInUse       ErrorCode = "PACKET_ID_IN_USE"
	ErrSessionTakenOver    ErrorCode = "SESSION_TAKEN_OVER"
	ErrKeepAliveTimeout    ErrorCode = "KEEPALIVE_TIMEOUT"
	ErrConnectionClosed    ErrorCode = "CONNECTION_CLOSED"
)

// Error is a classified protocol error.
type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError builds a classified protocol error.
func NewError(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
