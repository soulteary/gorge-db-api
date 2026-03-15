package compat

import "fmt"

type ErrorCode string

const (
	ErrConnectionLost     ErrorCode = "ERR_CONNECTION_LOST"
	ErrDeadlock           ErrorCode = "ERR_DEADLOCK"
	ErrLockTimeout        ErrorCode = "ERR_LOCK_TIMEOUT"
	ErrDuplicateKey       ErrorCode = "ERR_DUPLICATE_KEY"
	ErrAccessDenied       ErrorCode = "ERR_ACCESS_DENIED"
	ErrInvalidCredentials ErrorCode = "ERR_INVALID_CREDENTIALS"
	ErrSchema             ErrorCode = "ERR_SCHEMA"
	ErrConnection         ErrorCode = "ERR_CONNECTION"
	ErrQuery              ErrorCode = "ERR_QUERY"
	ErrReadonlyWrite      ErrorCode = "ERR_READONLY_WRITE"
	ErrMasterUnreachable  ErrorCode = "ERR_MASTER_UNREACHABLE"
	ErrAllUnreachable     ErrorCode = "ERR_ALL_UNREACHABLE"
	ErrUnconfigured       ErrorCode = "ERR_UNCONFIGURED"
)

type DBError struct {
	Code       ErrorCode `json:"code"`
	Message    string    `json:"message"`
	MySQLErrno uint16    `json:"mysql_errno,omitempty"`
	Details    any       `json:"details,omitempty"`
}

func (e *DBError) Error() string {
	if e.MySQLErrno > 0 {
		return fmt.Sprintf("[%s] #%d: %s", e.Code, e.MySQLErrno, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// mysqlErrnoMap maps MySQL error numbers to our ErrorCode values.
// Matches Phorge AphrontBaseMySQLDatabaseConnection::throwCommonException.
var mysqlErrnoMap = map[uint16]ErrorCode{
	2013: ErrConnectionLost,
	2006: ErrConnectionLost,
	1213: ErrDeadlock,
	1205: ErrLockTimeout,
	1062: ErrDuplicateKey,
	1044: ErrAccessDenied,
	1142: ErrAccessDenied,
	1143: ErrAccessDenied,
	1227: ErrAccessDenied,
	1045: ErrInvalidCredentials,
	1146: ErrSchema,
	1049: ErrSchema,
	1054: ErrSchema,
}

func FromMySQLError(errno uint16, message string) *DBError {
	code, ok := mysqlErrnoMap[errno]
	if !ok {
		code = ErrQuery
	}
	return &DBError{Code: code, Message: message, MySQLErrno: errno}
}

func NewClusterError(code ErrorCode, message string) *DBError {
	return &DBError{Code: code, Message: message}
}

// IsRetryable returns true if the error represents a condition where
// retrying the operation may succeed (connection lost scenarios).
func (e *DBError) IsRetryable() bool {
	return e.Code == ErrConnectionLost
}

// HTTPStatus returns the appropriate HTTP status code for this error.
func (e *DBError) HTTPStatus() int {
	switch e.Code {
	case ErrReadonlyWrite:
		return 409
	case ErrMasterUnreachable, ErrAllUnreachable:
		return 503
	case ErrAccessDenied, ErrInvalidCredentials:
		return 403
	case ErrSchema, ErrUnconfigured:
		return 500
	case ErrDuplicateKey:
		return 409
	case ErrDeadlock, ErrLockTimeout:
		return 409
	default:
		return 500
	}
}
