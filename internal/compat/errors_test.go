package compat

import (
	"strings"
	"testing"
)

func TestFromMySQLError(t *testing.T) {
	cases := []struct {
		errno uint16
		want  ErrorCode
	}{
		{2013, ErrConnectionLost},
		{2006, ErrConnectionLost},
		{1213, ErrDeadlock},
		{1205, ErrLockTimeout},
		{1062, ErrDuplicateKey},
		{1044, ErrAccessDenied},
		{1142, ErrAccessDenied},
		{1143, ErrAccessDenied},
		{1227, ErrAccessDenied},
		{1045, ErrInvalidCredentials},
		{1146, ErrSchema},
		{1049, ErrSchema},
		{1054, ErrSchema},
		{9999, ErrQuery},
	}
	for _, tc := range cases {
		e := FromMySQLError(tc.errno, "test")
		if e.Code != tc.want {
			t.Errorf("FromMySQLError(%d): got %s, want %s", tc.errno, e.Code, tc.want)
		}
		if e.MySQLErrno != tc.errno {
			t.Errorf("FromMySQLError(%d): errno got %d", tc.errno, e.MySQLErrno)
		}
		if e.Message != "test" {
			t.Errorf("FromMySQLError(%d): message got %q", tc.errno, e.Message)
		}
	}
}

func TestFromMySQLErrorUnmappedFallsToErrQuery(t *testing.T) {
	unmapped := []uint16{0, 1, 100, 5000, 65535}
	for _, errno := range unmapped {
		e := FromMySQLError(errno, "msg")
		if e.Code != ErrQuery {
			t.Errorf("FromMySQLError(%d) should be ErrQuery, got %s", errno, e.Code)
		}
	}
}

func TestHTTPStatus(t *testing.T) {
	cases := []struct {
		code ErrorCode
		want int
	}{
		{ErrReadonlyWrite, 409},
		{ErrMasterUnreachable, 503},
		{ErrAllUnreachable, 503},
		{ErrAccessDenied, 403},
		{ErrInvalidCredentials, 403},
		{ErrDeadlock, 409},
		{ErrLockTimeout, 409},
		{ErrDuplicateKey, 409},
		{ErrSchema, 500},
		{ErrUnconfigured, 500},
		{ErrQuery, 500},
		{ErrConnectionLost, 500},
		{ErrConnection, 500},
	}
	for _, tc := range cases {
		e := &DBError{Code: tc.code}
		if got := e.HTTPStatus(); got != tc.want {
			t.Errorf("HTTPStatus(%s) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	retryable := []ErrorCode{ErrConnectionLost}
	for _, code := range retryable {
		e := &DBError{Code: code}
		if !e.IsRetryable() {
			t.Errorf("%s should be retryable", code)
		}
	}

	notRetryable := []ErrorCode{
		ErrDeadlock, ErrLockTimeout, ErrDuplicateKey, ErrAccessDenied,
		ErrInvalidCredentials, ErrSchema, ErrConnection, ErrQuery,
		ErrReadonlyWrite, ErrMasterUnreachable, ErrAllUnreachable, ErrUnconfigured,
	}
	for _, code := range notRetryable {
		e := &DBError{Code: code}
		if e.IsRetryable() {
			t.Errorf("%s should NOT be retryable", code)
		}
	}
}

func TestDBErrorErrorWithMySQLErrno(t *testing.T) {
	e := &DBError{Code: ErrDuplicateKey, Message: "dup key", MySQLErrno: 1062}
	s := e.Error()
	if !strings.Contains(s, "[ERR_DUPLICATE_KEY]") {
		t.Errorf("error string should contain code, got %q", s)
	}
	if !strings.Contains(s, "#1062") {
		t.Errorf("error string should contain #errno, got %q", s)
	}
	if !strings.Contains(s, "dup key") {
		t.Errorf("error string should contain message, got %q", s)
	}
}

func TestDBErrorErrorWithoutMySQLErrno(t *testing.T) {
	e := &DBError{Code: ErrReadonlyWrite, Message: "read only"}
	s := e.Error()
	if !strings.Contains(s, "[ERR_READONLY_WRITE]") {
		t.Errorf("error string should contain code, got %q", s)
	}
	if strings.Contains(s, "#") {
		t.Errorf("error string should NOT contain # when no errno, got %q", s)
	}
	if !strings.Contains(s, "read only") {
		t.Errorf("error string should contain message, got %q", s)
	}
}

func TestNewClusterError(t *testing.T) {
	e := NewClusterError(ErrMasterUnreachable, "master down")
	if e.Code != ErrMasterUnreachable {
		t.Errorf("expected code ErrMasterUnreachable, got %s", e.Code)
	}
	if e.Message != "master down" {
		t.Errorf("expected message 'master down', got %q", e.Message)
	}
	if e.MySQLErrno != 0 {
		t.Errorf("cluster error should have 0 errno, got %d", e.MySQLErrno)
	}
}

func TestDBErrorImplementsErrorInterface(t *testing.T) {
	var err error = &DBError{Code: ErrQuery, Message: "test"}
	if err.Error() == "" {
		t.Error("Error() should return non-empty string")
	}
}

func TestErrorCodeConstants(t *testing.T) {
	codes := map[ErrorCode]string{
		ErrConnectionLost:     "ERR_CONNECTION_LOST",
		ErrDeadlock:           "ERR_DEADLOCK",
		ErrLockTimeout:        "ERR_LOCK_TIMEOUT",
		ErrDuplicateKey:       "ERR_DUPLICATE_KEY",
		ErrAccessDenied:       "ERR_ACCESS_DENIED",
		ErrInvalidCredentials: "ERR_INVALID_CREDENTIALS",
		ErrSchema:             "ERR_SCHEMA",
		ErrConnection:         "ERR_CONNECTION",
		ErrQuery:              "ERR_QUERY",
		ErrReadonlyWrite:      "ERR_READONLY_WRITE",
		ErrMasterUnreachable:  "ERR_MASTER_UNREACHABLE",
		ErrAllUnreachable:     "ERR_ALL_UNREACHABLE",
		ErrUnconfigured:       "ERR_UNCONFIGURED",
	}
	for code, want := range codes {
		if string(code) != want {
			t.Errorf("ErrorCode %v != %q", code, want)
		}
	}
}
