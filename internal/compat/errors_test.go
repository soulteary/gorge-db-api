package compat

import "testing"

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
		{1045, ErrInvalidCredentials},
		{1146, ErrSchema},
		{1049, ErrSchema},
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
		{ErrDeadlock, 409},
		{ErrDuplicateKey, 409},
		{ErrSchema, 500},
		{ErrQuery, 500},
	}
	for _, tc := range cases {
		e := &DBError{Code: tc.code}
		if got := e.HTTPStatus(); got != tc.want {
			t.Errorf("HTTPStatus(%s) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	e1 := &DBError{Code: ErrConnectionLost}
	if !e1.IsRetryable() {
		t.Error("ErrConnectionLost should be retryable")
	}
	e2 := &DBError{Code: ErrDeadlock}
	if e2.IsRetryable() {
		t.Error("ErrDeadlock should not be retryable")
	}
}
