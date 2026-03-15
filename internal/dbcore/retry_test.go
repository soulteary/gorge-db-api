package dbcore

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxAttempts != 3 {
		t.Errorf("expected MaxAttempts=3, got %d", p.MaxAttempts)
	}
}

func TestRetryPolicyZeroValue(t *testing.T) {
	p := RetryPolicy{}
	if p.MaxAttempts != 0 {
		t.Errorf("zero value MaxAttempts should be 0, got %d", p.MaxAttempts)
	}
}

func TestIsMySQLRetryableWithRetryableCode(t *testing.T) {
	err := &mysqldriver.MySQLError{Number: 2002, Message: "Connection Timeout"}
	errno, retryable := isMySQLRetryable(err, retryableConnectCodes)
	if !retryable {
		t.Error("errno 2002 should be retryable for connect")
	}
	if errno != 2002 {
		t.Errorf("expected errno 2002, got %d", errno)
	}
}

func TestIsMySQLRetryableWithNonRetryableCode(t *testing.T) {
	err := &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate key"}
	errno, retryable := isMySQLRetryable(err, retryableConnectCodes)
	if retryable {
		t.Error("errno 1062 should not be retryable for connect")
	}
	if errno != 1062 {
		t.Errorf("expected errno 1062, got %d", errno)
	}
}

func TestIsMySQLRetryableWithNonMySQLError(t *testing.T) {
	err := errors.New("generic error")
	errno, retryable := isMySQLRetryable(err, retryableConnectCodes)
	if retryable {
		t.Error("non-MySQL error should not be retryable")
	}
	if errno != 0 {
		t.Errorf("expected errno 0 for non-MySQL error, got %d", errno)
	}
}

func TestIsMySQLRetryableQueryCodes(t *testing.T) {
	cases := []struct {
		number    uint16
		retryable bool
	}{
		{2013, true},
		{2006, true},
		{1062, false},
		{1045, false},
	}
	for _, tc := range cases {
		err := &mysqldriver.MySQLError{Number: tc.number}
		_, got := isMySQLRetryable(err, retryableQueryCodes)
		if got != tc.retryable {
			t.Errorf("isMySQLRetryable(errno=%d, queryCodes) = %v, want %v", tc.number, got, tc.retryable)
		}
	}
}

func TestRetryableConnectCodesContent(t *testing.T) {
	expected := []uint16{2002, 2003}
	for _, code := range expected {
		if !retryableConnectCodes[code] {
			t.Errorf("expected code %d in retryableConnectCodes", code)
		}
	}
}

func TestRetryableQueryCodesContent(t *testing.T) {
	expected := []uint16{2013, 2006}
	for _, code := range expected {
		if !retryableQueryCodes[code] {
			t.Errorf("expected code %d in retryableQueryCodes", code)
		}
	}
}

func TestQueryWithRetryWriteQueryNoRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn := NewConnFromDB(db, DSN{}, false)
	mock.ExpectQuery("INSERT INTO foo").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	rows, qErr := QueryWithRetry(context.Background(), conn, nil, DefaultRetryPolicy(), "INSERT INTO foo VALUES (1)")
	if qErr != nil {
		t.Fatalf("unexpected error: %v", qErr)
	}
	_ = rows.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestQueryWithRetryInsideTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectCommit()

	conn := NewConnFromDB(db, DSN{}, false)
	txm := NewTxManager(conn)
	ctx := context.Background()
	_ = txm.Begin(ctx)

	rows, qErr := QueryWithRetry(ctx, conn, txm, DefaultRetryPolicy(), "SELECT 1")
	if qErr != nil {
		t.Fatalf("unexpected error: %v", qErr)
	}
	_ = rows.Close()
	_ = txm.Commit(ctx)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestQueryWithRetryReadSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("ok"))

	conn := NewConnFromDB(db, DSN{}, false)
	rows, qErr := QueryWithRetry(context.Background(), conn, nil, DefaultRetryPolicy(), "SELECT 1")
	if qErr != nil {
		t.Fatalf("unexpected error: %v", qErr)
	}
	_ = rows.Close()
}

func TestQueryWithRetryReadNonMySQLError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("network timeout"))

	conn := NewConnFromDB(db, DSN{}, false)
	_, qErr := QueryWithRetry(context.Background(), conn, nil, RetryPolicy{MaxAttempts: 1}, "SELECT 1")
	if qErr == nil {
		t.Fatal("expected error")
	}
	if qErr.Error() != "network timeout" {
		t.Errorf("expected 'network timeout', got %q", qErr.Error())
	}
}

func TestQueryWithRetryReadMySQLNonRetryableError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT").WillReturnError(&mysqldriver.MySQLError{Number: 1062, Message: "dup"})

	conn := NewConnFromDB(db, DSN{}, false)
	_, qErr := QueryWithRetry(context.Background(), conn, nil, RetryPolicy{MaxAttempts: 3}, "SELECT 1")
	if qErr == nil {
		t.Fatal("expected error")
	}
}

func TestQueryWithRetryZeroAttempts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(1))

	conn := NewConnFromDB(db, DSN{}, false)
	rows, qErr := QueryWithRetry(context.Background(), conn, nil, RetryPolicy{MaxAttempts: 0}, "SELECT 1")
	if qErr != nil {
		t.Fatalf("unexpected error: %v", qErr)
	}
	_ = rows.Close()
}
