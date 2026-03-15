package dbcore

import (
	"context"
	"errors"
	"strings"
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

func TestMySQLDriverRetryableConnectErr(t *testing.T) {
	drv, err := GetDriver(DriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	mysqlErr := &mysqldriver.MySQLError{Number: 2002, Message: "Connection Timeout"}
	if !drv.IsRetryableConnectErr(mysqlErr) {
		t.Error("errno 2002 should be retryable for connect")
	}
}

func TestMySQLDriverNonRetryableConnectErr(t *testing.T) {
	drv, err := GetDriver(DriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	mysqlErr := &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate key"}
	if drv.IsRetryableConnectErr(mysqlErr) {
		t.Error("errno 1062 should not be retryable for connect")
	}
}

func TestMySQLDriverGenericErrNotRetryable(t *testing.T) {
	drv, err := GetDriver(DriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	genericErr := errors.New("generic error")
	if drv.IsRetryableConnectErr(genericErr) {
		t.Error("non-MySQL error should not be retryable")
	}
}

func TestMySQLDriverRetryableQueryErr(t *testing.T) {
	drv, err := GetDriver(DriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
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
		mysqlErr := &mysqldriver.MySQLError{Number: tc.number}
		_, got := drv.IsRetryableQueryErr(mysqlErr)
		if got != tc.retryable {
			t.Errorf("IsRetryableQueryErr(errno=%d) = %v, want %v", tc.number, got, tc.retryable)
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
	if !strings.Contains(qErr.Error(), "network timeout") {
		t.Errorf("expected error containing 'network timeout', got %q", qErr.Error())
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

func TestSQLiteDriverRegistered(t *testing.T) {
	drv, err := GetDriver(DriverSQLite)
	if err != nil {
		t.Fatalf("SQLite driver not registered: %v", err)
	}
	if drv == nil {
		t.Fatal("SQLite driver is nil")
	}
}

func TestGetDriverUnknown(t *testing.T) {
	_, err := GetDriver("unknown")
	if err == nil {
		t.Error("expected error for unknown driver")
	}
}
