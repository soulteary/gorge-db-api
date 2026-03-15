package dbcore

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/soulteary/gorge-db-api/internal/compat"
)

func TestIsReadQuery(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"SELECT * FROM foo", true},
		{"  SELECT 1", true},
		{"(SELECT a) UNION (SELECT b)", true},
		{"SHOW TABLES", true},
		{"EXPLAIN SELECT 1", true},
		{"select * from foo", true},
		{"  show databases", true},
		{"  explain analyze SELECT 1", true},
		{"INSERT INTO foo VALUES (1)", false},
		{"UPDATE foo SET a=1", false},
		{"DELETE FROM foo", false},
		{"CREATE TABLE foo (id INT)", false},
		{"DROP TABLE foo", false},
		{"START TRANSACTION", false},
		{"COMMIT", false},
		{"ALTER TABLE foo ADD COLUMN bar INT", false},
		{"TRUNCATE TABLE foo", false},
		{"REPLACE INTO foo VALUES (1)", false},
	}
	for _, tc := range cases {
		got := isReadQuery(tc.query)
		if got != tc.want {
			t.Errorf("isReadQuery(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestDSNString(t *testing.T) {
	dsn := DSN{
		Host:            "db.example.com",
		Port:            3306,
		User:            "testuser",
		Password:        "testpass",
		Database:        "testdb",
		ConnTimeoutSec:  10,
		QueryTimeoutSec: 30,
	}
	s := dsn.String()
	if !strings.Contains(s, "testuser") {
		t.Errorf("DSN string should contain user, got %q", s)
	}
	if !strings.Contains(s, "testpass") {
		t.Errorf("DSN string should contain password, got %q", s)
	}
	if !strings.Contains(s, "db.example.com:3306") {
		t.Errorf("DSN string should contain host:port, got %q", s)
	}
	if !strings.Contains(s, "testdb") {
		t.Errorf("DSN string should contain database, got %q", s)
	}
	if !strings.Contains(s, "tcp") {
		t.Errorf("DSN string should contain tcp, got %q", s)
	}
	if !strings.Contains(s, "parseTime=true") {
		t.Errorf("DSN string should contain parseTime=true, got %q", s)
	}
	if !strings.Contains(s, "interpolateParams=true") {
		t.Errorf("DSN string should contain interpolateParams=true, got %q", s)
	}
}

func TestDSNStringDifferentPorts(t *testing.T) {
	dsn := DSN{Host: "localhost", Port: 3307, User: "root"}
	s := dsn.String()
	if !strings.Contains(s, "localhost:3307") {
		t.Errorf("expected localhost:3307, got %q", s)
	}
}

func TestDSNStringTimeout(t *testing.T) {
	dsn := DSN{
		Host:            "localhost",
		Port:            3306,
		User:            "root",
		ConnTimeoutSec:  5,
		QueryTimeoutSec: 15,
	}
	s := dsn.String()
	if !strings.Contains(s, "timeout=5s") {
		t.Errorf("DSN should contain timeout=5s, got %q", s)
	}
	if !strings.Contains(s, "readTimeout=15s") {
		t.Errorf("DSN should contain readTimeout=15s, got %q", s)
	}
	if !strings.Contains(s, "writeTimeout=15s") {
		t.Errorf("DSN should contain writeTimeout=15s, got %q", s)
	}
}

func TestNewConnFromDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	dsn := DSN{Host: "mock", Port: 3306, Database: "testdb"}
	conn := NewConnFromDB(db, dsn, false)
	if conn == nil {
		t.Fatal("NewConnFromDB returned nil")
	}
	if conn.DB() != db {
		t.Error("DB() should return the injected *sql.DB")
	}
	if conn.IsReadOnly() {
		t.Error("should not be read-only")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestNewConnFromDBReadOnly(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn := NewConnFromDB(db, DSN{}, true)
	if !conn.IsReadOnly() {
		t.Error("should be read-only")
	}
}

func TestConnPing(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectPing()
	conn := NewConnFromDB(db, DSN{}, false)
	if err := conn.Ping(context.Background()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestConnClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	conn := NewConnFromDB(db, DSN{}, false)
	if err := conn.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestConnQueryContextReadOnWrite(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn := NewConnFromDB(db, DSN{Database: "mydb"}, true)
	_, qErr := conn.QueryContext(context.Background(), "INSERT INTO foo VALUES (1)")
	if qErr == nil {
		t.Fatal("expected error for write query on read-only conn")
	}
	dbErr, ok := qErr.(*compat.DBError)
	if !ok {
		t.Fatalf("expected *compat.DBError, got %T", qErr)
	}
	if dbErr.Code != compat.ErrReadonlyWrite {
		t.Errorf("expected ErrReadonlyWrite, got %s", dbErr.Code)
	}
}

func TestConnQueryContextReadOnRead(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	conn := NewConnFromDB(db, DSN{}, true)
	rows, qErr := conn.QueryContext(context.Background(), "SELECT 1")
	if qErr != nil {
		t.Fatalf("unexpected error: %v", qErr)
	}
	_ = rows.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestConnQueryContextWriteAllowed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("INSERT INTO foo").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	conn := NewConnFromDB(db, DSN{}, false)
	rows, qErr := conn.QueryContext(context.Background(), "INSERT INTO foo VALUES (1)")
	if qErr != nil {
		t.Fatalf("unexpected error on write-capable conn: %v", qErr)
	}
	_ = rows.Close()
}

func TestConnExecContextReadOnly(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn := NewConnFromDB(db, DSN{Database: "mydb"}, true)
	_, execErr := conn.ExecContext(context.Background(), "INSERT INTO foo VALUES (1)")
	if execErr == nil {
		t.Fatal("expected error for exec on read-only conn")
	}
	dbErr, ok := execErr.(*compat.DBError)
	if !ok {
		t.Fatalf("expected *compat.DBError, got %T", execErr)
	}
	if dbErr.Code != compat.ErrReadonlyWrite {
		t.Errorf("expected ErrReadonlyWrite, got %s", dbErr.Code)
	}
}

func TestConnExecContextWritable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectExec("INSERT INTO foo").WillReturnResult(sqlmock.NewResult(1, 1))
	conn := NewConnFromDB(db, DSN{}, false)
	res, execErr := conn.ExecContext(context.Background(), "INSERT INTO foo VALUES (1)")
	if execErr != nil {
		t.Fatalf("unexpected error: %v", execErr)
	}
	affected, _ := res.RowsAffected()
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}
}

func TestConnQueryRowContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT 42").WillReturnRows(sqlmock.NewRows([]string{"val"}).AddRow(42))
	conn := NewConnFromDB(db, DSN{}, false)
	var val int
	row := conn.QueryRowContext(context.Background(), "SELECT 42")
	if err := row.Scan(&val); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestConnIsReadOnly(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	ro := NewConnFromDB(db, DSN{}, true)
	rw := NewConnFromDB(db, DSN{}, false)
	if !ro.IsReadOnly() {
		t.Error("expected read-only")
	}
	if rw.IsReadOnly() {
		t.Error("expected read-write")
	}
}

func TestConnDB(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	conn := NewConnFromDB(db, DSN{}, false)
	if conn.DB() == nil {
		t.Error("DB() should not be nil")
	}
	if conn.DB() != db {
		t.Error("DB() should return the same *sql.DB")
	}
}
