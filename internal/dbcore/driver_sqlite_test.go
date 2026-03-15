package dbcore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteDriverOpen(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")

	dsn := DSN{
		Driver: DriverSQLite,
		Path:   dbPath,
	}
	conn, err := NewConn(dsn, false)
	if err != nil {
		t.Fatalf("NewConn failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestSQLiteDriverOpenMissingPath(t *testing.T) {
	dsn := DSN{
		Driver: DriverSQLite,
		Path:   "",
	}
	_, err := NewConn(dsn, false)
	if err == nil {
		t.Fatal("expected error for empty Path")
	}
}

func TestSQLiteDriverReadWrite(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "rw.db")

	dsn := DSN{Driver: DriverSQLite, Path: dbPath}
	conn, err := NewConn(dsn, false)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	_, err = conn.ExecContext(ctx, "CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = conn.ExecContext(ctx, "INSERT INTO test_tbl (name) VALUES (?)", "hello")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var name string
	row := conn.QueryRowContext(ctx, "SELECT name FROM test_tbl WHERE id = 1")
	if err := row.Scan(&name); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if name != "hello" {
		t.Errorf("expected 'hello', got %q", name)
	}
}

func TestSQLiteDriverReadOnlyProtection(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "ro.db")

	wDsn := DSN{Driver: DriverSQLite, Path: dbPath}
	wConn, err := NewConn(wDsn, false)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	ctx := context.Background()
	_, _ = wConn.ExecContext(ctx, "CREATE TABLE ro_test (id INTEGER PRIMARY KEY)")
	_ = wConn.Close()

	rDsn := DSN{Driver: DriverSQLite, Path: dbPath}
	rConn, err := NewConn(rDsn, true)
	if err != nil {
		t.Fatalf("NewConn readonly: %v", err)
	}
	defer func() { _ = rConn.Close() }()

	_, err = rConn.ExecContext(ctx, "INSERT INTO ro_test VALUES (1)")
	if err == nil {
		t.Fatal("expected error on write via read-only conn")
	}

	rows, err := rConn.QueryContext(ctx, "SELECT id FROM ro_test")
	if err != nil {
		t.Fatalf("SELECT on readonly conn should succeed: %v", err)
	}
	_ = rows.Close()
}

func TestSQLiteDriverPoolDefaults(t *testing.T) {
	drv, err := GetDriver(DriverSQLite)
	if err != nil {
		t.Fatal(err)
	}
	pool := drv.PoolDefaults()
	if pool.MaxOpenConns != 1 {
		t.Errorf("expected MaxOpenConns=1, got %d", pool.MaxOpenConns)
	}
	if pool.MaxIdleConns != 1 {
		t.Errorf("expected MaxIdleConns=1, got %d", pool.MaxIdleConns)
	}
}

func TestSQLiteDriverMapError(t *testing.T) {
	drv, err := GetDriver(DriverSQLite)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		msg      string
		wantCode string
	}{
		{"UNIQUE constraint failed: users.email", "ERR_DUPLICATE_KEY"},
		{"no such table: foo", "ERR_SCHEMA"},
		{"no such column: bar", "ERR_SCHEMA"},
		{"something else", "ERR_QUERY"},
	}
	for _, tc := range tests {
		dbErr := drv.MapError(errors.New(tc.msg))
		if string(dbErr.Code) != tc.wantCode {
			t.Errorf("MapError(%q): got %s, want %s", tc.msg, dbErr.Code, tc.wantCode)
		}
	}
}

func TestSQLiteDriverSavepoint(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "tx.db")

	dsn := DSN{Driver: DriverSQLite, Path: dbPath}
	conn, err := NewConn(dsn, false)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	_, _ = conn.ExecContext(ctx, "CREATE TABLE sp_test (id INTEGER PRIMARY KEY, val TEXT)")

	txm := NewTxManager(conn)
	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if txm.Depth() != 1 {
		t.Errorf("depth should be 1, got %d", txm.Depth())
	}

	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("nested Begin: %v", err)
	}
	if txm.Depth() != 2 {
		t.Errorf("depth should be 2, got %d", txm.Depth())
	}

	if err := txm.Commit(ctx); err != nil {
		t.Fatalf("nested Commit: %v", err)
	}
	if err := txm.Commit(ctx); err != nil {
		t.Fatalf("outer Commit: %v", err)
	}
}

func TestSQLiteDriverConnectWithRetry(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "retry.db")

	dsn := DSN{Driver: DriverSQLite, Path: dbPath}
	conn, err := ConnectWithRetry(dsn, false, DefaultRetryPolicy())
	if err != nil {
		t.Fatalf("ConnectWithRetry: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("Ping after ConnectWithRetry: %v", err)
	}
}

func TestSQLiteDriverPragmaTableInfo(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "pragma.db")

	dsn := DSN{Driver: DriverSQLite, Path: dbPath}
	conn, err := NewConn(dsn, false)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	_, err = conn.ExecContext(ctx, "CREATE TABLE pragma_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	rows, err := conn.QueryContext(ctx, "PRAGMA table_info('pragma_test')")
	if err != nil {
		t.Fatalf("PRAGMA table_info: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		var cid int
		var colName, colType string
		var notNull, pk int
		var dfltValue *string
		if err := rows.Scan(&cid, &colName, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		columns = append(columns, colName)
	}

	if len(columns) != 3 {
		t.Errorf("expected 3 columns, got %d: %v", len(columns), columns)
	}
}

func TestSQLiteDriverSqliteMaster(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "master.db")

	dsn := DSN{Driver: DriverSQLite, Path: dbPath}
	conn, err := NewConn(dsn, false)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	_, _ = conn.ExecContext(ctx, "CREATE TABLE alpha (id INTEGER PRIMARY KEY)")
	_, _ = conn.ExecContext(ctx, "CREATE TABLE beta (id INTEGER PRIMARY KEY)")

	rows, err := conn.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		t.Fatalf("sqlite_master query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}

	if len(tables) != 2 || tables[0] != "alpha" || tables[1] != "beta" {
		t.Errorf("expected [alpha beta], got %v", tables)
	}
}

func TestSQLiteDriverFileCreated(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "newfile.db")

	dsn := DSN{Driver: DriverSQLite, Path: dbPath}
	conn, err := NewConn(dsn, false)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _ = conn.ExecContext(ctx, "CREATE TABLE filecheck (id INTEGER)")
	_ = conn.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("database file should exist at %s", dbPath)
	}
}
