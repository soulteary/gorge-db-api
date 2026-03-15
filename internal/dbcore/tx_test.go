package dbcore

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSavepointName(t *testing.T) {
	txm := &TxManager{}
	txm.depth = 1
	if name := txm.savepointName(); name != "Aphront_Savepoint_1" {
		t.Errorf("got %q, want Aphront_Savepoint_1", name)
	}
	txm.depth = 3
	if name := txm.savepointName(); name != "Aphront_Savepoint_3" {
		t.Errorf("got %q, want Aphront_Savepoint_3", name)
	}
}

func TestSavepointNameSequence(t *testing.T) {
	txm := &TxManager{}
	for i := 0; i <= 10; i++ {
		txm.depth = i
		name := txm.savepointName()
		expected := "Aphront_Savepoint_" + itoa(i)
		if name != expected {
			t.Errorf("depth=%d: got %q, want %q", i, name, expected)
		}
	}
}

func TestNewTxManager(t *testing.T) {
	txm := NewTxManager(nil)
	if txm == nil {
		t.Fatal("NewTxManager returned nil")
	}
	if txm.Depth() != 0 {
		t.Errorf("initial depth should be 0, got %d", txm.Depth())
	}
	if txm.IsInsideTransaction() {
		t.Error("should not be inside transaction initially")
	}
}

func TestTxManagerDepthConcurrentSafe(t *testing.T) {
	txm := &TxManager{}
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_ = txm.Depth()
			_ = txm.IsInsideTransaction()
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestIsInsideTransactionReflectsDepth(t *testing.T) {
	txm := &TxManager{}
	if txm.IsInsideTransaction() {
		t.Error("depth=0 should not be inside transaction")
	}
	txm.depth = 1
	if !txm.IsInsideTransaction() {
		t.Error("depth=1 should be inside transaction")
	}
	txm.depth = 5
	if !txm.IsInsideTransaction() {
		t.Error("depth=5 should be inside transaction")
	}
}

func TestTxManagerBeginCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectCommit()

	conn := NewConnFromDB(db, DSN{}, false)
	txm := NewTxManager(conn)
	ctx := context.Background()

	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if txm.Depth() != 1 {
		t.Errorf("depth after Begin: %d", txm.Depth())
	}
	if !txm.IsInsideTransaction() {
		t.Error("should be inside transaction after Begin")
	}

	if err := txm.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if txm.Depth() != 0 {
		t.Errorf("depth after Commit: %d", txm.Depth())
	}
	if txm.IsInsideTransaction() {
		t.Error("should not be inside transaction after Commit")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestTxManagerBeginRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectRollback()

	conn := NewConnFromDB(db, DSN{}, false)
	txm := NewTxManager(conn)
	ctx := context.Background()

	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := txm.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if txm.Depth() != 0 {
		t.Errorf("depth after Rollback: %d", txm.Depth())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestTxManagerNestedSavepoints(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectExec("SAVEPOINT Aphront_Savepoint_1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SAVEPOINT Aphront_Savepoint_2").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	conn := NewConnFromDB(db, DSN{}, false)
	txm := NewTxManager(conn)
	ctx := context.Background()

	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("Begin depth 0: %v", err)
	}
	if txm.Depth() != 1 {
		t.Errorf("depth should be 1, got %d", txm.Depth())
	}

	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("Begin depth 1 (savepoint): %v", err)
	}
	if txm.Depth() != 2 {
		t.Errorf("depth should be 2, got %d", txm.Depth())
	}

	if err := txm.Begin(ctx); err != nil {
		t.Fatalf("Begin depth 2 (savepoint): %v", err)
	}
	if txm.Depth() != 3 {
		t.Errorf("depth should be 3, got %d", txm.Depth())
	}

	// Commit nested (depth 3 -> 2)
	if err := txm.Commit(ctx); err != nil {
		t.Fatalf("Commit depth 3: %v", err)
	}
	if txm.Depth() != 2 {
		t.Errorf("depth should be 2 after nested commit, got %d", txm.Depth())
	}

	// Commit nested (depth 2 -> 1)
	if err := txm.Commit(ctx); err != nil {
		t.Fatalf("Commit depth 2: %v", err)
	}

	// Final commit (depth 1 -> 0, real COMMIT)
	if err := txm.Commit(ctx); err != nil {
		t.Fatalf("Commit depth 1: %v", err)
	}
	if txm.Depth() != 0 {
		t.Errorf("depth should be 0, got %d", txm.Depth())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestTxManagerNestedRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectExec("SAVEPOINT Aphront_Savepoint_1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ROLLBACK TO SAVEPOINT Aphront_Savepoint_1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	conn := NewConnFromDB(db, DSN{}, false)
	txm := NewTxManager(conn)
	ctx := context.Background()

	if err := txm.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	if err := txm.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	if txm.Depth() != 2 {
		t.Errorf("depth should be 2, got %d", txm.Depth())
	}

	// Rollback inner (depth 2 -> 1, ROLLBACK TO SAVEPOINT)
	if err := txm.Rollback(ctx); err != nil {
		t.Fatalf("Rollback nested: %v", err)
	}
	if txm.Depth() != 1 {
		t.Errorf("depth should be 1, got %d", txm.Depth())
	}

	// Commit outer (depth 1 -> 0, real COMMIT)
	if err := txm.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestTxManagerCommitWithoutBegin(t *testing.T) {
	txm := NewTxManager(nil)
	err := txm.Commit(context.Background())
	if err == nil {
		t.Error("expected error when committing without begin")
	}
	if err.Error() != "no open transaction to commit" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTxManagerRollbackWithoutBegin(t *testing.T) {
	txm := NewTxManager(nil)
	err := txm.Rollback(context.Background())
	if err == nil {
		t.Error("expected error when rolling back without begin")
	}
	if err.Error() != "no open transaction to rollback" {
		t.Errorf("unexpected error: %v", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
