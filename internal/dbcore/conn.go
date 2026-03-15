package dbcore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/soulteary/gorge-db-api/internal/compat"
)

type DSN struct {
	Driver          DriverType
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	Path            string // SQLite file path
	MaxRetries      int
	ConnTimeoutSec  int
	QueryTimeoutSec int
}

type Conn struct {
	db       *sql.DB
	dsn      DSN
	readOnly bool
}

func NewConn(dsn DSN, readOnly bool) (*Conn, error) {
	dt := dsn.Driver
	if dt == "" {
		dt = DriverMySQL
	}
	drv, err := GetDriver(dt)
	if err != nil {
		return nil, err
	}

	db, err := drv.Open(dsn)
	if err != nil {
		return nil, err
	}

	pool := drv.PoolDefaults()
	db.SetMaxOpenConns(pool.MaxOpenConns)
	db.SetMaxIdleConns(pool.MaxIdleConns)
	if pool.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(pool.ConnMaxLifetime) * time.Second)
	}

	return &Conn{db: db, dsn: dsn, readOnly: readOnly}, nil
}

// ConnFactory creates a Conn from a DSN. Override in tests to inject sqlmock.
type ConnFactory func(dsn DSN, readOnly bool) (*Conn, error)

// NewConnFromDB wraps an existing *sql.DB, useful for testing with sqlmock.
func NewConnFromDB(db *sql.DB, dsn DSN, readOnly bool) *Conn {
	return &Conn{db: db, dsn: dsn, readOnly: readOnly}
}

func (c *Conn) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *Conn) Close() error {
	return c.db.Close()
}

func (c *Conn) IsReadOnly() bool {
	return c.readOnly
}

func (c *Conn) DB() *sql.DB {
	return c.db
}

func (c *Conn) DSN() DSN {
	return c.dsn
}

func (c *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if c.readOnly {
		if !isReadQuery(query) {
			return nil, &compat.DBError{
				Code:    compat.ErrReadonlyWrite,
				Message: fmt.Sprintf("write query on read-only connection (database %q)", c.dsn.Database),
			}
		}
	}
	return c.db.QueryContext(ctx, query, args...)
}

func (c *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if c.readOnly {
		return nil, &compat.DBError{
			Code:    compat.ErrReadonlyWrite,
			Message: fmt.Sprintf("write query on read-only connection (database %q)", c.dsn.Database),
		}
	}
	return c.db.ExecContext(ctx, query, args...)
}

func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}
