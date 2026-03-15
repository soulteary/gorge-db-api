package dbcore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/soulteary/gorge-db-api/internal/compat"

	"github.com/go-sql-driver/mysql"
)

type DSN struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	MaxRetries      int
	ConnTimeoutSec  int
	QueryTimeoutSec int
}

func (d DSN) String() string {
	cfg := mysql.NewConfig()
	cfg.User = d.User
	cfg.Passwd = d.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", d.Host, d.Port)
	cfg.DBName = d.Database
	cfg.Timeout = time.Duration(d.ConnTimeoutSec) * time.Second
	cfg.ReadTimeout = time.Duration(d.QueryTimeoutSec) * time.Second
	cfg.WriteTimeout = time.Duration(d.QueryTimeoutSec) * time.Second
	cfg.ParseTime = true
	cfg.InterpolateParams = true
	return cfg.FormatDSN()
}

type Conn struct {
	db       *sql.DB
	dsn      DSN
	readOnly bool
}

func NewConn(dsn DSN, readOnly bool) (*Conn, error) {
	db, err := sql.Open("mysql", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
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
