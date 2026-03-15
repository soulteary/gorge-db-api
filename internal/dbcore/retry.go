package dbcore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/soulteary/gorge-db-api/internal/compat"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// RetryPolicy configures query/connection retry behavior.
// Matches behavior-spec section 3.
type RetryPolicy struct {
	MaxAttempts int
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3}
}

// retryableConnectCodes are MySQL errors that allow connection retry.
var retryableConnectCodes = map[uint16]bool{
	2002: true, // Connection Timeout
	2003: true, // Unable to Connect
}

// retryableQueryCodes are MySQL errors that allow query retry.
var retryableQueryCodes = map[uint16]bool{
	2013: true, // Connection Dropped
	2006: true, // Gone Away
}

func isMySQLRetryable(err error, codes map[uint16]bool) (uint16, bool) {
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		if codes[mysqlErr.Number] {
			return mysqlErr.Number, true
		}
		return mysqlErr.Number, false
	}
	return 0, false
}

// ConnectWithRetry opens and pings a connection with retry.
func ConnectWithRetry(dsn DSN, readOnly bool, policy RetryPolicy) (*Conn, error) {
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := NewConn(dsn, readOnly)
		if err != nil {
			lastErr = err
			if errno, ok := isMySQLRetryable(err, retryableConnectCodes); ok && attempt < maxAttempts {
				_ = errno
				continue
			}
			return nil, err
		}
		if err := conn.Ping(context.Background()); err != nil {
			_ = conn.Close()
			lastErr = err
			if _, ok := isMySQLRetryable(err, retryableConnectCodes); ok && attempt < maxAttempts {
				continue
			}
			return nil, err
		}
		return conn, nil
	}
	return nil, fmt.Errorf("connect failed after %d attempts: %w", maxAttempts, lastErr)
}

// QueryWithRetry executes a read query with retry.
// Write queries and queries inside transactions are NOT retried.
func QueryWithRetry(ctx context.Context, conn *Conn, txm *TxManager, policy RetryPolicy, query string, args ...any) (*sql.Rows, error) {
	if !isReadQuery(query) || (txm != nil && txm.IsInsideTransaction()) {
		return conn.QueryContext(ctx, query, args...)
	}

	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rows, err := conn.QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		errno, retryable := isMySQLRetryable(err, retryableQueryCodes)
		if retryable && attempt < maxAttempts {
			continue
		}
		if errno > 0 {
			return nil, compat.FromMySQLError(errno, err.Error())
		}
		return nil, err
	}
	return nil, lastErr
}
