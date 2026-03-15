package dbcore

import (
	"context"
	"database/sql"
	"fmt"
)

// RetryPolicy configures query/connection retry behavior.
// Matches behavior-spec section 3.
type RetryPolicy struct {
	MaxAttempts int
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3}
}

// ConnectWithRetry opens and pings a connection with retry.
func ConnectWithRetry(dsn DSN, readOnly bool, policy RetryPolicy) (*Conn, error) {
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	dt := dsn.Driver
	if dt == "" {
		dt = DriverMySQL
	}
	drv, err := GetDriver(dt)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := NewConn(dsn, readOnly)
		if err != nil {
			lastErr = err
			if drv.IsRetryableConnectErr(err) && attempt < maxAttempts {
				continue
			}
			return nil, err
		}
		if err := conn.Ping(context.Background()); err != nil {
			_ = conn.Close()
			lastErr = err
			if drv.IsRetryableConnectErr(err) && attempt < maxAttempts {
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

	dt := conn.dsn.Driver
	if dt == "" {
		dt = DriverMySQL
	}
	drv, drvErr := GetDriver(dt)
	if drvErr != nil {
		return conn.QueryContext(ctx, query, args...)
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rows, err := conn.QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		_, retryable := drv.IsRetryableQueryErr(err)
		if retryable && attempt < maxAttempts {
			continue
		}
		return nil, drv.MapError(err)
	}
	return nil, lastErr
}
