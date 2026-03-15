package dbcore

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/soulteary/gorge-db-api/internal/compat"

	_ "modernc.org/sqlite"
)

func init() {
	RegisterDriver(DriverSQLite, &sqliteDriver{})
}

type sqliteDriver struct{}

func (d *sqliteDriver) Open(dsn DSN) (*sql.DB, error) {
	path := dsn.Path
	if path == "" {
		return nil, fmt.Errorf("sqlite: Path is required in DSN")
	}
	uri := fmt.Sprintf("file:%s?_pragma=busy_timeout%%3d5000&_pragma=journal_mode%%3dwal", path)
	return sql.Open("sqlite", uri)
}

func (d *sqliteDriver) IsRetryableConnectErr(err error) bool {
	return isSQLiteBusy(err)
}

func (d *sqliteDriver) IsRetryableQueryErr(err error) (uint16, bool) {
	if isSQLiteBusy(err) {
		return 5, true // SQLITE_BUSY = 5
	}
	return 0, false
}

func (d *sqliteDriver) MapError(err error) *compat.DBError {
	if isSQLiteBusy(err) {
		return &compat.DBError{Code: compat.ErrLockTimeout, Message: err.Error()}
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "unique constraint"):
		return &compat.DBError{Code: compat.ErrDuplicateKey, Message: msg}
	case strings.Contains(lower, "no such table"):
		return &compat.DBError{Code: compat.ErrSchema, Message: msg}
	case strings.Contains(lower, "no such column"):
		return &compat.DBError{Code: compat.ErrSchema, Message: msg}
	default:
		return &compat.DBError{Code: compat.ErrQuery, Message: msg}
	}
}

func (d *sqliteDriver) PoolDefaults() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: 0,
	}
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	type errCoder interface {
		Code() int
	}
	var ec errCoder
	if errors.As(err, &ec) {
		code := ec.Code()
		return code == 5 || code == 6 // SQLITE_BUSY / SQLITE_LOCKED
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "busy")
}
