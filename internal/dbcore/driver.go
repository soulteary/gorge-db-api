package dbcore

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/soulteary/gorge-db-api/internal/compat"
)

type DriverType string

const (
	DriverMySQL  DriverType = "mysql"
	DriverSQLite DriverType = "sqlite"
)

// Driver abstracts database-engine-specific operations so that MySQL and
// SQLite can share the same connection/retry/error infrastructure.
type Driver interface {
	Open(dsn DSN) (*sql.DB, error)
	IsRetryableConnectErr(err error) bool
	IsRetryableQueryErr(err error) (errno uint16, retryable bool)
	MapError(err error) *compat.DBError
	PoolDefaults() PoolConfig
}

type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime int // seconds
}

var (
	driversMu sync.RWMutex
	drivers   = map[DriverType]Driver{}
)

func RegisterDriver(dt DriverType, d Driver) {
	driversMu.Lock()
	drivers[dt] = d
	driversMu.Unlock()
}

func GetDriver(dt DriverType) (Driver, error) {
	driversMu.RLock()
	d, ok := drivers[dt]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unsupported database driver: %s", dt)
	}
	return d, nil
}
