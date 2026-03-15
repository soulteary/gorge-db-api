package dbcore

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/soulteary/gorge-db-api/internal/compat"

	"github.com/go-sql-driver/mysql"
)

func init() {
	RegisterDriver(DriverMySQL, &mysqlDriver{})
}

type mysqlDriver struct{}

func (d *mysqlDriver) Open(dsn DSN) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User = dsn.User
	cfg.Passwd = dsn.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", dsn.Host, dsn.Port)
	cfg.DBName = dsn.Database
	cfg.Timeout = time.Duration(dsn.ConnTimeoutSec) * time.Second
	cfg.ReadTimeout = time.Duration(dsn.QueryTimeoutSec) * time.Second
	cfg.WriteTimeout = time.Duration(dsn.QueryTimeoutSec) * time.Second
	cfg.ParseTime = true
	cfg.InterpolateParams = true
	return sql.Open("mysql", cfg.FormatDSN())
}

var retryableConnectCodes = map[uint16]bool{
	2002: true, // Connection Timeout
	2003: true, // Unable to Connect
}

var retryableQueryCodes = map[uint16]bool{
	2013: true, // Connection Dropped
	2006: true, // Gone Away
}

func (d *mysqlDriver) IsRetryableConnectErr(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return retryableConnectCodes[mysqlErr.Number]
	}
	return false
}

func (d *mysqlDriver) IsRetryableQueryErr(err error) (uint16, bool) {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		if retryableQueryCodes[mysqlErr.Number] {
			return mysqlErr.Number, true
		}
		return mysqlErr.Number, false
	}
	return 0, false
}

func (d *mysqlDriver) MapError(err error) *compat.DBError {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return compat.FromMySQLError(mysqlErr.Number, err.Error())
	}
	return &compat.DBError{Code: compat.ErrQuery, Message: err.Error()}
}

func (d *mysqlDriver) PoolDefaults() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 300,
	}
}
