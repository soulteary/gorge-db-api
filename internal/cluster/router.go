package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/soulteary/gorge-db-api/internal/compat"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

// DBRouter selects master/replica connections per application,
// mirroring PhabricatorLiskDAO::newClusterConnection (behavior-spec section 1).
type DBRouter struct {
	config   *ClusterConfig
	password string
	readOnly bool

	mu    sync.Mutex
	conns map[string]*dbcore.Conn
}

func NewDBRouter(config *ClusterConfig, password string) *DBRouter {
	return &DBRouter{
		config:   config,
		password: password,
		conns:    make(map[string]*dbcore.Conn),
	}
}

func (r *DBRouter) SetReadOnly(readOnly bool) {
	r.mu.Lock()
	r.readOnly = readOnly
	r.mu.Unlock()
}

func (r *DBRouter) GetWriter(ctx context.Context, application string) (*dbcore.Conn, error) {
	r.mu.Lock()
	isRO := r.readOnly
	r.mu.Unlock()

	if isRO {
		return nil, compat.NewClusterError(compat.ErrReadonlyWrite,
			fmt.Sprintf("server is read-only, cannot write to %q", application))
	}

	master := r.config.GetMasterForApplication(application)
	if master == nil {
		return nil, compat.NewClusterError(compat.ErrUnconfigured,
			fmt.Sprintf("no master configured for application %q", application))
	}

	conn, err := r.getOrCreateConn(master, application, false)
	if err != nil {
		return nil, compat.NewClusterError(compat.ErrMasterUnreachable,
			fmt.Sprintf("cannot connect to master for %q: %s", application, err))
	}
	return conn, nil
}

func (r *DBRouter) GetReader(ctx context.Context, application string) (*dbcore.Conn, error) {
	master := r.config.GetMasterForApplication(application)

	if master != nil {
		conn, err := r.getOrCreateConn(master, application, false)
		if err == nil {
			return conn, nil
		}
		r.mu.Lock()
		r.readOnly = true
		r.mu.Unlock()
	}

	replica := r.config.GetReplicaForApplication(application)
	if replica != nil {
		conn, err := r.getOrCreateConn(replica, application, true)
		if err == nil {
			return conn, nil
		}
	}

	if master == nil && replica == nil {
		return nil, compat.NewClusterError(compat.ErrUnconfigured,
			fmt.Sprintf("no master or replica for %q", application))
	}
	return nil, compat.NewClusterError(compat.ErrAllUnreachable,
		fmt.Sprintf("all hosts unreachable for %q", application))
}

func (r *DBRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.conns {
		_ = c.Close()
	}
	r.conns = make(map[string]*dbcore.Conn)
}

func (r *DBRouter) getOrCreateConn(ref *DatabaseRef, app string, readOnly bool) (*dbcore.Conn, error) {
	key := fmt.Sprintf("%s/%s/%v", ref.RefKey(), app, readOnly)

	r.mu.Lock()
	if c, ok := r.conns[key]; ok {
		r.mu.Unlock()
		return c, nil
	}
	r.mu.Unlock()

	dsn := dbcore.DSN{
		Driver:          r.config.Driver,
		Host:            ref.Host,
		Port:            ref.Port,
		User:            ref.User,
		Password:        r.password,
		Database:        r.config.DatabaseName(app),
		MaxRetries:      3,
		ConnTimeoutSec:  10,
		QueryTimeoutSec: 30,
	}
	if r.config.IsSQLite() {
		dsn.Path = r.config.SQLitePath
	}

	conn, err := dbcore.ConnectWithRetry(dsn, readOnly, dbcore.DefaultRetryPolicy())
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.conns[key] = conn
	r.mu.Unlock()

	return conn, nil
}
