package schema

import (
	"context"
	"fmt"

	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

type MigrationService struct {
	config      *cluster.ClusterConfig
	password    string
	connFactory dbcore.ConnFactory
}

func NewMigrationService(config *cluster.ClusterConfig, password string) *MigrationService {
	return &MigrationService{config: config, password: password, connFactory: dbcore.NewConn}
}

func (m *MigrationService) SetConnFactory(f dbcore.ConnFactory) {
	m.connFactory = f
}

// Status returns migration status for each ref, reading from {ns}_meta_data.patch_status.
func (m *MigrationService) Status(ctx context.Context) ([]MigrationStatus, error) {
	var statuses []MigrationStatus

	for _, ref := range m.config.GetAllRefs() {
		if ref.Disabled || !ref.IsMaster {
			continue
		}
		st := m.checkRef(ctx, ref)
		statuses = append(statuses, st)
	}
	return statuses, nil
}

func (m *MigrationService) buildDSN(ref *cluster.DatabaseRef) dbcore.DSN {
	dsn := dbcore.DSN{
		Driver:          m.config.Driver,
		Host:            ref.Host,
		Port:            ref.Port,
		User:            ref.User,
		Password:        m.password,
		ConnTimeoutSec:  2,
		QueryTimeoutSec: 10,
	}
	if m.config.IsSQLite() {
		dsn.Path = m.config.SQLitePath
	} else {
		dsn.Database = m.config.DatabaseName("meta_data")
	}
	return dsn
}

func (m *MigrationService) checkRef(ctx context.Context, ref *cluster.DatabaseRef) MigrationStatus {
	st := MigrationStatus{RefKey: ref.RefKey()}

	dsn := m.buildDSN(ref)
	conn, err := m.connFactory(dsn, true)
	if err != nil {
		return st
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Ping(ctx); err != nil {
		return st
	}

	st.Initialized = true

	rows, err := conn.QueryContext(ctx, "SELECT patch FROM patch_status")
	if err != nil {
		return st
	}
	defer func() { _ = rows.Close() }()

	applied := make(map[string]bool)
	for rows.Next() {
		var patch string
		if err := rows.Scan(&patch); err == nil {
			applied[patch] = true
			st.AppliedPatches = append(st.AppliedPatches, patch)
		}
	}

	st.TotalExpected = len(applied)

	if !m.config.IsSQLite() {
		var stateValue *string
		row := conn.QueryRowContext(ctx,
			fmt.Sprintf("SELECT stateValue FROM %s WHERE stateKey = 'cluster.databases'",
				"hoststate"))
		_ = row.Scan(&stateValue)
	}

	return st
}
