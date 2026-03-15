package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

func TestNewMigrationService(t *testing.T) {
	cfg := &cluster.ClusterConfig{Refs: []*cluster.DatabaseRef{{Host: "h1"}}, Namespace: "ns"}
	svc := NewMigrationService(cfg, "pass")
	if svc == nil {
		t.Fatal("nil")
	}
	if svc.connFactory == nil {
		t.Error("connFactory should be set")
	}
}

func TestMigrationStatusEmpty(t *testing.T) {
	cfg := &cluster.ClusterConfig{Refs: []*cluster.DatabaseRef{}, Namespace: "ns"}
	svc := NewMigrationService(cfg, "")
	statuses, err := svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 0 {
		t.Errorf("expected 0, got %d", len(statuses))
	}
}

func TestMigrationStatusSkipsDisabled(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", IsMaster: true, Disabled: true}},
		Namespace: "ns",
	}
	svc := NewMigrationService(cfg, "")
	statuses, _ := svc.Status(context.Background())
	if len(statuses) != 0 {
		t.Errorf("expected 0, got %d", len(statuses))
	}
}

func TestMigrationStatusSkipsReplica(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "r1", IsMaster: false}},
		Namespace: "ns",
	}
	svc := NewMigrationService(cfg, "")
	statuses, _ := svc.Status(context.Background())
	if len(statuses) != 0 {
		t.Errorf("expected 0, got %d", len(statuses))
	}
}

func TestMigrationCheckRefConnFail(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true}},
		Namespace: "ns",
	}
	svc := &MigrationService{config: cfg, connFactory: failConnFactory()}
	st := svc.checkRef(context.Background(), cfg.Refs[0])
	if st.Initialized {
		t.Error("should not be initialized on connection failure")
	}
	if st.RefKey != "h1:3306" {
		t.Errorf("unexpected refkey: %q", st.RefKey)
	}
}

func TestMigrationCheckRefPingFail(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing().WillReturnError(sql.ErrConnDone)
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true}},
		Namespace: "ns",
	}
	factory := func(dsn dbcore.DSN, ro bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, ro), nil
	}
	svc := &MigrationService{config: cfg, connFactory: factory}
	st := svc.checkRef(context.Background(), cfg.Refs[0])
	if st.Initialized {
		t.Error("should not be initialized on ping failure")
	}
}

func TestMigrationCheckRefSuccess(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT patch FROM patch_status").WillReturnRows(
		sqlmock.NewRows([]string{"patch"}).
			AddRow("001.sql").AddRow("002.sql").AddRow("003.sql"))
	mock.ExpectQuery("SELECT stateValue FROM hoststate").WillReturnRows(
		sqlmock.NewRows([]string{"stateValue"}))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true}},
		Namespace: "ns",
	}
	factory := func(dsn dbcore.DSN, ro bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, ro), nil
	}
	svc := &MigrationService{config: cfg, connFactory: factory}
	st := svc.checkRef(context.Background(), cfg.Refs[0])
	if !st.Initialized {
		t.Error("should be initialized")
	}
	if len(st.AppliedPatches) != 3 {
		t.Errorf("expected 3 patches, got %d", len(st.AppliedPatches))
	}
	if st.TotalExpected != 3 {
		t.Errorf("expected TotalExpected=3, got %d", st.TotalExpected)
	}
}

func TestMigrationCheckRefQueryFail(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT patch FROM patch_status").WillReturnError(sql.ErrConnDone)
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true}},
		Namespace: "ns",
	}
	factory := func(dsn dbcore.DSN, ro bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, ro), nil
	}
	svc := &MigrationService{config: cfg, connFactory: factory}
	st := svc.checkRef(context.Background(), cfg.Refs[0])
	if !st.Initialized {
		t.Error("should be initialized (ping passed)")
	}
	if len(st.AppliedPatches) != 0 {
		t.Error("should have 0 patches on query fail")
	}
}

func TestMigrationStatusIntegration(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT patch FROM patch_status").WillReturnRows(
		sqlmock.NewRows([]string{"patch"}).AddRow("init.sql"))
	mock.ExpectQuery("SELECT stateValue FROM hoststate").WillReturnRows(
		sqlmock.NewRows([]string{"stateValue"}))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true}},
		Namespace: "ns",
	}
	factory := func(dsn dbcore.DSN, ro bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, ro), nil
	}
	svc := &MigrationService{config: cfg, connFactory: factory}
	statuses, err := svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	if !statuses[0].Initialized || statuses[0].TotalExpected != 1 {
		t.Errorf("unexpected: %+v", statuses[0])
	}
}
