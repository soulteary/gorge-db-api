package cluster

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

func TestContains(t *testing.T) {
	cases := []struct {
		s, sub string
		want   bool
	}{
		{"Access denied for user", "Access denied", true},
		{"Access denied for user", "Access denied for user", true},
		{"Error 1227", "1227", true},
		{"Error 1044", "1044", true},
		{"Error 1045", "1045", true},
		{"hello world", "world", true},
		{"hello", "hello world", false},
		{"", "a", false},
		{"a", "", true},
		{"", "", true},
	}
	for _, tc := range cases {
		if got := contains(tc.s, tc.sub); got != tc.want {
			t.Errorf("contains(%q, %q) = %v, want %v", tc.s, tc.sub, got, tc.want)
		}
	}
}

func TestIsAccessDenied(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"Error 1227 (42000): Access denied", true},
		{"Error 1044: access denied", true},
		{"Access denied for user 'root'", true},
		{"some random error", false},
		{"Err", false},
		{"", false},
		{"Error: something went wrong", true},
	}
	for _, tc := range cases {
		if got := isAccessDenied(tc.msg); got != tc.want {
			t.Errorf("isAccessDenied(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"Error 1045: Access denied for user 'root'", true},
		{"Access denied for user 'app'@'10.0.0.1'", true},
		{"connection refused", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isAuthError(tc.msg); got != tc.want {
			t.Errorf("isAuthError(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}

func TestNewHealthService(t *testing.T) {
	cfg := &ClusterConfig{Refs: []*DatabaseRef{{Host: "h1", Port: 3306}}}
	svc := NewHealthService(cfg)
	if svc == nil {
		t.Fatal("nil")
	}
	if svc.connFactory == nil {
		t.Error("connFactory should be set")
	}
}

func TestQueryOneNotFound(t *testing.T) {
	cfg := &ClusterConfig{Refs: []*DatabaseRef{{Host: "h1", Port: 3306}}}
	svc := NewHealthService(cfg)
	_, err := svc.QueryOne(context.Background(), "nonexistent:9999", "")
	if err == nil {
		t.Error("expected error")
	}
}

func mockFactory(db *sql.DB) dbcore.ConnFactory {
	return func(dsn dbcore.DSN, readOnly bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, readOnly), nil
	}
}

func failFactory() dbcore.ConnFactory {
	return func(dsn dbcore.DSN, readOnly bool) (*dbcore.Conn, error) {
		return nil, sql.ErrConnDone
	}
}

func TestProbeRefConnFail(t *testing.T) {
	cfg := &ClusterConfig{Refs: []*DatabaseRef{{Host: "h1", Port: 3306}}}
	svc := &HealthService{config: cfg, connFactory: failFactory()}
	ref := cfg.Refs[0]
	svc.probeRef(context.Background(), ref, "")
	if ref.ConnectionStatus != StatusFail {
		t.Errorf("expected StatusFail, got %s", ref.ConnectionStatus)
	}
	if ref.ConnectionMessage == "" {
		t.Error("message should be set")
	}
}

func TestProbeRefPingFail(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing().WillReturnError(sql.ErrConnDone)
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "h1", Port: 3306}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ConnectionStatus != StatusFail {
		t.Errorf("expected StatusFail, got %s", ref.ConnectionStatus)
	}
}

func TestProbeRefMasterNoReplica(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "h1", Port: 3306, IsMaster: true}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ConnectionStatus != StatusOkay {
		t.Errorf("expected StatusOkay, got %s", ref.ConnectionStatus)
	}
	if ref.ReplicaStatus != ReplicationOkay {
		t.Errorf("expected ReplicationOkay, got %s", ref.ReplicaStatus)
	}
}

func TestProbeRefReplicaNone(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "r1", Port: 3306, IsMaster: false}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ConnectionStatus != StatusOkay {
		t.Errorf("expected StatusOkay, got %s", ref.ConnectionStatus)
	}
	if ref.ReplicaStatus != ReplicationReplicaNone {
		t.Errorf("expected ReplicationReplicaNone, got %s", ref.ReplicaStatus)
	}
}

func TestProbeRefMasterIsReplica(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"Seconds_Behind_Master"}).AddRow([]byte("5")))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "m1", Port: 3306, IsMaster: true}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ConnectionStatus != StatusOkay {
		t.Errorf("expected StatusOkay, got %s", ref.ConnectionStatus)
	}
	if ref.ReplicaStatus != ReplicationMasterReplica {
		t.Errorf("expected ReplicationMasterReplica, got %s", ref.ReplicaStatus)
	}
}

func TestProbeRefReplicaSlow(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"Seconds_Behind_Master"}).AddRow([]byte("120")))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "r1", Port: 3306, IsMaster: false}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ReplicaStatus != ReplicationSlow {
		t.Errorf("expected ReplicationSlow, got %s", ref.ReplicaStatus)
	}
	if ref.ReplicaDelay == nil || *ref.ReplicaDelay != 120 {
		t.Errorf("expected delay=120, got %v", ref.ReplicaDelay)
	}
}

func TestProbeRefReplicaOkay(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"Seconds_Behind_Master"}).AddRow([]byte("2")))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "r1", Port: 3306, IsMaster: false}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ReplicaStatus != ReplicationOkay {
		t.Errorf("expected ReplicationOkay, got %s", ref.ReplicaStatus)
	}
	if ref.ReplicaDelay == nil || *ref.ReplicaDelay != 2 {
		t.Errorf("expected delay=2, got %v", ref.ReplicaDelay)
	}
}

func TestProbeRefReplicaStatusAccessDenied(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnError(
		sql.ErrNoRows)
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "h1", Port: 3306}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ConnectionStatus != StatusFail {
		t.Errorf("expected StatusFail for generic error, got %s", ref.ConnectionStatus)
	}
}

func TestProbeRefReplicaNotReplicatingNilSBM(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"Other_Column"}).AddRow("value"))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "r1", Port: 3306, IsMaster: false}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	svc.probeRef(context.Background(), ref, "")
	if ref.ReplicaStatus != ReplicationNotReplicating {
		t.Errorf("expected ReplicationNotReplicating, got %s", ref.ReplicaStatus)
	}
}

func TestQueryAllWithMock(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "h1", Port: 3306, IsMaster: true}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	refs, err := svc.QueryAll(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Errorf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].ConnectionStatus != StatusOkay {
		t.Errorf("expected StatusOkay, got %s", refs[0].ConnectionStatus)
	}
}

func TestQueryOneWithMock(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	ref := &DatabaseRef{Host: "h1", Port: 3306, IsMaster: true}
	cfg := &ClusterConfig{Refs: []*DatabaseRef{ref}}
	svc := &HealthService{config: cfg, connFactory: mockFactory(db)}
	found, err := svc.QueryOne(context.Background(), "h1:3306", "")
	if err != nil {
		t.Fatal(err)
	}
	if found.ConnectionStatus != StatusOkay {
		t.Errorf("expected StatusOkay, got %s", found.ConnectionStatus)
	}
}
