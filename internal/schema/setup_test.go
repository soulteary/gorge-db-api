package schema

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

func TestCompareVersionsEqual(t *testing.T) {
	if compareVersions("8.0.0", "8.0.0") != 0 {
		t.Error("8.0.0 == 8.0.0")
	}
	if compareVersions("10.5.1", "10.5.1") != 0 {
		t.Error("10.5.1 == 10.5.1")
	}
}

func TestCompareVersionsLess(t *testing.T) {
	cases := []struct{ a, b string }{
		{"7.0.0", "8.0.0"}, {"8.0.0", "8.0.1"}, {"8.0.0", "8.1.0"},
		{"10.4.9", "10.5.1"}, {"5.7.42", "8.0.0"},
	}
	for _, tc := range cases {
		if compareVersions(tc.a, tc.b) >= 0 {
			t.Errorf("expected %s < %s", tc.a, tc.b)
		}
	}
}

func TestCompareVersionsGreater(t *testing.T) {
	cases := []struct{ a, b string }{
		{"8.0.1", "8.0.0"}, {"8.1.0", "8.0.0"}, {"9.0.0", "8.0.0"}, {"10.6.0", "10.5.1"},
	}
	for _, tc := range cases {
		if compareVersions(tc.a, tc.b) <= 0 {
			t.Errorf("expected %s > %s", tc.a, tc.b)
		}
	}
}

func TestCompareVersionsShort(t *testing.T) {
	if compareVersions("8", "8.0.0") != 0 {
		t.Error("8 should equal 8.0.0")
	}
	if compareVersions("8.0", "8.0.0") != 0 {
		t.Error("8.0 should equal 8.0.0")
	}
}

func TestCheckVersionMySQL(t *testing.T) {
	svc := &SetupService{}
	if issues := svc.checkVersion("ref", "8.0.33"); len(issues) != 0 {
		t.Errorf("8.0.33 should pass, got %v", issues)
	}
	if issues := svc.checkVersion("ref", "5.7.42"); len(issues) != 1 || !issues[0].IsFatal {
		t.Errorf("5.7.42 should fail, got %v", issues)
	}
}

func TestCheckVersionMariaDB(t *testing.T) {
	svc := &SetupService{}
	if issues := svc.checkVersion("ref", "10.6.12-MariaDB"); len(issues) != 0 {
		t.Errorf("MariaDB 10.6 should pass, got %v", issues)
	}
	if issues := svc.checkVersion("ref", "10.4.9-MariaDB-1:10.4.9+maria~focal"); len(issues) != 1 {
		t.Errorf("MariaDB 10.4 should fail, got %v", issues)
	}
}

func TestCheckVersionExactMinimum(t *testing.T) {
	svc := &SetupService{}
	if issues := svc.checkVersion("ref", "8.0.0"); len(issues) != 0 {
		t.Error("exact minimum MySQL should pass")
	}
	if issues := svc.checkVersion("ref", "10.5.1-MariaDB"); len(issues) != 0 {
		t.Error("exact minimum MariaDB should pass")
	}
}

func TestNewSetupService(t *testing.T) {
	cfg := &cluster.ClusterConfig{Refs: []*cluster.DatabaseRef{{Host: "h1"}}, Namespace: "ns"}
	svc := NewSetupService(cfg, "pass")
	if svc == nil {
		t.Fatal("nil")
	}
	if svc.connFactory == nil {
		t.Error("connFactory should be set to default")
	}
}

func TestCurrentEpoch(t *testing.T) {
	now := time.Now().Unix()
	got := currentEpoch()
	if got < now-1 || got > now+1 {
		t.Errorf("currentEpoch() = %d, want ~%d", got, now)
	}
}

func mockConnFactory(db *sql.DB) dbcore.ConnFactory {
	return func(dsn dbcore.DSN, readOnly bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, readOnly), nil
	}
}

func failConnFactory() dbcore.ConnFactory {
	return func(dsn dbcore.DSN, readOnly bool) (*dbcore.Conn, error) {
		return nil, sql.ErrConnDone
	}
}

func TestCheckRefConnectionFailed(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "phorge",
	}
	svc := &SetupService{config: cfg, connFactory: failConnFactory()}
	issues := svc.checkRef(context.Background(), cfg.Refs[0])
	if len(issues) == 0 {
		t.Fatal("expected connection error issue")
	}
	if issues[0].Key != "db.connection" || !issues[0].IsFatal {
		t.Errorf("unexpected issue: %+v", issues[0])
	}
}

func TestCheckRefPingFailed(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()
	mock.ExpectPing().WillReturnError(sql.ErrConnDone)
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "phorge",
	}
	svc := &SetupService{config: cfg, connFactory: mockConnFactory(db)}
	issues := svc.checkRef(context.Background(), cfg.Refs[0])
	if len(issues) == 0 || issues[0].Key != "db.connection" {
		t.Errorf("expected ping failure issue, got %v", issues)
	}
}

func TestCheckRefFullPass(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.33"))
	mock.ExpectQuery("SHOW ENGINES").WillReturnRows(
		sqlmock.NewRows([]string{"Engine", "Support", "c1", "c2", "c3", "c4"}).
			AddRow("InnoDB", "DEFAULT", nil, nil, nil, nil))
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(
		sqlmock.NewRows([]string{"Database"}).AddRow("phorge_meta_data"))
	// checkMySQLConfig queries
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(64 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@sql_mode").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION"))
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(512 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@local_infile").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(0))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(time.Now().Unix()))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "phorge",
	}
	svc := &SetupService{config: cfg, connFactory: mockConnFactory(db)}
	issues := svc.checkRef(context.Background(), cfg.Refs[0])
	if len(issues) != 0 {
		t.Errorf("expected 0 issues for full-pass, got %v", issues)
	}
}

func TestCheckRefNoInnoDB(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("8.0.33"))
	mock.ExpectQuery("SHOW ENGINES").WillReturnRows(
		sqlmock.NewRows([]string{"Engine", "Support", "c1", "c2", "c3", "c4"}).
			AddRow("MyISAM", "YES", nil, nil, nil, nil))
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(
		sqlmock.NewRows([]string{"Database"}).AddRow("phorge_meta_data"))
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(64 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@sql_mode").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("STRICT_ALL_TABLES"))
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(512 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@local_infile").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(0))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(time.Now().Unix()))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "phorge",
	}
	svc := &SetupService{config: cfg, connFactory: mockConnFactory(db)}
	issues := svc.checkRef(context.Background(), cfg.Refs[0])
	hasInnoDB := false
	for _, i := range issues {
		if i.Key == "mysql.innodb" {
			hasInnoDB = true
		}
	}
	if !hasInnoDB {
		t.Error("expected mysql.innodb issue")
	}
}

func TestCheckRefMissingMetaData(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	defer db.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("8.0.33"))
	mock.ExpectQuery("SHOW ENGINES").WillReturnRows(
		sqlmock.NewRows([]string{"Engine", "Support", "c1", "c2", "c3", "c4"}).
			AddRow("InnoDB", "YES", nil, nil, nil, nil))
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(
		sqlmock.NewRows([]string{"Database"}).AddRow("other_db"))
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(64 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@sql_mode").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("STRICT_ALL_TABLES"))
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(512 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@local_infile").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(0))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(time.Now().Unix()))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "phorge",
	}
	svc := &SetupService{config: cfg, connFactory: mockConnFactory(db)}
	issues := svc.checkRef(context.Background(), cfg.Refs[0])
	hasStorage := false
	for _, i := range issues {
		if i.Key == "storage.upgrade" {
			hasStorage = true
		}
	}
	if !hasStorage {
		t.Error("expected storage.upgrade issue")
	}
}

func TestCheckMySQLConfigAllWarnings(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(1024 * 1024)))
	mock.ExpectQuery("SELECT @@sql_mode").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("NO_ENGINE_SUBSTITUTION"))
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(8 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@local_infile").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(1))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(1000000)))

	conn := dbcore.NewConnFromDB(db, dbcore.DSN{}, true)
	svc := &SetupService{}
	issues := svc.checkMySQLConfig(context.Background(), conn, "h1:3306")

	keys := map[string]bool{}
	for _, i := range issues {
		keys[i.Key] = true
	}
	expected := []string{"mysql.max_allowed_packet", "sql_mode.strict", "mysql.innodb_buffer_pool_size", "mysql.local_infile", "mysql.clock"}
	for _, k := range expected {
		if !keys[k] {
			t.Errorf("expected issue %q", k)
		}
	}
}

func TestCheckMySQLConfigAllGood(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(64 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@sql_mode").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("STRICT_ALL_TABLES"))
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(int64(512 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@local_infile").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(0))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(time.Now().Unix()))

	conn := dbcore.NewConnFromDB(db, dbcore.DSN{}, true)
	svc := &SetupService{}
	issues := svc.checkMySQLConfig(context.Background(), conn, "h1:3306")
	if len(issues) != 0 {
		t.Errorf("expected 0 issues, got %v", issues)
	}
}

func TestSetupCollectIssuesSkipsDisabled(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Disabled: true}},
		Namespace: "ns",
	}
	svc := &SetupService{config: cfg, connFactory: failConnFactory()}
	issues, err := svc.CollectIssues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Errorf("disabled refs should be skipped, got %d issues", len(issues))
	}
}

func TestSetupCollectIssuesAggregates(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}, {Host: "h2", Port: 3306}},
		Namespace: "ns",
	}
	svc := &SetupService{config: cfg, connFactory: failConnFactory()}
	issues, _ := svc.CollectIssues(context.Background())
	if len(issues) != 2 {
		t.Errorf("expected 2 connection issues, got %d", len(issues))
	}
}
