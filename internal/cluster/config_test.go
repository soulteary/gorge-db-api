package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

func TestBuildConfigIndividual(t *testing.T) {
	raw := RawConfig{
		MysqlHost: "db1",
		MysqlPort: 3307,
		MysqlUser: "app",
		Namespace: "phorge",
	}
	cfg := BuildConfig(raw)
	if len(cfg.Refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(cfg.Refs))
	}
	ref := cfg.Refs[0]
	if !ref.IsMaster || !ref.IsIndividual || !ref.IsDefaultPartition {
		t.Error("individual ref should be master, individual, default partition")
	}
	if ref.Host != "db1" || ref.Port != 3307 {
		t.Errorf("host=%s port=%d", ref.Host, ref.Port)
	}
}

func TestBuildConfigCluster(t *testing.T) {
	raw := RawConfig{
		MysqlHost: "default-host",
		MysqlPort: 3306,
		MysqlUser: "root",
		Namespace: "phorge",
		ClusterDBs: []NodeSpec{
			{Host: "master1", Port: 3306, Role: "master", Partition: []string{"default"}},
			{Host: "master2", Port: 3306, Role: "master", Partition: []string{"maniphest"}},
			{Host: "replica1", Port: 3306, Role: "replica"},
		},
	}
	cfg := BuildConfig(raw)
	if len(cfg.Refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(cfg.Refs))
	}

	m := cfg.GetMasterForApplication("maniphest")
	if m == nil || m.Host != "master2" {
		t.Errorf("expected master2 for maniphest, got %v", m)
	}

	md := cfg.GetMasterForApplication("config")
	if md == nil || md.Host != "master1" {
		t.Errorf("expected master1 (default) for config, got %v", md)
	}
}

func TestDatabaseName(t *testing.T) {
	cfg := &ClusterConfig{Namespace: "phorge"}
	if got := cfg.DatabaseName("config"); got != "phorge_config" {
		t.Errorf("got %q, want phorge_config", got)
	}
}

func TestRefKey(t *testing.T) {
	r := &DatabaseRef{Host: "db1", Port: 3306}
	if k := r.RefKey(); k != "db1:3306" {
		t.Errorf("got %q", k)
	}
	r2 := &DatabaseRef{Host: "db2"}
	if k := r2.RefKey(); k != "db2" {
		t.Errorf("got %q", k)
	}
}

func TestBuildConfigDefaultNamespace(t *testing.T) {
	raw := RawConfig{MysqlHost: "localhost"}
	cfg := BuildConfig(raw)
	if cfg.Namespace != "phorge" {
		t.Errorf("expected default namespace 'phorge', got %q", cfg.Namespace)
	}
}

func TestBuildConfigEmptyRawUsesDefaults(t *testing.T) {
	raw := RawConfig{}
	cfg := BuildConfig(raw)
	if len(cfg.Refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(cfg.Refs))
	}
	ref := cfg.Refs[0]
	if ref.Host != "127.0.0.1" {
		t.Errorf("expected default host 127.0.0.1, got %q", ref.Host)
	}
	if ref.Port != 3306 {
		t.Errorf("expected default port 3306, got %d", ref.Port)
	}
	if ref.User != "root" {
		t.Errorf("expected default user root, got %q", ref.User)
	}
}

func TestBuildConfigClusterNodeInheritsDefaults(t *testing.T) {
	raw := RawConfig{
		MysqlHost: "shared-host",
		MysqlPort: 3307,
		MysqlUser: "admin",
		ClusterDBs: []NodeSpec{
			{Role: "master"},
		},
	}
	cfg := BuildConfig(raw)
	ref := cfg.Refs[0]
	if ref.Host != "shared-host" {
		t.Errorf("node should inherit host, got %q", ref.Host)
	}
	if ref.Port != 3307 {
		t.Errorf("node should inherit port, got %d", ref.Port)
	}
	if ref.User != "admin" {
		t.Errorf("node should inherit user, got %q", ref.User)
	}
}

func TestBuildConfigClusterNodeOverridesDefaults(t *testing.T) {
	raw := RawConfig{
		MysqlHost: "shared-host",
		MysqlPort: 3306,
		MysqlUser: "admin",
		ClusterDBs: []NodeSpec{
			{Host: "custom-host", Port: 3308, User: "custom", Role: "master"},
		},
	}
	cfg := BuildConfig(raw)
	ref := cfg.Refs[0]
	if ref.Host != "custom-host" {
		t.Errorf("expected custom-host, got %q", ref.Host)
	}
	if ref.Port != 3308 {
		t.Errorf("expected 3308, got %d", ref.Port)
	}
	if ref.User != "custom" {
		t.Errorf("expected custom, got %q", ref.User)
	}
}

func TestBuildConfigDisabledNode(t *testing.T) {
	raw := RawConfig{
		ClusterDBs: []NodeSpec{
			{Host: "m1", Role: "master", Disabled: true},
			{Host: "m2", Role: "master"},
		},
	}
	cfg := BuildConfig(raw)
	if len(cfg.Refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(cfg.Refs))
	}
	if !cfg.Refs[0].Disabled {
		t.Error("first ref should be disabled")
	}
	if cfg.Refs[1].Disabled {
		t.Error("second ref should not be disabled")
	}
}

func TestBuildConfigPartitionMapping(t *testing.T) {
	raw := RawConfig{
		ClusterDBs: []NodeSpec{
			{Host: "m1", Role: "master", Partition: []string{"default", "files"}},
		},
	}
	cfg := BuildConfig(raw)
	ref := cfg.Refs[0]
	if !ref.IsDefaultPartition {
		t.Error("ref should be default partition when partition includes 'default'")
	}
	if !ref.ApplicationMap["files"] {
		t.Error("ref should have 'files' in application map")
	}
	if ref.ApplicationMap["default"] {
		t.Error("'default' should NOT be in application map, it sets IsDefaultPartition")
	}
}

func TestBuildConfigMasterWithoutPartitionIsDefault(t *testing.T) {
	raw := RawConfig{
		ClusterDBs: []NodeSpec{
			{Host: "m1", Role: "master"},
		},
	}
	cfg := BuildConfig(raw)
	if !cfg.Refs[0].IsDefaultPartition {
		t.Error("master without partition should be default")
	}
}

func TestBuildConfigReplicaWithoutPartitionNotDefault(t *testing.T) {
	raw := RawConfig{
		ClusterDBs: []NodeSpec{
			{Host: "m1", Role: "master"},
			{Host: "r1", Role: "replica"},
		},
	}
	cfg := BuildConfig(raw)
	replica := cfg.Refs[1]
	if replica.IsDefaultPartition {
		t.Error("replica without explicit partition should not be default")
	}
}

func TestGetMasterForApplicationDisabledSkipped(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", IsDefaultPartition: true, Disabled: true},
			{Host: "m2", IsDefaultPartition: true},
		},
	}
	m := cfg.GetMasterForApplication("anything")
	if m == nil || m.Host != "m2" {
		t.Errorf("expected m2 (first non-disabled default), got %v", m)
	}
}

func TestGetMasterForApplicationPreferSpecific(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", IsDefaultPartition: true},
			{Host: "m2", ApplicationMap: map[string]bool{"files": true}},
		},
	}
	m := cfg.GetMasterForApplication("files")
	if m == nil || m.Host != "m2" {
		t.Errorf("expected m2 for specific app, got %v", m)
	}
}

func TestGetMasterForApplicationFallsBackToDefault(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", IsDefaultPartition: true},
			{Host: "m2", ApplicationMap: map[string]bool{"files": true}},
		},
	}
	m := cfg.GetMasterForApplication("config")
	if m == nil || m.Host != "m1" {
		t.Errorf("expected m1 (default) for unmatched app, got %v", m)
	}
}

func TestGetMasterForApplicationNoMatch(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", Disabled: true, IsDefaultPartition: true},
		},
	}
	if m := cfg.GetMasterForApplication("x"); m != nil {
		t.Errorf("expected nil when all masters disabled, got %v", m)
	}
}

func TestGetReplicaForApplicationReturnsNilWhenNone(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", IsDefaultPartition: true},
		},
	}
	r := cfg.GetReplicaForApplication("config")
	if r != nil {
		t.Errorf("expected nil replica, got %v", r)
	}
}

func TestGetReplicaForApplicationReturnsDefaultReplica(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", IsDefaultPartition: true},
		},
		replicas: []*DatabaseRef{
			{Host: "r1"},
		},
	}
	r := cfg.GetReplicaForApplication("config")
	if r == nil || r.Host != "r1" {
		t.Errorf("expected r1, got %v", r)
	}
}

func TestGetReplicaSkipsDisabled(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "m1", IsDefaultPartition: true},
		},
		replicas: []*DatabaseRef{
			{Host: "r1", Disabled: true},
			{Host: "r2"},
		},
	}
	r := cfg.GetReplicaForApplication("config")
	if r == nil || r.Host != "r2" {
		t.Errorf("expected r2 (skipping disabled), got %v", r)
	}
}

func TestGetAllRefs(t *testing.T) {
	refs := []*DatabaseRef{{Host: "a"}, {Host: "b"}}
	cfg := &ClusterConfig{Refs: refs}
	got := cfg.GetAllRefs()
	if len(got) != 2 {
		t.Errorf("expected 2 refs, got %d", len(got))
	}
}

func TestLoadFromFile(t *testing.T) {
	content := `{
		"mysql.host": "filehost",
		"mysql.port": 3307,
		"mysql.user": "fileuser",
		"storage.default-namespace": "testns"
	}`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if cfg.Namespace != "testns" {
		t.Errorf("expected namespace testns, got %q", cfg.Namespace)
	}
	if len(cfg.Refs) != 1 || cfg.Refs[0].Host != "filehost" {
		t.Errorf("expected host filehost, got %v", cfg.Refs)
	}
}

func TestLoadFromFileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path/config.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadFromFileInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("{invalid"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFromFile(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestLoadFromEnvDefaults(t *testing.T) {
	for _, key := range []string{"MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "STORAGE_NAMESPACE"} {
		t.Setenv(key, "")
	}
	cfg := LoadFromEnv()
	if cfg.Namespace != "phorge" {
		t.Errorf("expected default namespace phorge, got %q", cfg.Namespace)
	}
	ref := cfg.Refs[0]
	if ref.Host != "127.0.0.1" {
		t.Errorf("expected 127.0.0.1, got %q", ref.Host)
	}
	if ref.Port != 3306 {
		t.Errorf("expected 3306, got %d", ref.Port)
	}
	if ref.User != "root" {
		t.Errorf("expected root, got %q", ref.User)
	}
	if !ref.IsMaster || !ref.IsIndividual || !ref.IsDefaultPartition {
		t.Error("env ref should be master, individual, default")
	}
}

func TestLoadFromEnvCustom(t *testing.T) {
	t.Setenv("MYSQL_HOST", "envhost")
	t.Setenv("MYSQL_PORT", "3308")
	t.Setenv("MYSQL_USER", "envuser")
	t.Setenv("STORAGE_NAMESPACE", "envns")
	cfg := LoadFromEnv()
	if cfg.Namespace != "envns" {
		t.Errorf("expected envns, got %q", cfg.Namespace)
	}
	ref := cfg.Refs[0]
	if ref.Host != "envhost" {
		t.Errorf("expected envhost, got %q", ref.Host)
	}
	if ref.Port != 3308 {
		t.Errorf("expected 3308, got %d", ref.Port)
	}
	if ref.User != "envuser" {
		t.Errorf("expected envuser, got %q", ref.User)
	}
}

func TestLoadFromEnvInvalidPort(t *testing.T) {
	t.Setenv("MYSQL_PORT", "notanumber")
	cfg := LoadFromEnv()
	if cfg.Refs[0].Port != 3306 {
		t.Errorf("invalid port should fall back to 3306, got %d", cfg.Refs[0].Port)
	}
}

func TestNonEmpty(t *testing.T) {
	if nonEmpty("a", "b") != "a" {
		t.Error("should return first when non-empty")
	}
	if nonEmpty("", "b") != "b" {
		t.Error("should return second when first empty")
	}
}

func TestNonZero(t *testing.T) {
	if nonZero(5, 10) != 5 {
		t.Error("should return first when non-zero")
	}
	if nonZero(0, 10) != 10 {
		t.Error("should return second when first is zero")
	}
}

func TestDatabaseNameVariousApps(t *testing.T) {
	cfg := &ClusterConfig{Namespace: "myns"}
	cases := []struct {
		app, want string
	}{
		{"config", "myns_config"},
		{"meta_data", "myns_meta_data"},
		{"user", "myns_user"},
	}
	for _, tc := range cases {
		if got := cfg.DatabaseName(tc.app); got != tc.want {
			t.Errorf("DatabaseName(%q) = %q, want %q", tc.app, got, tc.want)
		}
	}
}

func TestLoadFromEnvSQLite(t *testing.T) {
	t.Setenv("DB_DRIVER", "sqlite")
	t.Setenv("SQLITE_PATH", "/data/test.db")
	t.Setenv("STORAGE_NAMESPACE", "myns")

	cfg := LoadFromEnv()
	if cfg.Driver != dbcore.DriverSQLite {
		t.Errorf("expected driver sqlite, got %q", cfg.Driver)
	}
	if cfg.SQLitePath != "/data/test.db" {
		t.Errorf("expected SQLitePath /data/test.db, got %q", cfg.SQLitePath)
	}
	if cfg.Namespace != "myns" {
		t.Errorf("expected namespace myns, got %q", cfg.Namespace)
	}
	if !cfg.IsSQLite() {
		t.Error("IsSQLite() should return true")
	}
	if len(cfg.Refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(cfg.Refs))
	}
	ref := cfg.Refs[0]
	if !ref.IsMaster || !ref.IsIndividual || !ref.IsDefaultPartition {
		t.Error("sqlite ref should be master, individual, default")
	}
}

func TestLoadFromEnvSQLiteDefaultPath(t *testing.T) {
	t.Setenv("DB_DRIVER", "sqlite")
	t.Setenv("SQLITE_PATH", "")

	cfg := LoadFromEnv()
	if cfg.SQLitePath != "gorge.db" {
		t.Errorf("expected default SQLitePath gorge.db, got %q", cfg.SQLitePath)
	}
}

func TestLoadFromEnvDefaultDriverIsMySQL(t *testing.T) {
	t.Setenv("DB_DRIVER", "")

	cfg := LoadFromEnv()
	if cfg.Driver != dbcore.DriverMySQL {
		t.Errorf("expected default driver mysql, got %q", cfg.Driver)
	}
	if cfg.IsSQLite() {
		t.Error("IsSQLite() should return false for mysql driver")
	}
}

func TestBuildConfigSQLite(t *testing.T) {
	raw := RawConfig{
		Driver:     "sqlite",
		SQLitePath: "/tmp/phorge.db",
		Namespace:  "testns",
	}
	cfg := BuildConfig(raw)
	if cfg.Driver != dbcore.DriverSQLite {
		t.Errorf("expected driver sqlite, got %q", cfg.Driver)
	}
	if cfg.SQLitePath != "/tmp/phorge.db" {
		t.Errorf("expected SQLitePath, got %q", cfg.SQLitePath)
	}
	if len(cfg.Refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(cfg.Refs))
	}
	if !cfg.Refs[0].IsMaster {
		t.Error("sqlite ref should be master")
	}
}

func TestBuildConfigSQLiteDefaultPath(t *testing.T) {
	raw := RawConfig{
		Driver: "sqlite",
	}
	cfg := BuildConfig(raw)
	if cfg.SQLitePath != "gorge.db" {
		t.Errorf("expected default SQLitePath gorge.db, got %q", cfg.SQLitePath)
	}
}

func TestBuildConfigDriverDefaultMySQL(t *testing.T) {
	raw := RawConfig{}
	cfg := BuildConfig(raw)
	if cfg.Driver != dbcore.DriverMySQL {
		t.Errorf("expected default driver mysql, got %q", cfg.Driver)
	}
}

func TestLoadFromFileSQLite(t *testing.T) {
	content := `{
		"db.driver": "sqlite",
		"sqlite.path": "/data/gorge.db",
		"storage.default-namespace": "filens"
	}`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if cfg.Driver != dbcore.DriverSQLite {
		t.Errorf("expected driver sqlite, got %q", cfg.Driver)
	}
	if cfg.SQLitePath != "/data/gorge.db" {
		t.Errorf("expected /data/gorge.db, got %q", cfg.SQLitePath)
	}
	if cfg.Namespace != "filens" {
		t.Errorf("expected filens, got %q", cfg.Namespace)
	}
}
