package cluster

import "testing"

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
