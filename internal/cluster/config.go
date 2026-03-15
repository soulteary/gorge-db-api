package cluster

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

// ClusterConfig holds the parsed cluster.databases configuration,
// mirroring PhabricatorDatabaseRefParser behavior.
type ClusterConfig struct {
	Refs       []*DatabaseRef
	Namespace  string
	Driver     dbcore.DriverType
	SQLitePath string
	masters    []*DatabaseRef
	replicas   []*DatabaseRef
}

func (cc *ClusterConfig) IsSQLite() bool {
	return cc.Driver == dbcore.DriverSQLite
}

// NodeSpec is the JSON shape of a single node in cluster.databases config.
type NodeSpec struct {
	Host       string   `json:"host"`
	Port       int      `json:"port,omitempty"`
	User       string   `json:"user,omitempty"`
	Pass       string   `json:"pass,omitempty"`
	Role       string   `json:"role"`
	Disabled   bool     `json:"disabled,omitempty"`
	Partition  []string `json:"partition,omitempty"`
	Persistent bool     `json:"persistent,omitempty"`
}

type RawConfig struct {
	MysqlHost  string     `json:"mysql.host"`
	MysqlPort  int        `json:"mysql.port"`
	MysqlUser  string     `json:"mysql.user"`
	MysqlPass  string     `json:"mysql.pass"`
	Namespace  string     `json:"storage.default-namespace"`
	ClusterDBs []NodeSpec `json:"cluster.databases"`
	Driver     string     `json:"db.driver"`
	SQLitePath string     `json:"sqlite.path"`
}

// LoadFromFile parses a Phorge-style local.json config file.
func LoadFromFile(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var raw RawConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return BuildConfig(raw), nil
}

// LoadFromEnv builds config from environment variables (individual mode).
func LoadFromEnv() *ClusterConfig {
	driverStr := envOr("DB_DRIVER", "mysql")
	driver := dbcore.DriverType(driverStr)
	ns := envOr("STORAGE_NAMESPACE", "phorge")

	if driver == dbcore.DriverSQLite {
		sqlitePath := envOr("SQLITE_PATH", "gorge.db")
		ref := &DatabaseRef{
			Host:               "localhost",
			IsMaster:           true,
			IsIndividual:       true,
			IsDefaultPartition: true,
		}
		return &ClusterConfig{
			Refs:       []*DatabaseRef{ref},
			Namespace:  ns,
			Driver:     driver,
			SQLitePath: sqlitePath,
			masters:    []*DatabaseRef{ref},
		}
	}

	host := envOr("MYSQL_HOST", "127.0.0.1")
	port := envIntOr("MYSQL_PORT", 3306)
	user := envOr("MYSQL_USER", "root")

	ref := &DatabaseRef{
		Host:               host,
		Port:               port,
		User:               user,
		IsMaster:           true,
		IsIndividual:       true,
		IsDefaultPartition: true,
	}
	return &ClusterConfig{
		Refs:      []*DatabaseRef{ref},
		Namespace: ns,
		Driver:    driver,
		masters:   []*DatabaseRef{ref},
	}
}

func BuildConfig(raw RawConfig) *ClusterConfig {
	ns := raw.Namespace
	if ns == "" {
		ns = "phorge"
	}

	driver := dbcore.DriverType(raw.Driver)
	if driver == "" {
		driver = dbcore.DriverMySQL
	}

	if driver == dbcore.DriverSQLite {
		ref := &DatabaseRef{
			Host:               "localhost",
			IsMaster:           true,
			IsIndividual:       true,
			IsDefaultPartition: true,
		}
		return &ClusterConfig{
			Refs:       []*DatabaseRef{ref},
			Namespace:  ns,
			Driver:     driver,
			SQLitePath: nonEmpty(raw.SQLitePath, "gorge.db"),
			masters:    []*DatabaseRef{ref},
		}
	}

	if len(raw.ClusterDBs) == 0 {
		ref := &DatabaseRef{
			Host:               nonEmpty(raw.MysqlHost, "127.0.0.1"),
			Port:               nonZero(raw.MysqlPort, 3306),
			User:               nonEmpty(raw.MysqlUser, "root"),
			IsMaster:           true,
			IsIndividual:       true,
			IsDefaultPartition: true,
		}
		return &ClusterConfig{
			Refs:      []*DatabaseRef{ref},
			Namespace: ns,
			Driver:    driver,
			masters:   []*DatabaseRef{ref},
		}
	}

	var refs []*DatabaseRef
	var masters, replicas []*DatabaseRef
	mastersByHost := map[string]*DatabaseRef{}

	for _, spec := range raw.ClusterDBs {
		ref := &DatabaseRef{
			Host:     nonEmpty(spec.Host, raw.MysqlHost),
			Port:     nonZero(spec.Port, nonZero(raw.MysqlPort, 3306)),
			User:     nonEmpty(spec.User, raw.MysqlUser),
			IsMaster: spec.Role == "master",
			Disabled: spec.Disabled,
		}
		if len(spec.Partition) > 0 {
			ref.ApplicationMap = make(map[string]bool, len(spec.Partition))
			for _, app := range spec.Partition {
				if app == "default" {
					ref.IsDefaultPartition = true
				} else {
					ref.ApplicationMap[app] = true
				}
			}
		} else if ref.IsMaster {
			ref.IsDefaultPartition = true
		}
		refs = append(refs, ref)
		if ref.IsMaster {
			masters = append(masters, ref)
			mastersByHost[ref.RefKey()] = ref
		} else {
			replicas = append(replicas, ref)
		}
	}

	return &ClusterConfig{
		Refs:      refs,
		Namespace: ns,
		Driver:    driver,
		masters:   masters,
		replicas:  replicas,
	}
}

func (cc *ClusterConfig) GetMasterForApplication(app string) *DatabaseRef {
	var appMaster, defaultMaster *DatabaseRef
	for _, m := range cc.masters {
		if m.Disabled {
			continue
		}
		if m.IsApplicationHost(app) {
			appMaster = m
			break
		}
		if m.IsDefaultPartition && defaultMaster == nil {
			defaultMaster = m
		}
	}
	if appMaster != nil {
		return appMaster
	}
	return defaultMaster
}

func (cc *ClusterConfig) GetReplicaForApplication(app string) *DatabaseRef {
	var appReplicas, defaultReplicas []*DatabaseRef
	for _, r := range cc.replicas {
		if r.Disabled {
			continue
		}
		master := cc.GetMasterForApplication(app)
		if master != nil && master.IsApplicationHost(app) {
			appReplicas = append(appReplicas, r)
		}
		if master != nil && master.IsDefaultPartition {
			defaultReplicas = append(defaultReplicas, r)
		}
	}
	if len(appReplicas) > 0 {
		return appReplicas[0]
	}
	if len(defaultReplicas) > 0 {
		return defaultReplicas[0]
	}
	return nil
}

func (cc *ClusterConfig) GetAllRefs() []*DatabaseRef {
	return cc.Refs
}

func (cc *ClusterConfig) DatabaseName(app string) string {
	return cc.Namespace + "_" + app
}

func nonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func nonZero(a, b int) int {
	if a != 0 {
		return a
	}
	return b
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return fallback
	}
	return n
}
