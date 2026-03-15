package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

// SetupIssue mirrors PhabricatorSetupIssue for database-related checks.
type SetupIssue struct {
	Key     string `json:"key"`
	Name    string `json:"name"`
	Summary string `json:"summary,omitempty"`
	Message string `json:"message"`
	IsFatal bool   `json:"is_fatal"`
	RefKey  string `json:"ref_key,omitempty"`
}

// MigrationStatus mirrors bin/storage status output.
type MigrationStatus struct {
	RefKey         string   `json:"ref_key"`
	Initialized    bool     `json:"initialized"`
	AppliedPatches []string `json:"applied_patches"`
	MissingPatches []string `json:"missing_patches,omitempty"`
	TotalExpected  int      `json:"total_expected"`
}

type SetupService struct {
	config   *cluster.ClusterConfig
	password string
}

func NewSetupService(config *cluster.ClusterConfig, password string) *SetupService {
	return &SetupService{config: config, password: password}
}

// CollectIssues runs database-related setup checks mirroring
// PhabricatorDatabaseSetupCheck and PhabricatorMySQLSetupCheck.
func (s *SetupService) CollectIssues(ctx context.Context) ([]SetupIssue, error) {
	var issues []SetupIssue

	for _, ref := range s.config.GetAllRefs() {
		if ref.Disabled {
			continue
		}
		refIssues := s.checkRef(ctx, ref)
		issues = append(issues, refIssues...)
	}
	return issues, nil
}

func (s *SetupService) checkRef(ctx context.Context, ref *cluster.DatabaseRef) []SetupIssue {
	var issues []SetupIssue
	refKey := ref.RefKey()

	dsn := dbcore.DSN{
		Host:            ref.Host,
		Port:            ref.Port,
		User:            ref.User,
		Password:        s.password,
		ConnTimeoutSec:  2,
		QueryTimeoutSec: 10,
	}
	conn, err := dbcore.NewConn(dsn, true)
	if err != nil {
		issues = append(issues, SetupIssue{
			Key: "db.connection", Name: "Database Connection Failed",
			Message: fmt.Sprintf("Cannot connect to %s: %s", refKey, err), IsFatal: true, RefKey: refKey,
		})
		return issues
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Ping(ctx); err != nil {
		issues = append(issues, SetupIssue{
			Key: "db.connection", Name: "Database Connection Failed",
			Message: fmt.Sprintf("Ping failed on %s: %s", refKey, err), IsFatal: true, RefKey: refKey,
		})
		return issues
	}

	// Version check
	var version string
	row := conn.QueryRowContext(ctx, "SELECT VERSION()")
	if err := row.Scan(&version); err == nil {
		issues = append(issues, s.checkVersion(refKey, version)...)
	}

	// InnoDB check
	rows, err := conn.QueryContext(ctx, "SHOW ENGINES")
	if err == nil {
		hasInnoDB := false
		for rows.Next() {
			var engine, support string
			var extra1, extra2, extra3, extra4 sql.NullString
			if err := rows.Scan(&engine, &support, &extra1, &extra2, &extra3, &extra4); err != nil {
				continue
			}
			if engine == "InnoDB" && (support == "YES" || support == "DEFAULT") {
				hasInnoDB = true
			}
		}
		_ = rows.Close()
		if !hasInnoDB {
			issues = append(issues, SetupIssue{
				Key: "mysql.innodb", Name: "InnoDB Not Available",
				Message: fmt.Sprintf("InnoDB engine not available on %s", refKey), IsFatal: true, RefKey: refKey,
			})
		}
	}

	// meta_data check
	metaDB := s.config.DatabaseName("meta_data")
	dbRows, err := conn.QueryContext(ctx, "SHOW DATABASES")
	if err == nil {
		found := false
		for dbRows.Next() {
			var db string
			if err := dbRows.Scan(&db); err == nil && db == metaDB {
				found = true
			}
		}
		_ = dbRows.Close()
		if !found {
			issues = append(issues, SetupIssue{
				Key: "storage.upgrade", Name: "Setup MySQL Schema",
				Message: fmt.Sprintf("Database %s not found on %s. Run bin/storage upgrade.", metaDB, refKey),
				IsFatal: true, RefKey: refKey,
			})
		}
	}

	// MySQL config checks
	issues = append(issues, s.checkMySQLConfig(ctx, conn, refKey)...)

	return issues
}

func (s *SetupService) checkVersion(refKey, version string) []SetupIssue {
	isMariaDB := strings.Contains(strings.ToLower(version), "mariadb")
	parts := strings.SplitN(version, "-", 2)
	ver := parts[0]

	var minVer, name string
	if isMariaDB {
		minVer = "10.5.1"
		name = "MariaDB"
	} else {
		minVer = "8.0.0"
		name = "MySQL"
	}

	if compareVersions(ver, minVer) < 0 {
		return []SetupIssue{{
			Key: "mysql.version", Name: fmt.Sprintf("Update %s", name),
			Message: fmt.Sprintf("Running %s %s, minimum required is %s", name, ver, minVer),
			IsFatal: true, RefKey: refKey,
		}}
	}
	return nil
}

func (s *SetupService) checkMySQLConfig(ctx context.Context, conn *dbcore.Conn, refKey string) []SetupIssue {
	var issues []SetupIssue

	// max_allowed_packet
	var maxPacket int64
	row := conn.QueryRowContext(ctx, "SELECT @@max_allowed_packet")
	if err := row.Scan(&maxPacket); err == nil && maxPacket < 32*1024*1024 {
		issues = append(issues, SetupIssue{
			Key: "mysql.max_allowed_packet", Name: "Small max_allowed_packet",
			Message: fmt.Sprintf("max_allowed_packet=%d on %s, recommended >= 33554432", maxPacket, refKey),
			RefKey:  refKey,
		})
	}

	// sql_mode
	var sqlMode string
	row = conn.QueryRowContext(ctx, "SELECT @@sql_mode")
	if err := row.Scan(&sqlMode); err == nil {
		if !strings.Contains(sqlMode, "STRICT_ALL_TABLES") {
			issues = append(issues, SetupIssue{
				Key: "sql_mode.strict", Name: "STRICT_ALL_TABLES Not Set",
				Summary: fmt.Sprintf("MySQL on %s not in strict mode", refKey),
				Message: "Enable STRICT_ALL_TABLES in sql_mode for safer behavior",
				RefKey:  refKey,
			})
		}
	}

	// innodb_buffer_pool_size
	var poolSize int64
	row = conn.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_size")
	if err := row.Scan(&poolSize); err == nil && poolSize < 225*1024*1024 {
		issues = append(issues, SetupIssue{
			Key: "mysql.innodb_buffer_pool_size", Name: "Small Buffer Pool",
			Message: fmt.Sprintf("innodb_buffer_pool_size=%d on %s, recommended >= 235929600", poolSize, refKey),
			RefKey:  refKey,
		})
	}

	// local_infile
	var localInfile int
	row = conn.QueryRowContext(ctx, "SELECT @@local_infile")
	if err := row.Scan(&localInfile); err == nil && localInfile != 0 {
		issues = append(issues, SetupIssue{
			Key: "mysql.local_infile", Name: "Unsafe local_infile Enabled",
			Message: fmt.Sprintf("local_infile is enabled on %s, disable it for security", refKey),
			RefKey:  refKey,
		})
	}

	// clock skew
	var epoch int64
	row = conn.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP()")
	if err := row.Scan(&epoch); err == nil {
		now := currentEpoch()
		delta := now - epoch
		if delta < 0 {
			delta = -delta
		}
		if delta > 60 {
			issues = append(issues, SetupIssue{
				Key: "mysql.clock", Name: "Major Clock Skew",
				Message: fmt.Sprintf("Clock skew of %d seconds between app server and %s", delta, refKey),
				RefKey:  refKey,
			})
		}
	}

	return issues
}

func currentEpoch() int64 {
	return time.Now().Unix()
}

func compareVersions(a, b string) int {
	pa := strings.Split(a, ".")
	pb := strings.Split(b, ".")
	for i := 0; i < 3; i++ {
		va, vb := 0, 0
		if i < len(pa) {
			_, _ = fmt.Sscanf(pa[i], "%d", &va)
		}
		if i < len(pb) {
			_, _ = fmt.Sscanf(pb[i], "%d", &vb)
		}
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
	}
	return 0
}
