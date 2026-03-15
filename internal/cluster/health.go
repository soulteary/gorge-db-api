package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

// HealthService probes database nodes and populates health/replication status,
// mirroring PhabricatorDatabaseRef::queryAll (behavior-spec section 5).
type HealthService struct {
	config *ClusterConfig
}

func NewHealthService(config *ClusterConfig) *HealthService {
	return &HealthService{config: config}
}

func (s *HealthService) QueryAll(ctx context.Context, password string) ([]*DatabaseRef, error) {
	refs := s.config.GetAllRefs()
	for _, ref := range refs {
		s.probeRef(ctx, ref, password)
	}
	return refs, nil
}

func (s *HealthService) QueryOne(ctx context.Context, refKey string, password string) (*DatabaseRef, error) {
	for _, ref := range s.config.GetAllRefs() {
		if ref.RefKey() == refKey {
			s.probeRef(ctx, ref, password)
			return ref, nil
		}
	}
	return nil, fmt.Errorf("ref %q not found", refKey)
}

func (s *HealthService) probeRef(ctx context.Context, ref *DatabaseRef, password string) {
	dsn := dbcore.DSN{
		Host:            ref.Host,
		Port:            ref.Port,
		User:            ref.User,
		Password:        password,
		MaxRetries:      0,
		ConnTimeoutSec:  2,
		QueryTimeoutSec: 2,
	}

	start := time.Now()

	conn, err := dbcore.NewConn(dsn, true)
	if err != nil {
		ref.ConnectionStatus = StatusFail
		ref.ConnectionMessage = err.Error()
		ref.ConnectionLatency = time.Since(start).Seconds()
		return
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Ping(ctx); err != nil {
		ref.ConnectionStatus = StatusFail
		ref.ConnectionMessage = err.Error()
		ref.ConnectionLatency = time.Since(start).Seconds()
		return
	}

	var replicaRow *sql.Rows
	replicaRow, err = conn.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		errMsg := err.Error()
		if isAccessDenied(errMsg) {
			ref.ConnectionStatus = StatusReplicationClient
			ref.ConnectionMessage = "No permission to run SHOW REPLICA STATUS"
		} else if isAuthError(errMsg) {
			ref.ConnectionStatus = StatusAuth
			ref.ConnectionMessage = errMsg
		} else {
			ref.ConnectionStatus = StatusFail
			ref.ConnectionMessage = errMsg
		}
		ref.ConnectionLatency = time.Since(start).Seconds()
		return
	}
	defer func() { _ = replicaRow.Close() }()

	ref.ConnectionStatus = StatusOkay
	ref.ConnectionLatency = time.Since(start).Seconds()

	columns, _ := replicaRow.Columns()
	isReplica := replicaRow.Next() && len(columns) > 0

	if ref.IsMaster && isReplica {
		ref.ReplicaStatus = ReplicationMasterReplica
		ref.ReplicaMessage = "This host has a master role, but is replicating data from another host"
	} else if !ref.IsMaster && !isReplica {
		ref.ReplicaStatus = ReplicationReplicaNone
		ref.ReplicaMessage = "This host has a replica role, but is not replicating"
	} else {
		ref.ReplicaStatus = ReplicationOkay
	}

	if isReplica {
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		_ = replicaRow.Scan(ptrs...)

		sbmIdx := -1
		for i, col := range columns {
			if col == "Seconds_Behind_Master" {
				sbmIdx = i
				break
			}
		}
		if sbmIdx >= 0 && vals[sbmIdx] != nil {
			var delay int
			if _, err := fmt.Sscanf(fmt.Sprintf("%s", vals[sbmIdx]), "%d", &delay); err == nil {
				ref.ReplicaDelay = &delay
				if delay > 30 {
					ref.ReplicaStatus = ReplicationSlow
					ref.ReplicaMessage = "This replica is lagging far behind the master"
				}
			} else {
				ref.ReplicaStatus = ReplicationNotReplicating
			}
		} else {
			ref.ReplicaStatus = ReplicationNotReplicating
		}
	}
}

func isAccessDenied(msg string) bool {
	return len(msg) > 5 && (msg[0:5] == "Error" || contains(msg, "Access denied") || contains(msg, "1227") || contains(msg, "1044"))
}

func isAuthError(msg string) bool {
	return contains(msg, "1045") || contains(msg, "Access denied for user")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
