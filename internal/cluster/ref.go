package cluster

import "fmt"

// DatabaseRef models a single database node, mirroring
// PhabricatorDatabaseRef fields and status constants.
type DatabaseRef struct {
	Host               string          `json:"host"`
	Port               int             `json:"port"`
	User               string          `json:"user"`
	IsMaster           bool            `json:"is_master"`
	Disabled           bool            `json:"disabled"`
	IsIndividual       bool            `json:"is_individual"`
	IsDefaultPartition bool            `json:"is_default_partition"`
	ApplicationMap     map[string]bool `json:"application_map,omitempty"`

	ConnectionStatus  ConnectionStatus `json:"connection_status"`
	ConnectionLatency float64          `json:"connection_latency_sec"`
	ConnectionMessage string           `json:"connection_message,omitempty"`

	ReplicaStatus  ReplicaStatus `json:"replica_status,omitempty"`
	ReplicaMessage string        `json:"replica_message,omitempty"`
	ReplicaDelay   *int          `json:"replica_delay_sec,omitempty"`
}

type ConnectionStatus string

const (
	StatusOkay              ConnectionStatus = "okay"
	StatusFail              ConnectionStatus = "fail"
	StatusAuth              ConnectionStatus = "auth"
	StatusReplicationClient ConnectionStatus = "replication-client"
)

type ReplicaStatus string

const (
	ReplicationOkay           ReplicaStatus = "okay"
	ReplicationMasterReplica  ReplicaStatus = "master-replica"
	ReplicationReplicaNone    ReplicaStatus = "replica-none"
	ReplicationSlow           ReplicaStatus = "replica-slow"
	ReplicationNotReplicating ReplicaStatus = "not-replicating"
)

func (r *DatabaseRef) RefKey() string {
	if r.Port > 0 {
		return fmt.Sprintf("%s:%d", r.Host, r.Port)
	}
	return r.Host
}

func (r *DatabaseRef) IsApplicationHost(app string) bool {
	return r.ApplicationMap[app]
}
