package cluster

import "testing"

func TestRefKeyWithPort(t *testing.T) {
	cases := []struct {
		host string
		port int
		want string
	}{
		{"db1", 3306, "db1:3306"},
		{"10.0.0.1", 3307, "10.0.0.1:3307"},
		{"localhost", 1, "localhost:1"},
	}
	for _, tc := range cases {
		r := &DatabaseRef{Host: tc.host, Port: tc.port}
		if got := r.RefKey(); got != tc.want {
			t.Errorf("RefKey(%s:%d) = %q, want %q", tc.host, tc.port, got, tc.want)
		}
	}
}

func TestRefKeyWithoutPort(t *testing.T) {
	r := &DatabaseRef{Host: "myhost", Port: 0}
	if got := r.RefKey(); got != "myhost" {
		t.Errorf("RefKey with port=0: got %q, want %q", got, "myhost")
	}
	r2 := &DatabaseRef{Host: "myhost", Port: -1}
	if got := r2.RefKey(); got != "myhost" {
		t.Errorf("RefKey with port=-1: got %q, want %q", got, "myhost")
	}
}

func TestIsApplicationHostMatch(t *testing.T) {
	r := &DatabaseRef{
		ApplicationMap: map[string]bool{
			"files":     true,
			"maniphest": true,
		},
	}
	if !r.IsApplicationHost("files") {
		t.Error("expected true for 'files'")
	}
	if !r.IsApplicationHost("maniphest") {
		t.Error("expected true for 'maniphest'")
	}
}

func TestIsApplicationHostNoMatch(t *testing.T) {
	r := &DatabaseRef{
		ApplicationMap: map[string]bool{
			"files": true,
		},
	}
	if r.IsApplicationHost("config") {
		t.Error("expected false for 'config'")
	}
}

func TestIsApplicationHostNilMap(t *testing.T) {
	r := &DatabaseRef{}
	if r.IsApplicationHost("anything") {
		t.Error("expected false when ApplicationMap is nil")
	}
}

func TestIsApplicationHostEmptyKey(t *testing.T) {
	r := &DatabaseRef{
		ApplicationMap: map[string]bool{"files": true},
	}
	if r.IsApplicationHost("") {
		t.Error("expected false for empty key")
	}
}

func TestDatabaseRefJSONTags(t *testing.T) {
	r := &DatabaseRef{
		Host:              "h1",
		Port:              3306,
		IsMaster:          true,
		ConnectionStatus:  StatusOkay,
		ConnectionLatency: 0.5,
		ReplicaStatus:     ReplicationOkay,
	}
	if r.ConnectionStatus != StatusOkay {
		t.Error("connection status mismatch")
	}
	if r.ReplicaStatus != ReplicationOkay {
		t.Error("replica status mismatch")
	}
}

func TestConnectionStatusConstants(t *testing.T) {
	cases := []struct {
		status ConnectionStatus
		want   string
	}{
		{StatusOkay, "okay"},
		{StatusFail, "fail"},
		{StatusAuth, "auth"},
		{StatusReplicationClient, "replication-client"},
	}
	for _, tc := range cases {
		if string(tc.status) != tc.want {
			t.Errorf("ConnectionStatus %v != %q", tc.status, tc.want)
		}
	}
}

func TestReplicaStatusConstants(t *testing.T) {
	cases := []struct {
		status ReplicaStatus
		want   string
	}{
		{ReplicationOkay, "okay"},
		{ReplicationMasterReplica, "master-replica"},
		{ReplicationReplicaNone, "replica-none"},
		{ReplicationSlow, "replica-slow"},
		{ReplicationNotReplicating, "not-replicating"},
	}
	for _, tc := range cases {
		if string(tc.status) != tc.want {
			t.Errorf("ReplicaStatus %v != %q", tc.status, tc.want)
		}
	}
}
