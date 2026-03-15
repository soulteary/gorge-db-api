package cluster

import (
	"context"
	"testing"
)

func TestNewDBRouter(t *testing.T) {
	cfg := &ClusterConfig{
		Refs:      []*DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true, IsDefaultPartition: true}},
		Namespace: "ns",
		masters:   []*DatabaseRef{{Host: "h1", Port: 3306, IsMaster: true, IsDefaultPartition: true}},
	}
	r := NewDBRouter(cfg, "pass")
	if r == nil {
		t.Fatal("NewDBRouter returned nil")
	}
	if r.config != cfg {
		t.Error("config mismatch")
	}
	if r.password != "pass" {
		t.Errorf("password mismatch: %q", r.password)
	}
	if r.readOnly {
		t.Error("should not be read-only initially")
	}
	if r.conns == nil {
		t.Error("conns map should be initialized")
	}
}

func TestSetReadOnly(t *testing.T) {
	cfg := &ClusterConfig{}
	r := NewDBRouter(cfg, "")
	if r.readOnly {
		t.Error("should start not read-only")
	}
	r.SetReadOnly(true)
	if !r.readOnly {
		t.Error("should be read-only after SetReadOnly(true)")
	}
	r.SetReadOnly(false)
	if r.readOnly {
		t.Error("should not be read-only after SetReadOnly(false)")
	}
}

func TestGetWriterReadOnly(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{{Host: "h1", IsDefaultPartition: true}},
	}
	r := NewDBRouter(cfg, "")
	r.SetReadOnly(true)

	_, err := r.GetWriter(context.Background(), "config")
	if err == nil {
		t.Fatal("expected error for write on read-only router")
	}
	if err.Error() == "" {
		t.Error("error message should not be empty")
	}
}

func TestGetWriterNoMaster(t *testing.T) {
	cfg := &ClusterConfig{
		masters: []*DatabaseRef{
			{Host: "h1", IsDefaultPartition: true, Disabled: true},
		},
	}
	r := NewDBRouter(cfg, "")

	_, err := r.GetWriter(context.Background(), "config")
	if err == nil {
		t.Fatal("expected error when no master available")
	}
}

func TestGetReaderNoConfig(t *testing.T) {
	cfg := &ClusterConfig{}
	r := NewDBRouter(cfg, "")

	_, err := r.GetReader(context.Background(), "config")
	if err == nil {
		t.Fatal("expected error when no master or replica configured")
	}
}

func TestClose(t *testing.T) {
	cfg := &ClusterConfig{}
	r := NewDBRouter(cfg, "")
	r.Close()
	if len(r.conns) != 0 {
		t.Error("conns should be empty after Close")
	}
}

func TestCloseMultipleTimes(t *testing.T) {
	cfg := &ClusterConfig{}
	r := NewDBRouter(cfg, "")
	r.Close()
	r.Close()
	if len(r.conns) != 0 {
		t.Error("conns should be empty after multiple Close calls")
	}
}

func TestSetReadOnlyConcurrent(t *testing.T) {
	cfg := &ClusterConfig{}
	r := NewDBRouter(cfg, "")
	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func() {
			r.SetReadOnly(true)
			done <- true
		}()
		go func() {
			r.SetReadOnly(false)
			done <- true
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}
