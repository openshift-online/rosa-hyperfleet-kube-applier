package keys

import (
	"testing"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

func TestApplyDesireKeyFromDesire_ClusterScoped(t *testing.T) {
	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "desire-1"},
		Spec: kubeapplier.ApplyDesireSpec{
			ClusterID: "cluster-a",
		},
	}
	key, err := ApplyDesireKeyFromDesire(d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.ClusterID != "cluster-a" {
		t.Errorf("ClusterID: got %s, want cluster-a", key.ClusterID)
	}
	if key.Name != "desire-1" {
		t.Errorf("Name: got %s, want desire-1", key.Name)
	}
	if key.IsNodePoolScoped() {
		t.Error("cluster-scoped key should not be node-pool-scoped")
	}
}

func TestApplyDesireKeyFromDesire_NodePoolScoped(t *testing.T) {
	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "desire-2"},
		Spec: kubeapplier.ApplyDesireSpec{
			ClusterID:    "cluster-a",
			NodePoolName: "np-1",
		},
	}
	key, err := ApplyDesireKeyFromDesire(d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !key.IsNodePoolScoped() {
		t.Error("should be node-pool-scoped")
	}
	if key.NodePoolName != "np-1" {
		t.Errorf("NodePoolName: got %s, want np-1", key.NodePoolName)
	}
}

func TestApplyDesireKeyFromDesire_EmptyDocumentID(t *testing.T) {
	d := &kubeapplier.ApplyDesire{
		Spec: kubeapplier.ApplyDesireSpec{ClusterID: "cluster-a"},
	}
	_, err := ApplyDesireKeyFromDesire(d)
	if err == nil {
		t.Error("expected error for empty DocumentID")
	}
}

func TestReadDesireKeyFromDesire(t *testing.T) {
	d := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "read-1"},
		Spec: kubeapplier.ReadDesireSpec{
			ClusterID: "cluster-c",
		},
	}
	key, err := ReadDesireKeyFromDesire(d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.ClusterID != "cluster-c" || key.Name != "read-1" {
		t.Errorf("unexpected key: %+v", key)
	}
	if key.IsNodePoolScoped() {
		t.Error("cluster-scoped key should not be node-pool-scoped")
	}
}

func TestReadDesireKeyFromDesire_EmptyDocumentID(t *testing.T) {
	d := &kubeapplier.ReadDesire{}
	_, err := ReadDesireKeyFromDesire(d)
	if err == nil {
		t.Error("expected error for empty DocumentID")
	}
}
