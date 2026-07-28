package read_desire_kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listertesting"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/conditions"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/desirestatuswriter"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/keys"
)

func configMapTarget(name string) kubeapplier.ResourceReference {
	return kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "configmaps", Namespace: "default", Name: name,
	}
}

func newReadDesire(t *testing.T, target kubeapplier.ResourceReference) *kubeapplier.ReadDesire {
	t.Helper()
	d := &kubeapplier.ReadDesire{}
	d.SetDocumentID("cluster1--rd1")
	d.SetUpdateTime(time.Unix(1, 0))
	d.Spec = kubeapplier.ReadDesireSpec{
		ManagementCluster: "mc-1",
		ClusterID:         "cluster1",
		TargetItem:        target,
	}
	return d
}

func testKey() keys.ReadDesireKey {
	return keys.ReadDesireKey{ClusterID: "cluster1", Name: "cluster1--rd1"}
}

// recordingWriter captures UpdateStatus calls for assertion.
type recordingWriter struct {
	updates []*kubeapplier.ReadDesire
	desire  *kubeapplier.ReadDesire
}

func (w *recordingWriter) UpdateStatus(_ context.Context, _ keys.ReadDesireKey, mutate desirestatuswriter.MutateFunc[kubeapplier.ReadDesire]) error {
	if w.desire == nil {
		return nil
	}
	cp := w.desire.DeepCopy()
	mutate(cp)
	w.updates = append(w.updates, cp)
	*w.desire = *cp
	return nil
}

// syncedInformer returns a fake informer with HasSynced = true.
func syncedInformer(objects ...runtime.Object) cache.SharedIndexInformer {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, obj := range objects {
		_ = indexer.Add(obj)
	}
	return &fakeInformer{indexer: indexer, synced: true}
}

// unsyncedInformer returns a fake informer with HasSynced = false.
func unsyncedInformer() cache.SharedIndexInformer {
	return &fakeInformer{
		indexer: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}),
		synced:  false,
	}
}

type fakeInformer struct {
	cache.SharedIndexInformer
	indexer cache.Store
	synced  bool
}

func (f *fakeInformer) GetStore() cache.Store { return f.indexer }
func (f *fakeInformer) HasSynced() bool       { return f.synced }

func testConfigMap(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": "default"},
		"data":     map[string]any{"key": "value"},
	}}
}

// --- Constructor Tests ---

func TestNewReadDesireKubernetesController_PreCheckError(t *testing.T) {
	crud := listertesting.NewFakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]()
	tests := []struct {
		name   string
		target kubeapplier.ResourceReference
	}{
		{"missing version", kubeapplier.ResourceReference{Resource: "configmaps", Name: "cm1"}},
		{"missing resource", kubeapplier.ResourceReference{Version: "v1", Name: "cm1"}},
		{"missing name", kubeapplier.ResourceReference{Version: "v1", Resource: "configmaps"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReadDesireKubernetesController(testKey(), tt.target, time.Unix(1, 0), nil, crud)
			if err == nil {
				t.Fatal("expected error")
			}
			var preCheck *conditions.PreCheckError
			if !errors.As(err, &preCheck) {
				t.Errorf("expected PreCheckError, got %T: %v", err, err)
			}
		})
	}
}

// --- SyncOnce Tests ---

func TestSyncOnce_TargetExists(t *testing.T) {
	cm := testConfigMap("cm1")
	desire := newReadDesire(t, configMapTarget("cm1"))
	writer := &recordingWriter{desire: desire}

	specTime := time.Unix(1, 0)
	c := &ReadDesireKubernetesController{
		key:            testKey(),
		target:         configMapTarget("cm1"),
		specUpdateTime: specTime,
		namespaced:     true,
		informer:       syncedInformer(cm),
		fetcher:        &readDesireStatusFetcher{crud: crudWithDesire(t, desire)},
		writer:         writer,
	}

	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(writer.updates))
	}
	updated := writer.updates[0]
	if updated.Status.KubeContent == nil {
		t.Fatal("expected KubeContent to be set")
	}
	var m map[string]any
	if err := json.Unmarshal(updated.Status.KubeContent.Raw, &m); err != nil {
		t.Fatalf("unmarshal kubeContent: %v", err)
	}
	data, _ := m["data"].(map[string]any)
	if data["key"] != "value" {
		t.Errorf("expected data.key=value, got %v", data)
	}
	assertCondition(t, updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue)
	if !updated.Status.ObservedDesireUpdateTime.Equal(specTime) {
		t.Errorf("ObservedDesireUpdateTime: got %v, want %v", updated.Status.ObservedDesireUpdateTime, specTime)
	}
}

func TestSyncOnce_TargetAbsent_AfterSync(t *testing.T) {
	desire := newReadDesire(t, configMapTarget("cm1"))
	writer := &recordingWriter{desire: desire}

	c := &ReadDesireKubernetesController{
		key:            testKey(),
		target:         configMapTarget("cm1"),
		specUpdateTime: time.Unix(1, 0),
		namespaced:     true,
		informer:       syncedInformer(), // empty store
		fetcher:        &readDesireStatusFetcher{crud: crudWithDesire(t, desire)},
		writer:         writer,
	}

	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(writer.updates))
	}
	updated := writer.updates[0]
	if updated.Status.KubeContent != nil {
		t.Errorf("expected nil KubeContent for absent target, got %v", updated.Status.KubeContent)
	}
	assertCondition(t, updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue)
}

func TestSyncOnce_ByteEqual_NoOp(t *testing.T) {
	cm := testConfigMap("cm1")
	cmJSON, _ := json.Marshal(cm)

	desire := newReadDesire(t, configMapTarget("cm1"))
	desire.Status.KubeContent = &runtime.RawExtension{Raw: cmJSON}

	writer := &recordingWriter{desire: desire}

	c := &ReadDesireKubernetesController{
		key:            testKey(),
		target:         configMapTarget("cm1"),
		specUpdateTime: time.Unix(1, 0),
		namespaced:     true,
		informer:       syncedInformer(cm),
		fetcher:        &readDesireStatusFetcher{crud: crudWithDesire(t, desire)},
		writer:         writer,
	}

	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	// Byte-equal: should still call UpdateStatus (to ensure Successful=True)
	// but not change KubeContent.
	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 update (condition flip), got %d", len(writer.updates))
	}
	assertCondition(t, writer.updates[0].Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue)
}

func TestSyncOnce_NotSynced_Skips(t *testing.T) {
	desire := newReadDesire(t, configMapTarget("cm1"))
	writer := &recordingWriter{desire: desire}

	c := &ReadDesireKubernetesController{
		key:            testKey(),
		target:         configMapTarget("cm1"),
		specUpdateTime: time.Unix(1, 0),
		informer:       unsyncedInformer(),
		fetcher:        &readDesireStatusFetcher{crud: crudWithDesire(t, desire)},
		writer:         writer,
	}

	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	if len(writer.updates) != 0 {
		t.Errorf("expected 0 updates when not synced, got %d", len(writer.updates))
	}
}

func TestSyncOnce_DesireNotFound(t *testing.T) {
	crud := listertesting.NewFakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]()
	writer := &recordingWriter{}

	c := &ReadDesireKubernetesController{
		key:            testKey(),
		target:         configMapTarget("cm1"),
		specUpdateTime: time.Unix(1, 0),
		informer:       syncedInformer(),
		fetcher:        &readDesireStatusFetcher{crud: crud},
		writer:         writer,
	}

	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if len(writer.updates) != 0 {
		t.Errorf("expected 0 updates for not-found desire, got %d", len(writer.updates))
	}
}

func TestSyncOnce_ClusterScoped(t *testing.T) {
	ns := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": "test-ns"},
	}}

	target := kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "namespaces", Name: "test-ns",
	}
	desire := newReadDesire(t, target)
	writer := &recordingWriter{desire: desire}

	// Cluster-scoped: store key is just name, no namespace prefix
	indexer := cache.NewIndexer(func(obj any) (string, error) {
		return "test-ns", nil
	}, cache.Indexers{})
	_ = indexer.Add(ns)

	c := &ReadDesireKubernetesController{
		key:            testKey(),
		target:         target,
		specUpdateTime: time.Unix(1, 0),
		namespaced:     false,
		informer:       &fakeInformer{indexer: indexer, synced: true},
		fetcher:        &readDesireStatusFetcher{crud: crudWithDesire(t, desire)},
		writer:         writer,
	}

	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(writer.updates))
	}
	if writer.updates[0].Status.KubeContent == nil {
		t.Error("expected KubeContent for cluster-scoped object")
	}
}

// --- Helpers ---

func crudWithDesire(t *testing.T, d *kubeapplier.ReadDesire) *listertesting.FakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire] {
	t.Helper()
	crud := listertesting.NewFakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]()
	created, err := crud.Create(context.Background(), d)
	if err != nil {
		t.Fatalf("create desire: %v", err)
	}
	// Update the desire's UpdateTime to match what's in the store (for Replace precondition)
	d.SetUpdateTime(created.GetUpdateTime())
	d.SetCreateTime(created.GetCreateTime())
	return crud
}

func assertCondition(t *testing.T, conds []metav1.Condition, condType string, status metav1.ConditionStatus) {
	t.Helper()
	for _, c := range conds {
		if c.Type == condType {
			if c.Status != status {
				t.Errorf("condition %s: expected status %s, got %s", condType, status, c.Status)
			}
			return
		}
	}
	t.Errorf("condition %s not found in %+v", condType, conds)
}
