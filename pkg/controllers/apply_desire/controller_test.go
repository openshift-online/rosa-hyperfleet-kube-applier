package apply_desire

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/controllerutils"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listertesting"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/conditions"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/desirestatuswriter"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/keys"
)

var configMapGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

func fakeDynamic(t *testing.T, gvrToListKind map[schema.GroupVersionResource]string, objects ...runtime.Object) *fake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	return fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objects...)
}

func configMapTarget(name string) kubeapplier.ResourceReference {
	return kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "configmaps", Namespace: "default", Name: name,
	}
}

// newApplyDesire creates an SSA ApplyDesire with kubeContent.
func newApplyDesire(t *testing.T, name string, target kubeapplier.ResourceReference, kubeContent []byte) *kubeapplier.ApplyDesire {
	t.Helper()
	d := &kubeapplier.ApplyDesire{}
	d.SetDocumentID("cluster1--" + name)
	d.SetUpdateTime(time.Unix(1, 0))
	d.Spec = kubeapplier.ApplyDesireSpec{
		ManagementCluster: "mc-1",
		ClusterID:         "cluster1",
		Type:              kubeapplier.ApplyDesireTypeServerSideApply,
		TargetItem:        target,
	}
	if kubeContent != nil {
		d.Spec.ServerSideApply = &kubeapplier.ServerSideApplyConfig{
			KubeContent: &runtime.RawExtension{Raw: kubeContent},
		}
	}
	return d
}

// newDeleteApplyDesire creates a Delete-type ApplyDesire.
func newDeleteApplyDesire(t *testing.T, name string, target kubeapplier.ResourceReference) *kubeapplier.ApplyDesire {
	t.Helper()
	d := &kubeapplier.ApplyDesire{}
	d.SetDocumentID("cluster1--" + name)
	d.SetUpdateTime(time.Unix(1, 0))
	d.Spec = kubeapplier.ApplyDesireSpec{
		ManagementCluster: "mc-1",
		ClusterID:         "cluster1",
		Type:              kubeapplier.ApplyDesireTypeDelete,
		TargetItem:        target,
	}
	return d
}

func mustKey(t *testing.T, d *kubeapplier.ApplyDesire) keys.ApplyDesireKey {
	t.Helper()
	key, err := keys.ApplyDesireKeyFromDesire(d)
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	return key
}

// newCadenceController builds a controller wired only with the fields the
// cadence tests touch.
func newCadenceController(t *testing.T, cfg Config) *ApplyDesireController {
	t.Helper()
	cfg = cfg.withDefaults()
	checker := controllerutils.NewTimeBasedCooldownChecker(cfg.CooldownPeriod)
	checker.SetClock(cfg.Clock)
	deleteCooldownChecker := controllerutils.NewTimeBasedCooldownChecker(cfg.DeleteCooldownPeriod)
	deleteCooldownChecker.SetClock(cfg.Clock)
	return &ApplyDesireController{
		name: "ApplyDesireController",
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.ApplyDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ApplyDesireKey]{Name: "test"},
		),
		cfg:            cfg,
		cooldown:       checker,
		deleteCooldown: deleteCooldownChecker,
	}
}

type errFetcher struct{ err error }

func (f *errFetcher) Fetch(context.Context, keys.ApplyDesireKey) (*kubeapplier.ApplyDesire, error) {
	return nil, f.err
}

func validConfigMapJSON(name string) []byte {
	return []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + name + `","namespace":"default"},"data":{"key":"value"}}`)
}

func existingConfigMap(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"name": name, "namespace": "default",
			"uid": "test-uid-123",
		},
	}}
}

func terminatingConfigMap(name string) *unstructured.Unstructured {
	now := metav1.Now()
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"name": name, "namespace": "default",
			"uid":                        "test-uid-123",
			"deletionTimestamp":          now.Format(time.RFC3339),
			"deletionGracePeriodSeconds": int64(0),
		},
	}}
}

func assertCondition(t *testing.T, conds []metav1.Condition, condType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	for _, c := range conds {
		if c.Type == condType {
			if c.Status != status {
				t.Errorf("condition %s: expected status %s, got %s", condType, status, c.Status)
			}
			if reason != "" && c.Reason != reason {
				t.Errorf("condition %s: expected reason %s, got %s", condType, reason, c.Reason)
			}
			return
		}
	}
	t.Errorf("condition %s not found in %+v", condType, conds)
}

func assertConditionMessage(t *testing.T, conds []metav1.Condition, condType string, substr string) {
	t.Helper()
	for _, c := range conds {
		if c.Type == condType {
			if !strings.Contains(c.Message, substr) {
				t.Errorf("condition %s message %q does not contain %q", condType, c.Message, substr)
			}
			return
		}
	}
	t.Errorf("condition %s not found", condType)
}

// --- SSA Tests ---

func TestApplyDesired_IssuesSSAPatch(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "ConfigMapList"})

	// The fake dynamic client requires a reactor to handle Apply (patch).
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "cm1", "namespace": "default"},
		}}, nil
	})

	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	c := &ApplyDesireController{dyn: dyn}

	rv, err := c.applyDesired(ctx, d)
	if err != nil {
		t.Fatalf("applyDesired: %v", err)
	}

	actions := dyn.Actions()
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	pa, ok := actions[0].(clienttesting.PatchAction)
	if !ok {
		t.Fatalf("expected PatchAction, got %T", actions[0])
	}
	if pa.GetPatchType() != "application/apply-patch+yaml" {
		t.Errorf("expected apply patch type, got %s", pa.GetPatchType())
	}
	if pa.GetNamespace() != "default" {
		t.Errorf("expected namespace default, got %s", pa.GetNamespace())
	}
	if pa.GetName() != "cm1" {
		t.Errorf("expected name cm1, got %s", pa.GetName())
	}
	_ = rv
}

func TestApplyDesired_NamespacedVsClusterScoped(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "NamespaceList"})

	dyn.PrependReactor("patch", "namespaces", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace",
			"metadata": map[string]any{"name": "test-ns"},
		}}, nil
	})

	nsJSON := []byte(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"test-ns"}}`)
	target := kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "namespaces", Name: "test-ns",
	}
	d := newApplyDesire(t, "ns1", target, nsJSON)
	c := &ApplyDesireController{dyn: dyn}

	if _, err := c.applyDesired(ctx, d); err != nil {
		t.Fatalf("applyDesired: %v", err)
	}

	pa := dyn.Actions()[0].(clienttesting.PatchAction)
	if pa.GetNamespace() != "" {
		t.Errorf("expected empty namespace for cluster-scoped, got %q", pa.GetNamespace())
	}
}

// --- PreCheck Tests ---

func TestApplyDesired_PreCheck_MissingFields(t *testing.T) {
	ctx := context.Background()
	c := &ApplyDesireController{}
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
			d := newApplyDesire(t, "cm1", tt.target, validConfigMapJSON("cm1"))
			_, err := c.applyDesired(ctx, d)
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

func TestApplyDesired_PreCheck_EmptyKubeContent(t *testing.T) {
	ctx := context.Background()
	c := &ApplyDesireController{}

	t.Run("nil serverSideApply", func(t *testing.T) {
		d := newApplyDesire(t, "cm1", configMapTarget("cm1"), nil)
		_, err := c.applyDesired(ctx, d)
		var preCheck *conditions.PreCheckError
		if !errors.As(err, &preCheck) {
			t.Errorf("expected PreCheckError, got %v", err)
		}
	})

	t.Run("empty kubeContent", func(t *testing.T) {
		d := newApplyDesire(t, "cm1", configMapTarget("cm1"), nil)
		d.Spec.ServerSideApply = &kubeapplier.ServerSideApplyConfig{KubeContent: &runtime.RawExtension{Raw: []byte{}}}
		_, err := c.applyDesired(ctx, d)
		var preCheck *conditions.PreCheckError
		if !errors.As(err, &preCheck) {
			t.Errorf("expected PreCheckError, got %v", err)
		}
	})
}

func TestApplyDesired_PreCheck_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	c := &ApplyDesireController{}
	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), []byte("not json"))
	_, err := c.applyDesired(ctx, d)
	var preCheck *conditions.PreCheckError
	if !errors.As(err, &preCheck) {
		t.Errorf("expected PreCheckError, got %v", err)
	}
}

// --- Error Classification Tests ---

func TestApplyDesired_KubeAPIError_4xx(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "ConfigMapList"})
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "cm1", errors.New("forbidden"))
	})

	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	_, err := c.applyDesired(ctx, d)

	if err == nil {
		t.Fatal("expected error")
	}
	if classifyAsDegraded(err) != nil {
		t.Error("4xx errors should NOT classify as degraded")
	}
}

func TestApplyDesired_KubeAPIError_5xx(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "ConfigMapList"})
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusInternalServerError,
			Reason:  metav1.StatusReasonInternalError,
			Message: "internal server error",
		}}
	})

	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	_, err := c.applyDesired(ctx, d)

	if err == nil {
		t.Fatal("expected error")
	}
	if classifyAsDegraded(err) == nil {
		t.Error("5xx errors should classify as degraded")
	}
}

func TestClassifyAsDegraded_PreCheckNotDegraded(t *testing.T) {
	err := conditions.NewPreCheckError(errors.New("bad spec"))
	if classifyAsDegraded(err) != nil {
		t.Error("PreCheckError should not be degraded")
	}
}

func TestClassifyAsDegraded_NilNotDegraded(t *testing.T) {
	if classifyAsDegraded(nil) != nil {
		t.Error("nil error should not be degraded")
	}
}

// --- SyncOnce Tests ---

func TestSyncOnce_DesireNotFound(t *testing.T) {
	ctx := context.Background()
	crud := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	specFetcher := &applyDesireSpecFetcher{reader: crud}
	c := &ApplyDesireController{specFetcher: specFetcher}

	key := keys.ApplyDesireKey{ClusterID: "c1", Name: "cluster1--cm1"}
	if err := c.SyncOnce(ctx, key); err != nil {
		t.Fatalf("SyncOnce should return nil for not-found, got: %v", err)
	}
}

func TestSyncOnce_Success(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "ConfigMapList"})
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "cm1", "namespace": "default", "generation": int64(3)},
		}}, nil
	})

	specCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	created, err := specCRUD.Create(ctx, d)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	specFetcher := &applyDesireSpecFetcher{reader: specCRUD}
	statusFetcher := &applyDesireStatusFetcher{crud: statusCRUD}
	writer := desirestatuswriter.New[kubeapplier.ApplyDesire, keys.ApplyDesireKey, *kubeapplier.ApplyDesire](
		statusFetcher, &applyDesireReplacer{crud: statusCRUD}, &applyDesireCreator{crud: statusCRUD},
	)
	c := &ApplyDesireController{
		specFetcher: specFetcher,
		dyn:         dyn,
		writer:      writer,
	}

	key := mustKey(t, created)
	if err := c.SyncOnce(ctx, key); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	updated, err := statusCRUD.Get(ctx, created.DocumentID)
	if err != nil {
		t.Fatalf("get after sync: %v", err)
	}
	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected conditions to be set")
	}
	found := false
	for _, c := range updated.Status.Conditions {
		if c.Type == kubeapplier.ConditionTypeSuccessful && c.Status == metav1.ConditionTrue {
			found = true
		}
	}
	if !found {
		t.Errorf("expected Successful=True, conditions: %+v", updated.Status.Conditions)
	}
	if !updated.Status.ObservedDesireUpdateTime.Equal(created.GetUpdateTime()) {
		t.Errorf("ObservedDesireUpdateTime: got %v, want %v", updated.Status.ObservedDesireUpdateTime, created.GetUpdateTime())
	}
	if updated.Status.AppliedResourceGeneration != 3 {
		t.Errorf("AppliedResourceGeneration: got %d, want %d", updated.Status.AppliedResourceGeneration, 3)
	}
}

func TestSyncOnce_PreCheckError_SetsConditions(t *testing.T) {
	ctx := context.Background()

	specCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newApplyDesire(t, "cm1", kubeapplier.ResourceReference{Version: "v1", Resource: "configmaps"}, validConfigMapJSON("cm1"))
	created, err := specCRUD.Create(ctx, d)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	specFetcher := &applyDesireSpecFetcher{reader: specCRUD}
	statusFetcher := &applyDesireStatusFetcher{crud: statusCRUD}
	writer := desirestatuswriter.New[kubeapplier.ApplyDesire, keys.ApplyDesireKey, *kubeapplier.ApplyDesire](
		statusFetcher, &applyDesireReplacer{crud: statusCRUD}, &applyDesireCreator{crud: statusCRUD},
	)
	c := &ApplyDesireController{
		specFetcher: specFetcher,
		writer:      writer,
	}

	key := mustKey(t, created)
	// SyncOnce now returns the syncErr so processNext can apply exponential
	// backoff via AddRateLimited rather than silently cooling down.
	if err := c.SyncOnce(ctx, key); err == nil {
		t.Fatal("SyncOnce should return non-nil error for pre-check failure")
	}

	updated, err := statusCRUD.Get(ctx, created.DocumentID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	for _, c := range updated.Status.Conditions {
		if c.Type == kubeapplier.ConditionTypeSuccessful {
			if c.Status != metav1.ConditionFalse {
				t.Errorf("expected Successful=False, got %s", c.Status)
			}
			if c.Reason != kubeapplier.ConditionReasonPreCheckFailed {
				t.Errorf("expected reason PreCheckFailed, got %s", c.Reason)
			}
		}
		if c.Type == kubeapplier.ConditionTypeDegraded {
			if c.Status != metav1.ConditionFalse {
				t.Errorf("precheck should not degrade: got Degraded=%s", c.Status)
			}
		}
	}
}

// --- Cadence Tests ---

func TestHandleAdd_QueuesImmediately(t *testing.T) {
	c := newCadenceController(t, Config{})
	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	c.handleAdd(d)

	if c.queue.Len() != 1 {
		t.Errorf("expected 1 item in queue, got %d", c.queue.Len())
	}
}

func TestHandleUpdate_ChangedQueuesImmediately(t *testing.T) {
	c := newCadenceController(t, Config{})
	d1 := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	d1.SetUpdateTime(time.Unix(1, 0))
	d2 := d1.DeepCopy()
	d2.SetUpdateTime(time.Unix(2, 0))

	c.handleUpdate(d1, d2)

	if c.queue.Len() != 1 {
		t.Errorf("expected 1 item in queue, got %d", c.queue.Len())
	}
}

func TestHandleUpdate_UnchangedConsultsCooldown(t *testing.T) {
	now := time.Now()
	fakeClock := clocktesting.NewFakePassiveClock(now)
	c := newCadenceController(t, Config{
		CooldownPeriod: 10 * time.Minute,
		Clock:          fakeClock,
	})
	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))

	// First unchanged update: allowed (first call always allowed)
	c.handleUpdate(d, d)
	if c.queue.Len() != 1 {
		t.Fatalf("expected 1 item after first unchanged update, got %d", c.queue.Len())
	}
	// Drain queue
	key, _ := c.queue.Get()
	c.queue.Done(key)

	// Second unchanged update within cooldown: rejected
	fakeClock.SetTime(now.Add(5 * time.Minute))
	c.handleUpdate(d, d)
	if c.queue.Len() != 0 {
		t.Errorf("expected 0 items within cooldown, got %d", c.queue.Len())
	}

	// After cooldown: allowed again
	fakeClock.SetTime(now.Add(11 * time.Minute))
	c.handleUpdate(d, d)
	if c.queue.Len() != 1 {
		t.Errorf("expected 1 item after cooldown, got %d", c.queue.Len())
	}
}

func TestHandleUpdate_DeleteDesire_UsesDeleteCooldown(t *testing.T) {
	now := time.Now()
	fakeClock := clocktesting.NewFakePassiveClock(now)
	c := newCadenceController(t, Config{
		CooldownPeriod:       10 * time.Minute,
		DeleteCooldownPeriod: 1 * time.Minute,
		Clock:                fakeClock,
	})
	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))

	// First unchanged update: allowed
	c.handleUpdate(d, d)
	if c.queue.Len() != 1 {
		t.Fatalf("expected 1 item after first unchanged update, got %d", c.queue.Len())
	}
	key, _ := c.queue.Get()
	c.queue.Done(key)

	// Within 1-minute delete cooldown: rejected
	fakeClock.SetTime(now.Add(30 * time.Second))
	c.handleUpdate(d, d)
	if c.queue.Len() != 0 {
		t.Errorf("expected 0 items within delete cooldown, got %d", c.queue.Len())
	}

	// After 1-minute delete cooldown: allowed (at 2 minutes, well after the 1-min delete cooldown)
	fakeClock.SetTime(now.Add(2 * time.Minute))
	c.handleUpdate(d, d)
	if c.queue.Len() != 1 {
		t.Errorf("expected 1 item after delete cooldown, got %d", c.queue.Len())
	}
}

func TestHandleAdd_InvalidDesireType_NoQueue(t *testing.T) {
	c := newCadenceController(t, Config{})
	c.handleAdd("not a desire")
	if c.queue.Len() != 0 {
		t.Errorf("expected 0 items, got %d", c.queue.Len())
	}
}

// --- ProcessNext Tests ---

func TestProcessNext_ErrorRequeues(t *testing.T) {
	// Suppress utilruntime error logging during test
	saved := utilruntime.ErrorHandlers
	utilruntime.ErrorHandlers = nil
	defer func() { utilruntime.ErrorHandlers = saved }()

	fetcher := &errFetcher{err: errors.New("fetch failed")}
	// Use a zero-delay rate limiter so AddRateLimited is immediate.
	c := &ApplyDesireController{
		specFetcher: fetcher,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedMaxOfRateLimiter[keys.ApplyDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ApplyDesireKey]{Name: "test"},
		),
	}

	key := keys.ApplyDesireKey{ClusterID: "c1", Name: "cluster1--cm1"}
	c.queue.Add(key)

	ctx := context.Background()
	c.processNext(ctx)

	// AddRateLimited with zero-delay limiter should make the key available
	// immediately (or near-immediately). Give it a moment to land.
	time.Sleep(10 * time.Millisecond)
	if c.queue.Len() != 1 {
		t.Errorf("expected key to be requeued, queue len: %d", c.queue.Len())
	}
}

// --- FieldManager Test ---

func TestFieldManager_IsGCPHCP(t *testing.T) {
	if FieldManager != "gcp-hcp-kube-applier" {
		t.Errorf("expected gcp-hcp-kube-applier, got %s", FieldManager)
	}
	if !strings.HasPrefix(FieldManager, "gcp-hcp-") {
		t.Error("FieldManager must have gcp-hcp- prefix")
	}
}

// --- SSA with kube error wrapping Test ---

func TestApplyDesired_KubeError_WrapsWithContext(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "ConfigMapList"})
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})

	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	_, err := c.applyDesired(ctx, d)

	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "server-side apply") {
		t.Errorf("expected 'server-side apply' prefix, got: %v", err)
	}
}

// --- Constant Tests ---

func TestDefaultCooldownPeriod_IsTenMinutes(t *testing.T) {
	if DefaultCooldownPeriod != 10*time.Minute {
		t.Errorf("expected 10m, got %v", DefaultCooldownPeriod)
	}
}

func TestDefaultDeleteCooldownPeriod_IsOneMinute(t *testing.T) {
	if DefaultDeleteCooldownPeriod != 1*time.Minute {
		t.Errorf("expected 1m, got %v", DefaultDeleteCooldownPeriod)
	}
}

// =============================================================================
// evaluateDelete() State Machine Tests
// =============================================================================

func TestEvaluateDelete_TargetAbsent(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)

	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue, "")
}

func TestEvaluateDelete_Target404Race(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"}, existingConfigMap("cm1"))

	callCount := 0
	dyn.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "cm1")
	})
	dyn.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		callCount++
		if callCount == 1 {
			return true, existingConfigMap("cm1"), nil
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "cm1")
	})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)
	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue, "")
}

func TestEvaluateDelete_AlreadyTerminating(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})
	dyn.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, terminatingConfigMap("cm1"), nil
	})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)
	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionFalse, kubeapplier.ConditionReasonWaitingForDeletion)
}

func TestEvaluateDelete_DeleteSucceeds_Terminating(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})

	getCount := 0
	dyn.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getCount++
		if getCount == 1 {
			return true, existingConfigMap("cm1"), nil
		}
		return true, terminatingConfigMap("cm1"), nil
	})
	dyn.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)
	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionFalse, kubeapplier.ConditionReasonWaitingForDeletion)
	assertConditionMessage(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, "test-uid-123")
}

func TestEvaluateDelete_DeleteSucceeds_ImmediateGone(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})

	getCount := 0
	dyn.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getCount++
		if getCount == 1 {
			return true, existingConfigMap("cm1"), nil
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "cm1")
	})
	dyn.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)
	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue, "")
}

func TestEvaluateDelete_DeleteReturns500(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})
	dyn.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, existingConfigMap("cm1"), nil
	})
	dyn.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure, Code: http.StatusInternalServerError,
			Reason: metav1.StatusReasonInternalError, Message: "internal",
		}}
	})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)
	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionFalse, kubeapplier.ConditionReasonKubeAPIError)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeDegraded, metav1.ConditionTrue, "")
}

func TestEvaluateDelete_GetReturns500(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})
	dyn.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure, Code: http.StatusInternalServerError,
			Reason: metav1.StatusReasonInternalError, Message: "internal",
		}}
	})

	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	c := &ApplyDesireController{dyn: dyn}
	mutate := c.evaluateDelete(ctx, d)
	mutate(d)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionFalse, kubeapplier.ConditionReasonKubeAPIError)
	assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeDegraded, metav1.ConditionTrue, "")
}

func TestEvaluateDelete_PreCheck_MissingFields(t *testing.T) {
	ctx := context.Background()
	c := &ApplyDesireController{}
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
			d := newDeleteApplyDesire(t, "cm1", tt.target)
			mutate := c.evaluateDelete(ctx, d)
			mutate(d)
			assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionFalse, kubeapplier.ConditionReasonPreCheckFailed)
			assertCondition(t, d.Status.Conditions, kubeapplier.ConditionTypeDegraded, metav1.ConditionFalse, "")
		})
	}
}

func TestEvaluateDelete_UnknownType_PreCheckFailed(t *testing.T) {
	ctx := context.Background()

	specCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := &kubeapplier.ApplyDesire{}
	d.SetDocumentID("cluster1--cm1")
	d.SetUpdateTime(time.Unix(1, 0))
	d.Spec = kubeapplier.ApplyDesireSpec{
		ManagementCluster: "mc-1",
		ClusterID:         "cluster1",
		Type:              "Bogus",
		TargetItem:        configMapTarget("cm1"),
	}
	created, err := specCRUD.Create(ctx, d)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	specFetcher := &applyDesireSpecFetcher{reader: specCRUD}
	statusFetcher := &applyDesireStatusFetcher{crud: statusCRUD}
	writer := desirestatuswriter.New[kubeapplier.ApplyDesire, keys.ApplyDesireKey, *kubeapplier.ApplyDesire](
		statusFetcher, &applyDesireReplacer{crud: statusCRUD}, &applyDesireCreator{crud: statusCRUD},
	)
	c := &ApplyDesireController{
		specFetcher: specFetcher,
		writer:      writer,
	}

	key := mustKey(t, created)
	// SyncOnce now returns the syncErr so processNext can apply exponential
	// backoff via AddRateLimited rather than silently cooling down.
	if err := c.SyncOnce(ctx, key); err == nil {
		t.Fatal("SyncOnce should return non-nil error for unknown desire type")
	}

	updated, err := statusCRUD.Get(ctx, created.DocumentID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertCondition(t, updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionFalse, kubeapplier.ConditionReasonPreCheckFailed)
}

func TestSyncOnce_DeleteDesire_TargetAbsent_Successful(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{configMapGVR: "ConfigMapList"})

	specCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newDeleteApplyDesire(t, "cm1", configMapTarget("cm1"))
	created, err := specCRUD.Create(ctx, d)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	specFetcher := &applyDesireSpecFetcher{reader: specCRUD}
	statusFetcher := &applyDesireStatusFetcher{crud: statusCRUD}
	writer := desirestatuswriter.New[kubeapplier.ApplyDesire, keys.ApplyDesireKey, *kubeapplier.ApplyDesire](
		statusFetcher, &applyDesireReplacer{crud: statusCRUD}, &applyDesireCreator{crud: statusCRUD},
	)
	c := &ApplyDesireController{
		specFetcher: specFetcher,
		dyn:         dyn,
		writer:      writer,
	}

	key := mustKey(t, created)
	if err := c.SyncOnce(ctx, key); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	updated, err := statusCRUD.Get(ctx, created.DocumentID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertCondition(t, updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful, metav1.ConditionTrue, "")
	if !updated.Status.ObservedDesireUpdateTime.Equal(created.GetUpdateTime()) {
		t.Errorf("ObservedDesireUpdateTime: got %v, want %v", updated.Status.ObservedDesireUpdateTime, created.GetUpdateTime())
	}
}

// suppress unused import
var _ = database.IsNotFoundError
var _ = unstructured.UnstructuredList{}
var _ = types.UID("")

// --- handleDelete Tests ---

func TestHandleDelete_DeletesStatusRecord(t *testing.T) {
	ctx := context.Background()

	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()

	// Pre-populate a status record for the desire.
	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	if _, err := statusCRUD.Create(ctx, d); err != nil {
		t.Fatalf("create status: %v", err)
	}

	c := &ApplyDesireController{statusCRUD: statusCRUD}
	c.handleDelete(d)

	// Status record should now be gone.
	if _, err := statusCRUD.Get(ctx, d.GetDocumentID()); !database.IsNotFoundError(err) {
		t.Errorf("expected status record to be deleted, got err: %v", err)
	}
}

func TestHandleDelete_StatusAlreadyGone_NoError(t *testing.T) {
	// If the status record doesn't exist, handleDelete should not panic or log an error.
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	c := &ApplyDesireController{statusCRUD: statusCRUD}

	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	// Should not panic.
	c.handleDelete(d)
}

func TestHandleDelete_Tombstone_DeletesStatusRecord(t *testing.T) {
	ctx := context.Background()

	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newApplyDesire(t, "cm1", configMapTarget("cm1"), validConfigMapJSON("cm1"))
	if _, err := statusCRUD.Create(ctx, d); err != nil {
		t.Fatalf("create status: %v", err)
	}

	c := &ApplyDesireController{statusCRUD: statusCRUD}

	// Simulate the informer wrapping the object in a DeletedFinalStateUnknown tombstone.
	tombstone := cache.DeletedFinalStateUnknown{Key: d.GetDocumentID(), Obj: d}
	c.handleDelete(tombstone)

	if _, err := statusCRUD.Get(ctx, d.GetDocumentID()); !database.IsNotFoundError(err) {
		t.Errorf("expected status record to be deleted via tombstone, got err: %v", err)
	}
}

func TestHandleDelete_InvalidType_NoOp(t *testing.T) {
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	c := &ApplyDesireController{statusCRUD: statusCRUD}
	// Should not panic.
	c.handleDelete("not a desire")
}
