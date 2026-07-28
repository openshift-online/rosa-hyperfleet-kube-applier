// Package read_desire_kubernetes implements the per-ReadDesire kubernetes
// reflector. One instance is created for each ReadDesire by the manager
// (see ../read_desire_manager). It list/watches a single named object via
// the dynamic client and mirrors its observed state into the ReadDesire's
// .status.kubeContent in the status database.
package read_desire_kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/conditions"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/desirestatuswriter"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/keys"
)

// ResyncDuration is how often a ReadDesireKubernetesController re-evaluates
// even without a fresh kube event, so a missing target object can be reflected
// into status.
const ResyncDuration = 60 * time.Second

// listWatchWithoutWatchListSemantics opts out of the WatchList streaming mode
// enabled by default in client-go v0.35+. The dynamic client's Watch does not
// emit bookmark events that WatchList requires, so the reflector would never
// reach Synced without this wrapper.
type listWatchWithoutWatchListSemantics struct {
	*cache.ListWatch
}

func (listWatchWithoutWatchListSemantics) IsWatchListSemanticsUnSupported() bool { return true }

// ReadDesireKubernetesController reflects a single named kube object into a
// ReadDesire's status. One instance per ReadDesire is owned by the manager.
type ReadDesireKubernetesController struct {
	key            keys.ReadDesireKey
	target         kubeapplier.ResourceReference
	specUpdateTime time.Time
	gvr            schema.GroupVersionResource
	namespaced     bool

	dyn      dynamic.Interface
	informer cache.SharedIndexInformer
	fetcher  *readDesireStatusFetcher
	writer   desirestatuswriter.StatusWriter[kubeapplier.ReadDesire, keys.ReadDesireKey]

	queue workqueue.TypedRateLimitingInterface[keys.ReadDesireKey]
}

// NewReadDesireKubernetesController constructs a per-ReadDesire kubernetes
// reflector. It builds a single-object ListWatch so the per-instance informer
// touches only the named object.
func NewReadDesireKubernetesController(
	key keys.ReadDesireKey,
	target kubeapplier.ResourceReference,
	specUpdateTime time.Time,
	dyn dynamic.Interface,
	statusCRUD database.ResourceCRUD[kubeapplier.ReadDesire],
) (*ReadDesireKubernetesController, error) {
	if len(target.Resource) == 0 || len(target.Version) == 0 || len(target.Name) == 0 {
		return nil, conditions.NewPreCheckError(errors.New("spec.targetItem requires version, resource, and name"))
	}

	fetcher := &readDesireStatusFetcher{crud: statusCRUD}
	c := &ReadDesireKubernetesController{
		key:            key,
		target:         target,
		specUpdateTime: specUpdateTime,
		gvr: schema.GroupVersionResource{
			Group: target.Group, Version: target.Version, Resource: target.Resource,
		},
		namespaced: len(target.Namespace) > 0,
		dyn:        dyn,
		fetcher:    fetcher,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.ReadDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ReadDesireKey]{
				Name: fmt.Sprintf("ReadDesireKubernetesController_%s_%s_%s", key.ClusterID, key.NodePoolName, key.Name),
			},
		),
		writer: desirestatuswriter.New[kubeapplier.ReadDesire, keys.ReadDesireKey, *kubeapplier.ReadDesire](
			fetcher,
			&readDesireReplacer{crud: statusCRUD},
			&readDesireCreator{crud: statusCRUD},
		),
	}

	c.informer = cache.NewSharedIndexInformerWithOptions(
		&listWatchWithoutWatchListSemantics{ListWatch: c.singleObjectListWatch()},
		&unstructured.Unstructured{},
		cache.SharedIndexInformerOptions{ResyncPeriod: ResyncDuration},
	)

	if _, err := c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.queue.Add(c.key) },
		UpdateFunc: func(_, _ any) { c.queue.Add(c.key) },
		DeleteFunc: func(obj any) { c.queue.Add(c.key) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

// Run starts the per-instance informer and worker. It blocks until ctx is canceled.
func (c *ReadDesireKubernetesController) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx).WithValues("readDesire", c.key.Name, "cluster", c.key.ClusterID)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("starting ReadDesireKubernetesController")
	defer logger.Info("stopped ReadDesireKubernetesController")

	go c.informer.RunWithContext(ctx)

	if !cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced) {
		logger.Info("per-instance informer cache failed to sync; exiting controller",
			"gvr", c.gvr.String(),
			"namespace", c.target.Namespace,
			"name", c.target.Name)
		return
	}

	ticker := time.NewTicker(ResyncDuration)
	defer ticker.Stop()
	go func() {
		defer utilruntime.HandleCrash()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.queue.Add(c.key)
			}
		}
	}()

	c.queue.Add(c.key)

	go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	<-ctx.Done()
}

func (c *ReadDesireKubernetesController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *ReadDesireKubernetesController) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)
	if err := c.SyncOnce(ctx); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "sync error; requeuing", "key", key)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

// SyncOnce reads the live object from the per-instance informer cache and
// updates the ReadDesire's status if its KubeContent differs.
func (c *ReadDesireKubernetesController) SyncOnce(ctx context.Context) error {
	if !c.informer.HasSynced() {
		klog.FromContext(ctx).Info("per-instance informer not yet synced; skipping",
			"gvr", c.gvr.String(),
			"namespace", c.target.Namespace,
			"name", c.target.Name)
		return nil
	}

	desire, err := c.fetcher.Fetch(ctx, c.key)
	if database.IsNotFoundError(err) {
		desire = nil
	} else if err != nil {
		return err
	}

	storeKey := c.target.Name
	if c.namespaced {
		storeKey = c.target.Namespace + "/" + c.target.Name
	}
	rawObj, exists, err := c.informer.GetStore().GetByKey(storeKey)
	if err != nil {
		return c.writer.UpdateStatus(ctx, c.key, func(d *kubeapplier.ReadDesire) {
			d.SetDocumentID(c.key.Name)
			d.Status.ObservedDesireUpdateTime = c.specUpdateTime
			conditions.SetSuccessful(&d.Status.Conditions, fmt.Errorf("read cache: %w", err))
		})
	}

	var newRaw []byte
	if exists {
		obj, ok := rawObj.(*unstructured.Unstructured)
		if !ok {
			return c.writer.UpdateStatus(ctx, c.key, func(d *kubeapplier.ReadDesire) {
				d.SetDocumentID(c.key.Name)
				d.Status.ObservedDesireUpdateTime = c.specUpdateTime
				conditions.SetSuccessful(&d.Status.Conditions, conditions.NewPreCheckError(
					fmt.Errorf("informer cached unexpected type %T", rawObj)))
			})
		}
		newRaw, err = json.Marshal(obj)
		if err != nil {
			return c.writer.UpdateStatus(ctx, c.key, func(d *kubeapplier.ReadDesire) {
				d.SetDocumentID(c.key.Name)
				d.Status.ObservedDesireUpdateTime = c.specUpdateTime
				conditions.SetSuccessful(&d.Status.Conditions, fmt.Errorf("marshal observed object: %w", err))
			})
		}
	}

	var existingRaw []byte
	if desire != nil && desire.Status.KubeContent != nil {
		existingRaw = desire.Status.KubeContent.Raw
	}
	if bytes.Equal(newRaw, existingRaw) {
		return c.writer.UpdateStatus(ctx, c.key, func(d *kubeapplier.ReadDesire) {
			d.SetDocumentID(c.key.Name)
			d.Status.ObservedDesireUpdateTime = c.specUpdateTime
			conditions.SetSuccessful(&d.Status.Conditions, nil)
		})
	}

	return c.writer.UpdateStatus(ctx, c.key, func(d *kubeapplier.ReadDesire) {
		d.SetDocumentID(c.key.Name)
		d.Status.ObservedDesireUpdateTime = c.specUpdateTime
		if newRaw == nil {
			d.Status.KubeContent = nil
		} else {
			d.Status.KubeContent = &runtime.RawExtension{Raw: append([]byte(nil), newRaw...)}
		}
		conditions.SetSuccessful(&d.Status.Conditions, nil)
	})
}

func (c *ReadDesireKubernetesController) singleObjectListWatch() *cache.ListWatch {
	resource := c.dyn.Resource(c.gvr)
	var kubeResourceAccessor dynamic.ResourceInterface = resource
	if c.namespaced {
		kubeResourceAccessor = resource.Namespace(c.target.Namespace)
	}
	fieldSelector := metav1.SingleObject(metav1.ObjectMeta{Name: c.target.Name}).FieldSelector
	return &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return kubeResourceAccessor.List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return kubeResourceAccessor.Watch(ctx, options)
		},
	}
}

type readDesireStatusFetcher struct {
	crud database.ResourceCRUD[kubeapplier.ReadDesire]
}

var _ desirestatuswriter.Fetcher[kubeapplier.ReadDesire, keys.ReadDesireKey] = &readDesireStatusFetcher{}

func (f *readDesireStatusFetcher) Fetch(ctx context.Context, key keys.ReadDesireKey) (*kubeapplier.ReadDesire, error) {
	return f.crud.Get(ctx, key.Name)
}

type readDesireReplacer struct {
	crud database.ResourceCRUD[kubeapplier.ReadDesire]
}

var _ desirestatuswriter.Replacer[kubeapplier.ReadDesire] = &readDesireReplacer{}

func (r *readDesireReplacer) Replace(ctx context.Context, desired *kubeapplier.ReadDesire) error {
	_, err := r.crud.Replace(ctx, desired)
	return err
}

type readDesireCreator struct {
	crud database.ResourceCRUD[kubeapplier.ReadDesire]
}

var _ desirestatuswriter.Creator[kubeapplier.ReadDesire] = &readDesireCreator{}

func (c *readDesireCreator) Create(ctx context.Context, obj *kubeapplier.ReadDesire) error {
	_, err := c.crud.Create(ctx, obj)
	return err
}
