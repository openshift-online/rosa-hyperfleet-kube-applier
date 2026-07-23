// Package read_desire_manager implements the ReadDesireInformerManagingController.
//
// It watches the ReadDesire informer and, for every key, owns the lifecycle of a
// per-ReadDesire ReadDesireKubernetesController. When a ReadDesire's TargetItem
// changes, the manager stops the old per-instance controller (waiting for its
// goroutine to exit) and starts a fresh one.
package read_desire_manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	utilsclock "k8s.io/utils/clock"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	controllerutil "github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/controllerutils"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/conditions"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/desirestatuswriter"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/keys"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/read_desire_kubernetes"
)

// DefaultCooldownPeriod is the minimum interval between two reconciles of a
// ReadDesire whose Firestore UpdateTime has not changed.
const DefaultCooldownPeriod = 10 * time.Minute

type Config struct {
	CooldownPeriod time.Duration
	Clock          utilsclock.PassiveClock
}

func (c Config) withDefaults() Config {
	if c.CooldownPeriod == 0 {
		c.CooldownPeriod = DefaultCooldownPeriod
	}
	if c.Clock == nil {
		c.Clock = utilsclock.RealClock{}
	}
	return c
}

// PerInstanceController abstracts the per-ReadDesire kube reflector so the
// manager can be tested with a fake.
type PerInstanceController interface {
	Run(ctx context.Context)
}

// PerInstanceFactory builds a per-ReadDesire controller.
type PerInstanceFactory interface {
	Build(key keys.ReadDesireKey, target kubeapplier.ResourceReference, specUpdateTime time.Time) (PerInstanceController, error)
}

// ReadDesireInformerManagingController watches ReadDesires and manages the
// per-instance kubernetes reflectors.
type ReadDesireInformerManagingController struct {
	readDesireInformer cache.SharedIndexInformer
	specFetcher        *readDesireSpecFetcher
	factory            PerInstanceFactory
	writer             desirestatuswriter.StatusWriter[kubeapplier.ReadDesire, keys.ReadDesireKey]
	statusCRUD         database.ResourceCRUD[kubeapplier.ReadDesire]
	queue              workqueue.TypedRateLimitingInterface[keys.ReadDesireKey]

	cfg      Config
	cooldown controllerutil.CooldownChecker

	mu      sync.Mutex
	running map[keys.ReadDesireKey]*runningInstance
}

type runningInstance struct {
	target kubeapplier.ResourceReference
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReadDesireInformerManagingController constructs a manager that uses the
// supplied dynamic client for every per-instance controller it spawns.
func NewReadDesireInformerManagingController(
	readDesireInformer cache.SharedIndexInformer,
	dyn dynamic.Interface,
	specReader database.SpecReader[kubeapplier.ReadDesire],
	statusCRUD database.ResourceCRUD[kubeapplier.ReadDesire],
	cfg Config,
) (*ReadDesireInformerManagingController, error) {
	cfg = cfg.withDefaults()
	specFetcher := &readDesireSpecFetcher{reader: specReader}
	statusFetcher := &readDesireStatusFetcher{crud: statusCRUD}
	cooldownChecker := controllerutil.NewTimeBasedCooldownChecker(cfg.CooldownPeriod)
	cooldownChecker.SetClock(cfg.Clock)
	c := &ReadDesireInformerManagingController{
		readDesireInformer: readDesireInformer,
		specFetcher:        specFetcher,
		factory:            &realPerInstanceFactory{dyn: dyn, statusCRUD: statusCRUD},
		statusCRUD:         statusCRUD,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.ReadDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ReadDesireKey]{Name: "ReadDesireInformerManagingController"},
		),
		writer: desirestatuswriter.New[kubeapplier.ReadDesire, keys.ReadDesireKey, *kubeapplier.ReadDesire](
			statusFetcher,
			&readDesireReplacer{crud: statusCRUD},
			&readDesireCreator{crud: statusCRUD},
		),
		cfg:      cfg,
		cooldown: cooldownChecker,
		running:  map[keys.ReadDesireKey]*runningInstance{},
	}

	if _, err := readDesireInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.handleAdd(obj) },
		UpdateFunc: func(oldObj, newObj any) { c.handleUpdate(oldObj, newObj) },
		DeleteFunc: func(obj any) { c.handleDelete(obj) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

// SetFactory swaps the per-instance controller factory. Intended for tests.
func (c *ReadDesireInformerManagingController) SetFactory(f PerInstanceFactory) { c.factory = f }

type realPerInstanceFactory struct {
	dyn        dynamic.Interface
	statusCRUD database.ResourceCRUD[kubeapplier.ReadDesire]
}

var _ PerInstanceFactory = &realPerInstanceFactory{}

func (f *realPerInstanceFactory) Build(
	key keys.ReadDesireKey, target kubeapplier.ResourceReference, specUpdateTime time.Time,
) (PerInstanceController, error) {
	return read_desire_kubernetes.NewReadDesireKubernetesController(key, target, specUpdateTime, f.dyn, f.statusCRUD)
}

func (c *ReadDesireInformerManagingController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	defer c.stopAll()

	logger := klog.FromContext(ctx).WithName("ReadDesireInformerManagingController")
	ctx = klog.NewContext(ctx, logger)
	logger.Info("starting ReadDesireInformerManagingController")
	defer logger.Info("stopped ReadDesireInformerManagingController")

	if threadiness < 1 {
		threadiness = 1
	}
	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	<-ctx.Done()
}

func (c *ReadDesireInformerManagingController) handleAdd(obj any) {
	d, ok := obj.(*kubeapplier.ReadDesire)
	if !ok {
		return
	}
	c.enqueue(d)
}

func (c *ReadDesireInformerManagingController) handleUpdate(oldObj, newObj any) {
	oldD, oldOK := oldObj.(*kubeapplier.ReadDesire)
	newD, newOK := newObj.(*kubeapplier.ReadDesire)
	if !oldOK || !newOK {
		return
	}
	if !oldD.UpdateTime.Equal(newD.UpdateTime) {
		c.enqueue(newD)
		return
	}
	key, err := keys.ReadDesireKeyFromDesire(newD)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if !c.cooldown.CanSync(context.TODO(), key) {
		return
	}
	c.queue.Add(key)
}

func (c *ReadDesireInformerManagingController) handleDelete(obj any) {
	d, ok := obj.(*kubeapplier.ReadDesire)
	if !ok {
		if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			d, _ = t.Obj.(*kubeapplier.ReadDesire)
		}
	}
	if d == nil {
		return
	}
	if err := c.statusCRUD.Delete(context.Background(), d.GetDocumentID()); err != nil && !database.IsNotFoundError(err) {
		klog.ErrorS(err, "failed to delete status record for removed read desire spec", "documentID", d.GetDocumentID())
	}
	c.stopByKey(keys.ReadDesireKey{ClusterID: d.Spec.ClusterID, Name: d.GetDocumentID()})
}

func (c *ReadDesireInformerManagingController) enqueue(d *kubeapplier.ReadDesire) {
	key, err := keys.ReadDesireKeyFromDesire(d)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *ReadDesireInformerManagingController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *ReadDesireInformerManagingController) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)
	if err := c.SyncOnce(ctx, key); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "sync error; requeuing", "key", key)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	// Safety-net: re-enqueue after 5 minutes so that any spec change missed
	// during downtime is eventually reconciled.
	c.queue.AddAfter(key, 5*time.Minute)
	return true
}

// EnqueueByDocumentID enqueues a ReadDesire for reconciliation by its raw
// document ID. It is called by the SQS poller when a spec change notification
// arrives for the read-desires table.
func (c *ReadDesireInformerManagingController) EnqueueByDocumentID(documentID string) {
	key, err := keys.ReadDesireKeyFromDocumentID(documentID)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

// SyncOnce reconciles one ReadDesire by ensuring its per-instance controller
// is running with the desired TargetItem.
func (c *ReadDesireInformerManagingController) SyncOnce(ctx context.Context, key keys.ReadDesireKey) error {
	desire, err := c.specFetcher.Fetch(ctx, key)
	if err != nil && !database.IsNotFoundError(err) {
		return err
	}
	if desire == nil {
		c.stopByKey(key)
		return nil
	}

	c.mu.Lock()
	cur, exists := c.running[key]
	c.mu.Unlock()

	target := desire.Spec.TargetItem
	if exists && cur.target == target {
		return nil
	}
	if exists {
		c.stopByKey(key)
	}

	per, err := c.factory.Build(key, target, desire.GetUpdateTime())
	if err != nil {
		return c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ReadDesire) {
			d.SetDocumentID(desire.GetDocumentID())
			d.Status.ObservedDesireUpdateTime = desire.GetUpdateTime()
			conditions.SetSuccessful(&d.Status.Conditions, err)
		})
	}

	childCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.mu.Lock()
	c.running[key] = &runningInstance{target: target, cancel: cancel, done: done}
	c.mu.Unlock()

	go func() {
		defer utilruntime.HandleCrash()
		defer close(done)
		per.Run(childCtx)
	}()

	return nil
}

func (c *ReadDesireInformerManagingController) stopByKey(key keys.ReadDesireKey) {
	c.mu.Lock()
	cur, ok := c.running[key]
	if ok {
		delete(c.running, key)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	cur.cancel()
	<-cur.done
}

func (c *ReadDesireInformerManagingController) stopAll() {
	c.mu.Lock()
	allKeys := make([]keys.ReadDesireKey, 0, len(c.running))
	for k := range c.running {
		allKeys = append(allKeys, k)
	}
	c.mu.Unlock()
	for _, k := range allKeys {
		c.stopByKey(k)
	}
}

// Running returns true when key has a per-instance controller in flight. Test-only.
func (c *ReadDesireInformerManagingController) Running(key keys.ReadDesireKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.running[key]
	return ok
}

type readDesireSpecFetcher struct {
	reader database.SpecReader[kubeapplier.ReadDesire]
}

var _ desirestatuswriter.Fetcher[kubeapplier.ReadDesire, keys.ReadDesireKey] = &readDesireSpecFetcher{}

func (f *readDesireSpecFetcher) Fetch(ctx context.Context, key keys.ReadDesireKey) (*kubeapplier.ReadDesire, error) {
	return f.reader.Get(ctx, key.Name)
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
