// Package apply_desire implements the ApplyDesireController.
//
// The controller handles two operation types via the ApplyDesire.Spec.Type
// discriminator:
//
//   - Type=ServerSideApply: decodes spec.serverSideApply.kubeContent into an
//     unstructured object and issues a server-side-apply with Force=true and
//     FieldManager from this package's FieldManager const via the dynamic
//     client. The outcome is recorded on .status.conditions["Successful"] /
//     ["Degraded"] and persisted to the status database via the StatusWriter.
//
//   - Type=Delete: resolves spec.targetItem to a GVR, gets the object, and
//     either reports Successful=True if the object is gone, reports
//     Successful=False (WaitingForDeletion) if it's there and has a
//     deletionTimestamp (or after issuing a delete that succeeded but the
//     object hasn't fully gone away yet because of finalizers), or issues a
//     delete and re-checks the same way. A shorter deleteCooldown (1 minute)
//     is used for this type to keep polling for finalizer completion.
package apply_desire

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	utilsclock "k8s.io/utils/clock"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/controllerutils"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/conditions"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/desirestatuswriter"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/controllers/keys"
)

const FieldManager = "gcp-hcp-kube-applier"

// DefaultCooldownPeriod is the minimum interval between two reconciles
// of an unchanged ApplyDesire with Type=ServerSideApply. Real content
// changes — Add events and Update events with a different UpdateTime —
// bypass this gate.
const DefaultCooldownPeriod = 10 * time.Minute

// DefaultDeleteCooldownPeriod is the minimum interval between two reconciles
// of an unchanged ApplyDesire with Type=Delete. 1 minute is shorter than
// the SSA default because a delete desire in the WaitingForDeletion state
// needs to keep checking whether finalizers have completed.
const DefaultDeleteCooldownPeriod = 1 * time.Minute

// Config tunes the ApplyDesireController's cooldown behavior. Zero-valued
// fields take the Default* constants; tests pass shorter durations and a
// fake clock.
type Config struct {
	CooldownPeriod       time.Duration
	DeleteCooldownPeriod time.Duration
	Clock                utilsclock.PassiveClock
}

func (c Config) withDefaults() Config {
	if c.CooldownPeriod == 0 {
		c.CooldownPeriod = DefaultCooldownPeriod
	}
	if c.DeleteCooldownPeriod == 0 {
		c.DeleteCooldownPeriod = DefaultDeleteCooldownPeriod
	}
	if c.Clock == nil {
		c.Clock = utilsclock.RealClock{}
	}
	return c
}

// ApplyDesireController reconciles ApplyDesires by SSA-applying
// spec.serverSideApply.kubeContent (Type=ServerSideApply) or deleting
// spec.targetItem (Type=Delete).
type ApplyDesireController struct {
	name                string
	applyDesireInformer cache.SharedIndexInformer
	specFetcher         desirestatuswriter.Fetcher[kubeapplier.ApplyDesire, keys.ApplyDesireKey]
	dyn                 dynamic.Interface
	writer              desirestatuswriter.StatusWriter[kubeapplier.ApplyDesire, keys.ApplyDesireKey]
	statusCRUD          database.ResourceCRUD[kubeapplier.ApplyDesire]
	queue               workqueue.TypedRateLimitingInterface[keys.ApplyDesireKey]

	cfg            Config
	cooldown       controllerutils.CooldownChecker
	deleteCooldown controllerutils.CooldownChecker
}

// NewApplyDesireController wires up the informer event handler and returns a
// ready-to-Run controller.
func NewApplyDesireController(
	applyDesireInformer cache.SharedIndexInformer,
	dyn dynamic.Interface,
	specReader database.SpecReader[kubeapplier.ApplyDesire],
	statusCRUD database.ResourceCRUD[kubeapplier.ApplyDesire],
	cfg Config,
) (*ApplyDesireController, error) {
	cfg = cfg.withDefaults()
	specFetcher := &applyDesireSpecFetcher{reader: specReader}
	statusFetcher := &applyDesireStatusFetcher{crud: statusCRUD}
	cooldownChecker := controllerutils.NewTimeBasedCooldownChecker(cfg.CooldownPeriod)
	cooldownChecker.SetClock(cfg.Clock)
	deleteCooldownChecker := controllerutils.NewTimeBasedCooldownChecker(cfg.DeleteCooldownPeriod)
	deleteCooldownChecker.SetClock(cfg.Clock)
	c := &ApplyDesireController{
		name:                "ApplyDesireController",
		applyDesireInformer: applyDesireInformer,
		specFetcher:         specFetcher,
		dyn:                 dyn,
		statusCRUD:          statusCRUD,
		writer: desirestatuswriter.New[kubeapplier.ApplyDesire, keys.ApplyDesireKey, *kubeapplier.ApplyDesire](
			statusFetcher,
			&applyDesireReplacer{crud: statusCRUD},
			&applyDesireCreator{crud: statusCRUD},
		),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.ApplyDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.ApplyDesireKey]{Name: "ApplyDesireController"},
		),
		cfg:            cfg,
		cooldown:       cooldownChecker,
		deleteCooldown: deleteCooldownChecker,
	}

	if _, err := applyDesireInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.handleAdd(obj) },
		UpdateFunc: func(oldObj, newObj any) { c.handleUpdate(oldObj, newObj) },
		DeleteFunc: func(obj any) { c.handleDelete(obj) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

// Run starts threadiness workers. It returns when ctx is canceled.
func (c *ApplyDesireController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx).WithName(c.name)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("starting controller")
	defer logger.Info("stopped controller")

	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	<-ctx.Done()
}

func (c *ApplyDesireController) handleAdd(obj any) {
	d, ok := obj.(*kubeapplier.ApplyDesire)
	if !ok {
		return
	}
	c.enqueue(d)
}

func (c *ApplyDesireController) handleUpdate(oldObj, newObj any) {
	oldD, oldOK := oldObj.(*kubeapplier.ApplyDesire)
	newD, newOK := newObj.(*kubeapplier.ApplyDesire)
	if !oldOK || !newOK {
		return
	}
	changed := !oldD.UpdateTime.Equal(newD.UpdateTime)
	c.enqueueWithCooldown(newD, changed)
}

func (c *ApplyDesireController) handleDelete(obj any) {
	d, ok := obj.(*kubeapplier.ApplyDesire)
	if !ok {
		// obj may be a DeletedFinalStateUnknown tombstone.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		d, ok = tombstone.Obj.(*kubeapplier.ApplyDesire)
		if !ok {
			return
		}
	}
	if err := c.statusCRUD.Delete(context.Background(), d.GetDocumentID()); err != nil && !database.IsNotFoundError(err) {
		klog.ErrorS(err, "stream watcher failed to delete status record for removed spec", "documentID", d.GetDocumentID())
	}
}

func (c *ApplyDesireController) enqueue(d *kubeapplier.ApplyDesire) {
	key, err := keys.ApplyDesireKeyFromDesire(d)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *ApplyDesireController) enqueueWithCooldown(d *kubeapplier.ApplyDesire, changed bool) {
	key, err := keys.ApplyDesireKeyFromDesire(d)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if changed {
		c.queue.Add(key)
		return
	}
	// Delete desires need a shorter cooldown to poll for finalizer completion.
	cd := c.cooldown
	if d.Spec.Type == kubeapplier.ApplyDesireTypeDelete {
		cd = c.deleteCooldown
	}
	if !cd.CanSync(context.TODO(), key) {
		return
	}
	c.queue.Add(key)
}

func (c *ApplyDesireController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *ApplyDesireController) processNext(ctx context.Context) bool {
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
	return true
}

// SyncOnce performs a single reconcile pass for the named ApplyDesire.
func (c *ApplyDesireController) SyncOnce(ctx context.Context, key keys.ApplyDesireKey) error {
	desire, err := c.specFetcher.Fetch(ctx, key)
	if database.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if desire == nil {
		return nil
	}

	switch desire.Spec.Type {
	case kubeapplier.ApplyDesireTypeServerSideApply, "":
		// Empty type is treated as ServerSideApply for backward compatibility
		// with documents written before the Type field was introduced.
		appliedGen, syncErr := c.applyDesired(ctx, desire)
		statusErr := c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ApplyDesire) {
			d.SetDocumentID(desire.GetDocumentID())
			d.Status.ObservedDesireUpdateTime = desire.GetUpdateTime()
			d.Status.AppliedResourceGeneration = appliedGen
			conditions.SetSuccessful(&d.Status.Conditions, syncErr)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(syncErr))
		})
		// Return both errors joined: a non-nil syncErr causes processNext to
		// call AddRateLimited (exponential backoff) rather than Forget, so
		// transient failures (e.g. namespace not yet created) retry promptly
		// instead of waiting for the 10-minute cooldown to expire.
		return errors.Join(syncErr, statusErr)

	case kubeapplier.ApplyDesireTypeDelete:
		mutate := c.evaluateDelete(ctx, desire)
		return c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ApplyDesire) {
			d.SetDocumentID(desire.GetDocumentID())
			d.Status.ObservedDesireUpdateTime = desire.GetUpdateTime()
			mutate(d)
		})

	default:
		syncErr := conditions.NewPreCheckError(fmt.Errorf("unknown desire type %q", desire.Spec.Type))
		statusErr := c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.ApplyDesire) {
			d.SetDocumentID(desire.GetDocumentID())
			d.Status.ObservedDesireUpdateTime = desire.GetUpdateTime()
			conditions.SetSuccessful(&d.Status.Conditions, syncErr)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		})
		return errors.Join(syncErr, statusErr)
	}
}

func (c *ApplyDesireController) applyDesired(ctx context.Context, d *kubeapplier.ApplyDesire) (int64, error) {
	target := d.Spec.TargetItem
	if len(target.Resource) == 0 || len(target.Version) == 0 || len(target.Name) == 0 {
		return 0, conditions.NewPreCheckError(errors.New("spec.targetItem requires version, resource, and name"))
	}
	if d.Spec.ServerSideApply == nil || d.Spec.ServerSideApply.KubeContent == nil || len(d.Spec.ServerSideApply.KubeContent.Raw) == 0 {
		return 0, conditions.NewPreCheckError(errors.New("spec.serverSideApply.kubeContent is empty"))
	}
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(d.Spec.ServerSideApply.KubeContent.Raw); err != nil {
		return 0, conditions.NewPreCheckError(fmt.Errorf("decode kubeContent: %w", err))
	}

	gvr := schema.GroupVersionResource{Group: target.Group, Version: target.Version, Resource: target.Resource}
	resource := c.dyn.Resource(gvr)
	var kubeResourceAccessor dynamic.ResourceInterface = resource
	if len(target.Namespace) > 0 {
		kubeResourceAccessor = resource.Namespace(target.Namespace)
	}

	result, applyErr := kubeResourceAccessor.Apply(ctx, target.Name, obj, metav1.ApplyOptions{
		FieldManager: FieldManager,
		Force:        true,
	})
	if applyErr != nil {
		return 0, fmt.Errorf("server-side apply: %w", applyErr)
	}
	return result.GetGeneration(), nil
}

// evaluateDelete runs the delete state machine for one ApplyDesire with
// Type=Delete. It returns a MutateFunc that encodes the outcome into the
// desire's status conditions; any error is captured inside the closure rather
// than returned directly.
func (c *ApplyDesireController) evaluateDelete(ctx context.Context, d *kubeapplier.ApplyDesire) desirestatuswriter.MutateFunc[kubeapplier.ApplyDesire] {
	target := d.Spec.TargetItem
	if len(target.Resource) == 0 || len(target.Version) == 0 || len(target.Name) == 0 {
		err := conditions.NewPreCheckError(errors.New("spec.targetItem requires version, resource, and name"))
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}
	}

	gvr := schema.GroupVersionResource{Group: target.Group, Version: target.Version, Resource: target.Resource}
	resource := c.dyn.Resource(gvr)
	var kubeResourceAccessor dynamic.ResourceInterface = resource
	if len(target.Namespace) > 0 {
		kubeResourceAccessor = resource.Namespace(target.Namespace)
	}

	got, getErr := kubeResourceAccessor.Get(ctx, target.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, nil)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}
	}
	if getErr != nil {
		err := fmt.Errorf("get target: %w", getErr)
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}
	}

	if dt := got.GetDeletionTimestamp(); dt != nil {
		uid := got.GetUID()
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessfulWaitingForDeletion(&d.Status.Conditions, *dt, uid)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}
	}

	if delErr := kubeResourceAccessor.Delete(ctx, target.Name, metav1.DeleteOptions{}); delErr != nil {
		if apierrors.IsNotFound(delErr) {
			return func(d *kubeapplier.ApplyDesire) {
				conditions.SetSuccessful(&d.Status.Conditions, nil)
				conditions.SetDegraded(&d.Status.Conditions, nil)
			}
		}
		err := fmt.Errorf("delete target: %w", delErr)
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}
	}

	post, postErr := kubeResourceAccessor.Get(ctx, target.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(postErr) {
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, nil)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}
	}
	if postErr != nil {
		err := fmt.Errorf("post-delete get: %w", postErr)
		return func(d *kubeapplier.ApplyDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}
	}
	dt := post.GetDeletionTimestamp()
	uid := post.GetUID()
	if dt == nil {
		now := metav1.NewTime(time.Now())
		dt = &now
	}
	return func(d *kubeapplier.ApplyDesire) {
		conditions.SetSuccessfulWaitingForDeletion(&d.Status.Conditions, *dt, uid)
		conditions.SetDegraded(&d.Status.Conditions, nil)
	}
}

func classifyAsDegraded(err error) error {
	if err == nil {
		return nil
	}
	var preCheck *conditions.PreCheckError
	if errors.As(err, &preCheck) {
		return nil
	}
	if isClientError(err) {
		return nil
	}
	return err
}

func isClientError(err error) bool {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		c := statusErr.ErrStatus.Code
		return c >= 400 && c < 500
	}
	return false
}

// applyDesireSpecFetcher reads from the specs database.
type applyDesireSpecFetcher struct {
	reader database.SpecReader[kubeapplier.ApplyDesire]
}

var _ desirestatuswriter.Fetcher[kubeapplier.ApplyDesire, keys.ApplyDesireKey] = &applyDesireSpecFetcher{}

func (f *applyDesireSpecFetcher) Fetch(ctx context.Context, key keys.ApplyDesireKey) (*kubeapplier.ApplyDesire, error) {
	return f.reader.Get(ctx, key.Name)
}

// applyDesireStatusFetcher reads from the status database.
type applyDesireStatusFetcher struct {
	crud database.ResourceCRUD[kubeapplier.ApplyDesire]
}

var _ desirestatuswriter.Fetcher[kubeapplier.ApplyDesire, keys.ApplyDesireKey] = &applyDesireStatusFetcher{}

func (f *applyDesireStatusFetcher) Fetch(ctx context.Context, key keys.ApplyDesireKey) (*kubeapplier.ApplyDesire, error) {
	return f.crud.Get(ctx, key.Name)
}

type applyDesireReplacer struct {
	crud database.ResourceCRUD[kubeapplier.ApplyDesire]
}

var _ desirestatuswriter.Replacer[kubeapplier.ApplyDesire] = &applyDesireReplacer{}

func (r *applyDesireReplacer) Replace(ctx context.Context, desired *kubeapplier.ApplyDesire) error {
	_, err := r.crud.Replace(ctx, desired)
	return err
}

type applyDesireCreator struct {
	crud database.ResourceCRUD[kubeapplier.ApplyDesire]
}

var _ desirestatuswriter.Creator[kubeapplier.ApplyDesire] = &applyDesireCreator{}

func (c *applyDesireCreator) Create(ctx context.Context, obj *kubeapplier.ApplyDesire) error {
	_, err := c.crud.Create(ctx, obj)
	return err
}
