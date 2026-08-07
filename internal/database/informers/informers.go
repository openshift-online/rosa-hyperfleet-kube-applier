package informers

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
)

const defaultResyncPeriod = 30 * time.Second

// defaultFullResyncPeriod is the interval at which the fake watcher is stopped
// so the reflector re-Lists all desires from DynamoDB. This ensures items whose
// SQS notification was missed are eventually reconciled without a pod restart.
const defaultFullResyncPeriod = 5 * time.Minute

type KubeApplierInformers interface {
	ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister)
	ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister)
	RunWithContext(ctx context.Context)
}

type kubeApplierInformers struct {
	applyDesireInformer cache.SharedIndexInformer
	applyDesireLister   listers.ApplyDesireLister
	readDesireInformer  cache.SharedIndexInformer
	readDesireLister    listers.ReadDesireLister
}

// NewKubeApplierInformers creates informers that populate their caches from a
// full DynamoDB Scan on startup and re-scan periodically (every fullResyncPeriod)
// to catch any items whose SQS notification was missed. Incremental change
// notification between rescans is handled by the SQS poller.
// specsClient is the DynamoDB client for the specs tables. specsPrefix is the
// table name prefix (full table names are prefix + "-applydesires" / "-readdesires").
// If fullResyncPeriod is zero, defaultFullResyncPeriod (5 minutes) is used.
func NewKubeApplierInformers(
	specsClient *dynamodb.Client,
	specsPrefix string,
	fullResyncPeriod time.Duration,
) KubeApplierInformers {
	return NewKubeApplierInformersWithResyncPeriod(specsClient, specsPrefix, defaultResyncPeriod, fullResyncPeriod)
}

func NewKubeApplierInformersWithResyncPeriod(
	specsClient *dynamodb.Client,
	specsPrefix string,
	resyncPeriod time.Duration,
	fullResyncPeriod time.Duration,
) KubeApplierInformers {
	if fullResyncPeriod == 0 {
		fullResyncPeriod = defaultFullResyncPeriod
	}
	applyTable := specsPrefix + database.TableSuffixApplyDesires
	readTable := specsPrefix + database.TableSuffixReadDesires

	applyInf := newDesireInformer(
		specsClient,
		applyTable,
		&kubeapplier.ApplyDesire{},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).ApplyDesireSpecs()
			items, err := specReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.ApplyDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		resyncPeriod,
		fullResyncPeriod,
	)

	readInf := newDesireInformer(
		specsClient,
		readTable,
		&kubeapplier.ReadDesire{},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).ReadDesireSpecs()
			items, err := specReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.ReadDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		resyncPeriod,
		fullResyncPeriod,
	)

	return &kubeApplierInformers{
		applyDesireInformer: applyInf,
		applyDesireLister:   listers.NewApplyDesireLister(applyInf.GetIndexer()),
		readDesireInformer:  readInf,
		readDesireLister:    listers.NewReadDesireLister(readInf.GetIndexer()),
	}
}

// listWatchWithoutWatchListSemantics opts out of the WatchList streaming mode
// enabled by default in client-go v0.35+. Our fake watcher does not emit the
// bookmark that WatchList requires, so the reflector would never reach Synced
// without this wrapper.
type listWatchWithoutWatchListSemantics struct {
	*cache.ListWatch
}

func (listWatchWithoutWatchListSemantics) IsWatchListSemanticsUnSupported() bool { return true }

func newDesireInformer(
	_ *dynamodb.Client,
	_ string,
	exampleObj runtime.Object,
	listFn func(context.Context) (runtime.Object, error),
	resyncPeriod time.Duration,
	fullResyncPeriod time.Duration,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(ctx)
		},
		// WatchFuncWithContext returns a fake watcher that stops itself after
		// fullResyncPeriod. When it stops, client-go's reflector treats the
		// closed channel as a normal watch termination and immediately re-Lists,
		// issuing a fresh full DynamoDB Scan. This is the mechanism by which
		// items added to DynamoDB after startup (and whose SQS notification was
		// missed) are eventually discovered without a pod restart.
		WatchFuncWithContext: func(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
			fw := watch.NewFake()
			go func() {
				select {
				case <-ctx.Done():
				case <-time.After(fullResyncPeriod):
				}
				fw.Stop()
			}()
			return fw, nil
		},
	}
	return cache.NewSharedIndexInformerWithOptions(
		&listWatchWithoutWatchListSemantics{lw},
		exampleObj,
		cache.SharedIndexInformerOptions{
			ResyncPeriod: resyncPeriod,
		},
	)
}

func (k *kubeApplierInformers) ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister) {
	return k.applyDesireInformer, k.applyDesireLister
}

func (k *kubeApplierInformers) ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister) {
	return k.readDesireInformer, k.readDesireLister
}

func (k *kubeApplierInformers) RunWithContext(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		k.applyDesireInformer.RunWithContext(ctx)
	}()
	go func() {
		defer wg.Done()
		k.readDesireInformer.RunWithContext(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}
