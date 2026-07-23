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
// full DynamoDB Scan on startup. Incremental change notification is handled by
// the SQS poller (see internal/database/sqspoller), not by DynamoDB Streams.
// specsClient is the DynamoDB client for the specs tables. specsPrefix is the
// table name prefix (full table names are prefix + "-applydesires" / "-readdesires").
func NewKubeApplierInformers(
	specsClient *dynamodb.Client,
	specsPrefix string,
) KubeApplierInformers {
	return NewKubeApplierInformersWithResyncPeriod(specsClient, specsPrefix, defaultResyncPeriod)
}

func NewKubeApplierInformersWithResyncPeriod(
	specsClient *dynamodb.Client,
	specsPrefix string,
	resyncPeriod time.Duration,
) KubeApplierInformers {
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
	)

	return &kubeApplierInformers{
		applyDesireInformer: applyInf,
		applyDesireLister:   listers.NewApplyDesireLister(applyInf.GetIndexer()),
		readDesireInformer:  readInf,
		readDesireLister:    listers.NewReadDesireLister(readInf.GetIndexer()),
	}
}

func newDesireInformer(
	_ *dynamodb.Client,
	_ string,
	exampleObj runtime.Object,
	listFn func(context.Context) (runtime.Object, error),
	resyncPeriod time.Duration,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(ctx)
		},
		// WatchFuncWithContext returns a no-op watcher that blocks until ctx is
		// cancelled. Incremental updates are driven by the SQS poller which
		// enqueues directly into the controller workqueue — not through the
		// informer cache. The informer cache is populated once by the List
		// (full Scan) above and then remains a stable snapshot.
		WatchFuncWithContext: func(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
			fw := watch.NewFake()
			go func() {
				<-ctx.Done()
				fw.Stop()
			}()
			return fw, nil
		},
	}
	return cache.NewSharedIndexInformerWithOptions(
		lw,
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
