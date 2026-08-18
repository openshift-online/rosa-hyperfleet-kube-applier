package informers

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
)

const defaultResyncPeriod = 30 * time.Second

// defaultFullResyncPeriod is the interval at which the Streams watcher is
// stopped so the reflector re-Lists all desires from DynamoDB. This ensures
// items whose stream notification was missed are eventually reconciled without
// a pod restart.
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

// NewKubeApplierInformers creates informers that watch the specs DynamoDB
// tables for desire document changes. specsClient is the DynamoDB client for
// the specs tables; streamsClient is the DynamoDB Streams client used for
// change notification. specsPrefix is the table name prefix (full table names
// are prefix + "-applydesires" / "-readdesires").
func NewKubeApplierInformers(
	specsClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	specsPrefix string,
) KubeApplierInformers {
	return NewKubeApplierInformersWithResyncPeriod(specsClient, streamsClient, specsPrefix, defaultResyncPeriod, defaultFullResyncPeriod)
}

func NewKubeApplierInformersWithResyncPeriod(
	specsClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
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
		streamsClient,
		applyTable,
		&kubeapplier.ApplyDesire{},
		func(item map[string]streamtypes.AttributeValue) (runtime.Object, error) {
			// Convert stream image attributes to dynamodb/types.AttributeValue.
			return database.ItemToApplyDesire(streamImageToDynamoDBItem(item))
		},
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
		streamsClient,
		readTable,
		&kubeapplier.ReadDesire{},
		func(item map[string]streamtypes.AttributeValue) (runtime.Object, error) {
			return database.ItemToReadDesire(streamImageToDynamoDBItem(item))
		},
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

func newDesireInformer(
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	exampleObj runtime.Object,
	streamConvertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
	listFn func(context.Context) (runtime.Object, error),
	resyncPeriod time.Duration,
	fullResyncPeriod time.Duration,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(ctx)
		},
		// WatchFuncWithContext returns the real DynamoDB Streams watcher, but
		// wraps it so it is stopped after fullResyncPeriod. When it stops,
		// client-go's reflector treats the closed channel as a normal watch
		// termination and immediately re-Lists, issuing a fresh full DynamoDB
		// Scan. This ensures items whose stream notification was missed are
		// eventually discovered without a pod restart.
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			w := newDynamoDBStreamWatcher(ctx, dbClient, streamsClient, tableName, streamConvertFn)
			go func() {
				select {
				case <-ctx.Done():
				case <-time.After(fullResyncPeriod):
					w.Stop()
				}
			}()
			return w, nil
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
