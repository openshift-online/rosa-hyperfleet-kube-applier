package informers

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
)

// --- Unit tests (no LocalStack required) ---

// TestGetRecordsLimit verifies the declared per-call record limit is the AWS
// maximum so that it is always explicit rather than relying on API defaults.
func TestGetRecordsLimit(t *testing.T) {
	if getRecordsLimit != 1000 {
		t.Errorf("getRecordsLimit = %d, want 1000", getRecordsLimit)
	}
}

// TestPollShardWaitsForParent verifies the parent-before-child ordering
// guarantee (Fix 1): a child shard goroutine must not start polling until the
// parent's done channel is closed.
func TestPollShardWaitsForParent(t *testing.T) {
	parentDone := make(chan struct{})

	var mu sync.RWMutex
	workers := map[string]*shardWorkerState{
		"parent-shard": {done: parentDone},
	}

	w := &dynamoDBStreamWatcher{
		resultCh: make(chan watch.Event, 10),
		done:     make(chan struct{}),
	}
	childWS := &shardWorkerState{done: make(chan struct{})}
	workers["child-shard"] = childWS

	// childPolling is closed by the child goroutine as soon as it exits the
	// parent-wait select (i.e. once it would start polling).
	childPolling := make(chan struct{})

	// Run pollShard in the background. We use a context that we cancel as soon
	// as the parent is done, so the child exits immediately after unblocking
	// rather than panicking on a nil streamsClient.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rediscoverCh := make(chan string, 1)
	go func() {
		w.pollShard(
			ctx,
			nil, // streamsClient — child exits via ctx cancel before using it
			"stream-arn",
			"table",
			shardState{shardID: "child-shard", parentShardID: "parent-shard"},
			childWS,
			&mu,
			workers,
			nil,
			rediscoverCh,
		)
		close(childPolling)
	}()

	// Give the goroutine time to reach the parent-wait select.
	time.Sleep(30 * time.Millisecond)

	// Child should not have proceeded yet.
	select {
	case <-childPolling:
		t.Fatal("child started polling before parent shard was done")
	default:
	}

	// Close parent done and cancel ctx simultaneously so child unblocks then
	// exits cleanly without calling the nil streamsClient.
	cancel()
	close(parentDone)

	// Child must now exit promptly.
	select {
	case <-childPolling:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("child shard goroutine did not exit after parent done and ctx cancel")
	}
}

func TestListWatchWithoutWatchListSemantics(t *testing.T) {
	lw := listWatchWithoutWatchListSemantics{&cache.ListWatch{}}
	if !lw.IsWatchListSemanticsUnSupported() {
		t.Error("expected IsWatchListSemanticsUnSupported to return true")
	}
}

func TestListerListFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	desires := []*kubeapplier.ApplyDesire{
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"}},
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--b"}},
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c2--a"}},
	}
	for _, d := range desires {
		if err := indexer.Add(d); err != nil {
			t.Fatalf("indexer.Add: %v", err)
		}
	}

	lister := listers.NewApplyDesireLister(indexer)

	items, err := lister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("List returned %d items, want 3", len(items))
	}
}

func TestListerGetFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"},
		Spec:             kubeapplier.ApplyDesireSpec{ClusterID: "c1"},
	}
	if err := indexer.Add(d); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}

	lister := listers.NewApplyDesireLister(indexer)

	got, err := lister.Get("c1--a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}
}

func TestListerGetNotFound(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	lister := listers.NewApplyDesireLister(indexer)

	_, err := lister.Get("nonexistent")
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestReadDesireListerFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	d := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--read1"},
		Spec:             kubeapplier.ReadDesireSpec{ClusterID: "c1"},
	}
	if err := indexer.Add(d); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}

	lister := listers.NewReadDesireLister(indexer)

	got, err := lister.Get("c1--read1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}

	items, err := lister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("List returned %d items, want 1", len(items))
	}
}

// TestFullResyncTriggersReList verifies that when the timed-stop goroutine
// fires after fullResyncPeriod the reflector re-issues a List call, picking up
// items that were added to DynamoDB after the initial scan (i.e. items whose
// stream notification was missed). We use a very short fullResyncPeriod
// (100ms) and a mock listFn that injects a second item on the second call.
func TestFullResyncTriggersReList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const fullResyncPeriod = 100 * time.Millisecond

	listCalls := 0
	item1 := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "initial-item"},
	}
	item2 := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "post-startup-item"},
	}

	listFn := func(_ context.Context) (runtime.Object, error) {
		listCalls++
		list := &kubeapplier.ApplyDesireList{}
		list.ResourceVersion = "0"
		list.Items = append(list.Items, *item1)
		if listCalls >= 2 {
			// Simulate an item written to DynamoDB after the initial scan
			// whose stream notification was missed.
			list.Items = append(list.Items, *item2)
		}
		return list, nil
	}

	// Build the ListWatch the same way newDesireInformer does, but using a
	// nil Streams watcher that stops itself after fullResyncPeriod.
	lw := &cache.ListWatch{
		ListWithContextFunc: func(lctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(lctx)
		},
		WatchFuncWithContext: func(wctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
			fw := watch.NewFake()
			go func() {
				select {
				case <-wctx.Done():
				case <-time.After(fullResyncPeriod):
					fw.Stop()
				}
			}()
			return fw, nil
		},
	}
	inf := cache.NewSharedIndexInformerWithOptions(
		&listWatchWithoutWatchListSemantics{lw},
		&kubeapplier.ApplyDesire{},
		cache.SharedIndexInformerOptions{ResyncPeriod: 30 * time.Second},
	)

	go inf.RunWithContext(ctx)
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		t.Fatal("informer did not sync on first List")
	}

	if got := len(inf.GetStore().List()); got != 1 {
		t.Fatalf("after initial sync: cache has %d items, want 1", got)
	}

	// Wait for the watcher to stop and the reflector to re-List (up to 3s).
	deadline := time.After(3 * time.Second)
	for {
		if len(inf.GetStore().List()) == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for re-List to populate post-startup item; listCalls=%d cache=%d",
				listCalls, len(inf.GetStore().List()))
		case <-time.After(50 * time.Millisecond):
		}
	}

	if listCalls < 2 {
		t.Errorf("expected at least 2 List calls (initial + re-List); got %d", listCalls)
	}
}

// --- Integration tests (require LOCALSTACK_ENDPOINT) ---

func requireLocalStack(t *testing.T) {
	t.Helper()
	if os.Getenv("LOCALSTACK_ENDPOINT") == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set; skipping integration test")
	}
}

func newLocalStackClients(t *testing.T) (*dynamodb.Client, *dynamodbstreams.Client) {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		t.Fatalf("awsconfig.LoadDefaultConfig: %v", err)
	}
	dbClient := dynamodb.NewFromConfig(cfg)
	streamsClient := dynamodbstreams.NewFromConfig(cfg)
	return dbClient, streamsClient
}

func createTestTable(t *testing.T, dbClient *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, err := dbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: dbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{AttributeName: aws.String("documentID"), AttributeType: dbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []dbtypes.KeySchemaElement{
			{AttributeName: aws.String("documentID"), KeyType: dbtypes.KeyTypeHash},
		},
		StreamSpecification: &dbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		t.Fatalf("CreateTable %s: %v", tableName, err)
	}
	t.Cleanup(func() {
		dbClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

func startAndSync(t *testing.T, ctx context.Context, info KubeApplierInformers) {
	t.Helper()
	go info.RunWithContext(ctx)
	applyInf, _ := info.ApplyDesires()
	readInf, _ := info.ReadDesires()
	if !cache.WaitForCacheSync(ctx.Done(), applyInf.HasSynced, readInf.HasSynced) {
		t.Fatal("informers did not sync")
	}
}

func waitForCacheCount(t *testing.T, store cache.Store, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if len(store.List()) == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for cache to contain %d items (has %d)", want, len(store.List()))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestIntegration_InformerSyncsExistingDocuments(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient, streamsClient := newLocalStackClients(t)
	prefix := fmt.Sprintf("inf-existing-%d", time.Now().UnixNano())

	applyTable := prefix + database.TableSuffixApplyDesires
	readTable := prefix + database.TableSuffixReadDesires
	createTestTable(t, dbClient, applyTable)
	createTestTable(t, dbClient, readTable)

	dbCRUD := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)

	for i := 0; i < 3; i++ {
		d := &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: fmt.Sprintf("c1--item%d", i)},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: "mc-test",
				ClusterID:         "c1",
				TargetItem: kubeapplier.ResourceReference{
					Version:  "v1",
					Resource: "configmaps",
					Name:     fmt.Sprintf("cm-%d", i),
				},
			},
		}
		if _, err := dbCRUD.ApplyDesireStatus().Create(ctx, d); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	info := NewKubeApplierInformersWithResyncPeriod(dbClient, streamsClient, prefix, 30*time.Second)
	startAndSync(t, ctx, info)

	applyInf, applyLister := info.ApplyDesires()
	if len(applyInf.GetStore().List()) != 3 {
		t.Errorf("expected 3 items in cache, got %d", len(applyInf.GetStore().List()))
	}

	items, err := applyLister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("lister returned %d items, want 3", len(items))
	}

	got, err := applyLister.Get("c1--item1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}
}

func TestIntegration_StreamDeliversEvents(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient, streamsClient := newLocalStackClients(t)
	prefix := fmt.Sprintf("inf-stream-%d", time.Now().UnixNano())

	applyTable := prefix + database.TableSuffixApplyDesires
	readTable := prefix + database.TableSuffixReadDesires
	createTestTable(t, dbClient, applyTable)
	createTestTable(t, dbClient, readTable)

	dbCRUD := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)
	crud := dbCRUD.ApplyDesireStatus()

	info := NewKubeApplierInformersWithResyncPeriod(dbClient, streamsClient, prefix, 30*time.Second)
	startAndSync(t, ctx, info)

	applyInf, _ := info.ApplyDesires()

	// Create a document — the stream watcher should deliver it.
	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--live"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "live-cm",
			},
		},
	}
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitForCacheCount(t, applyInf.GetStore(), 1, 30*time.Second)

	// Modify the document.
	created.Spec.ClusterID = "c2"
	if _, err := crud.Replace(ctx, created); err != nil {
		t.Fatalf("Replace: %v", err)
	}

	// Wait for the modification to appear in the cache.
	deadline := time.After(30 * time.Second)
	for {
		item, exists, _ := applyInf.GetStore().GetByKey("c1--live")
		if exists {
			if item.(*kubeapplier.ApplyDesire).Spec.ClusterID == "c2" {
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for modification in cache")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Delete the document.
	if err := crud.Delete(ctx, "c1--live"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitForCacheCount(t, applyInf.GetStore(), 0, 30*time.Second)
}

func TestIntegration_PerTableIsolation(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient, streamsClient := newLocalStackClients(t)
	prefixA := fmt.Sprintf("inf-iso-a-%d", time.Now().UnixNano())
	prefixB := fmt.Sprintf("inf-iso-b-%d", time.Now().UnixNano())

	for _, prefix := range []string{prefixA, prefixB} {
		createTestTable(t, dbClient, prefix+database.TableSuffixApplyDesires)
		createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)
	}

	dbCRUDA := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefixA, prefixA)

	infoA := NewKubeApplierInformersWithResyncPeriod(dbClient, streamsClient, prefixA, 30*time.Second)
	infoB := NewKubeApplierInformersWithResyncPeriod(dbClient, streamsClient, prefixB, 30*time.Second)
	startAndSync(t, ctx, infoA)
	startAndSync(t, ctx, infoB)

	applyInfA, _ := infoA.ApplyDesires()
	applyInfB, _ := infoB.ApplyDesires()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--isolated"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-a",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "iso-cm",
			},
		},
	}
	if _, err := dbCRUDA.ApplyDesireStatus().Create(ctx, d); err != nil {
		t.Fatalf("Create in A: %v", err)
	}

	waitForCacheCount(t, applyInfA.GetStore(), 1, 30*time.Second)

	// B should remain empty.
	time.Sleep(500 * time.Millisecond)
	if len(applyInfB.GetStore().List()) != 0 {
		t.Errorf("expected 0 items in B's cache, got %d", len(applyInfB.GetStore().List()))
	}
}
