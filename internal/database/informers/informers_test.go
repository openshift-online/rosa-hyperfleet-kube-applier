package informers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
)

// --- Unit tests (no LocalStack required) ---

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

// --- Integration tests (require LOCALSTACK_ENDPOINT) ---

func requireLocalStack(t *testing.T) {
	t.Helper()
	if os.Getenv("LOCALSTACK_ENDPOINT") == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set; skipping integration test")
	}
}

func newLocalStackClient(t *testing.T) *dynamodb.Client {
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
	return dynamodb.NewFromConfig(cfg)
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

	dbClient := newLocalStackClient(t)
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

	info := NewKubeApplierInformersWithResyncPeriod(dbClient, prefix, 30*time.Second)
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

func TestIntegration_PerTableIsolation(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefixA := fmt.Sprintf("inf-iso-a-%d", time.Now().UnixNano())
	prefixB := fmt.Sprintf("inf-iso-b-%d", time.Now().UnixNano())

	for _, prefix := range []string{prefixA, prefixB} {
		createTestTable(t, dbClient, prefix+database.TableSuffixApplyDesires)
		createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)
	}

	dbCRUDA := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefixA, prefixA)

	infoA := NewKubeApplierInformersWithResyncPeriod(dbClient, prefixA, 30*time.Second)
	infoB := NewKubeApplierInformersWithResyncPeriod(dbClient, prefixB, 30*time.Second)
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

	// B should remain empty (no stream/poller delivering to B's cache).
	time.Sleep(500 * time.Millisecond)
	if len(applyInfB.GetStore().List()) != 0 {
		t.Errorf("expected 0 items in B's cache, got %d", len(applyInfB.GetStore().List()))
	}
	_ = applyInfA // suppress unused warning
}
