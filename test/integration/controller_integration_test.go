// Package integration contains end-to-end tests that wire the full
// kube-applier-aws stack: LocalStack (DynamoDB + SQS) as the backend store
// and a Kind cluster (real kube-apiserver) as the reconciliation target.
//
// # Prerequisites
//
//   - LOCALSTACK_ENDPOINT env var set (e.g. http://localhost:4566)
//   - KUBECONFIG env var pointing at a Kind cluster
//
// Both env vars must be set; if either is missing all tests are skipped.
//
// # Quick start
//
//	make localstack          # starts LocalStack detached
//	make kind-setup          # creates Kind cluster + namespace + RBAC
//	LOCALSTACK_ENDPOINT=http://localhost:4566 \
//	  KUBECONFIG=$HOME/.kube/config \
//	  go test ./test/integration/... -v -count=1 -timeout 120s
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/prometheus/client_golang/prometheus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/informers"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/app"
)

// ----------------------------------------------------------------------------
// Guard helpers
// ----------------------------------------------------------------------------

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("skipping integration test: %s not set", key)
	}
	return v
}

// requireIntegration skips the test unless both LOCALSTACK_ENDPOINT and
// KUBECONFIG are set.
func requireIntegration(t *testing.T) (localstackEndpoint, kubeconfigPath string) {
	t.Helper()
	localstackEndpoint = requireEnv(t, "LOCALSTACK_ENDPOINT")
	kubeconfigPath = requireEnv(t, "KUBECONFIG")
	return
}

// ----------------------------------------------------------------------------
// Fixture
// ----------------------------------------------------------------------------

// fixture holds all wired dependencies for a single test run.
type fixture struct {
	dynDB          *dynamodb.Client
	sqsClient      *sqs.Client
	sqsQueueURL    string
	dbClient       database.KubeApplierDBClient
	dynKube        dynamic.Interface
	kubeconfigPath string
	specsPrefix    string
	statusPrefix   string
}

// newFixture creates unique DynamoDB tables and an SQS queue, and registers
// t.Cleanup to delete them. It also builds the dynamic Kube client.
func newFixture(t *testing.T, localstackEndpoint, kubeconfigPath string) *fixture {
	t.Helper()

	// AWS / LocalStack clients.
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithBaseEndpoint(localstackEndpoint),
	)
	if err != nil {
		t.Fatalf("awsconfig.LoadDefaultConfig: %v", err)
	}

	dynDB := dynamodb.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)

	ts := fmt.Sprintf("%d", time.Now().UnixNano())
	specsPrefix := "inttest-" + ts + "-specs"
	statusPrefix := "inttest-" + ts + "-status"

	for _, p := range []string{specsPrefix, statusPrefix} {
		for _, suffix := range []string{
			database.TableSuffixApplyDesires,
			database.TableSuffixReadDesires,
		} {
			createTable(t, dynDB, p+suffix)
		}
	}

	// Create an SQS queue for this test run.
	queueName := "inttest-" + ts
	createOut, err := sqsClient.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		t.Fatalf("CreateQueue %s: %v", queueName, err)
	}
	sqsQueueURL := aws.ToString(createOut.QueueUrl)
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{
			QueueUrl: aws.String(sqsQueueURL),
		})
	})

	dbClient := database.NewDynamoDBKubeApplierDBClient(dynDB, dynDB, specsPrefix, statusPrefix)

	// Kubernetes dynamic client.
	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		t.Fatalf("clientcmd.BuildConfigFromFlags: %v", err)
	}
	dynKube, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}

	return &fixture{
		dynDB:          dynDB,
		sqsClient:      sqsClient,
		sqsQueueURL:    sqsQueueURL,
		dbClient:       dbClient,
		dynKube:        dynKube,
		kubeconfigPath: kubeconfigPath,
		specsPrefix:    specsPrefix,
		statusPrefix:   statusPrefix,
	}
}

// createTable creates a DynamoDB table with a "documentID" hash key and
// registers deletion in t.Cleanup.
func createTable(t *testing.T, dbClient *dynamodb.Client, tableName string) {
	t.Helper()
	_, err := dbClient.CreateTable(context.Background(), &dynamodb.CreateTableInput{
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
		_, _ = dbClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

// ----------------------------------------------------------------------------
// App runner
// ----------------------------------------------------------------------------

// startApp wires and runs app.Options.Run in a background goroutine. It
// returns a cancel func to stop the app and a channel that receives the Run
// error when the app exits.
func startApp(t *testing.T, f *fixture) (context.CancelFunc, <-chan error) {
	t.Helper()

	reg := prometheus.NewRegistry()

	inf := informers.NewKubeApplierInformersWithResyncPeriod(
		f.dynDB, f.specsPrefix,
		5*time.Second,
	)

	restCfg, err := clientcmd.BuildConfigFromFlags("", f.kubeconfigPath)
	if err != nil {
		t.Fatalf("BuildConfigFromFlags for leader election: %v", err)
	}
	// Use a unique lease name so parallel tests don't interfere.
	leaseName := fmt.Sprintf("inttest-%d", time.Now().UnixNano())
	lock, err := resourcelock.NewFromKubeconfig(
		resourcelock.LeasesResourceLock,
		"kube-applier-system",
		leaseName,
		resourcelock.ResourceLockConfig{Identity: "integration-test"},
		restCfg,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("NewFromKubeconfig: %v", err)
	}

	opts := &app.Options{
		ManagementCluster:          "inttest",
		LeaderElectionLock:         lock,
		KubeApplierDBClient:        f.dbClient,
		Informers:                  inf,
		DynamicClient:              f.dynKube,
		SQSClient:                  f.sqsClient,
		SQSQueueURL:                f.sqsQueueURL,
		MetricsServerListenAddress: "", // disabled in tests
		HealthzServerListenAddress: "", // disabled in tests
		MetricsRegisterer:          reg,
		MetricsGatherer:            reg,
		ExitOnPanic:                false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- opts.Run(ctx)
	}()

	return cancel, errCh
}

// ----------------------------------------------------------------------------
// Poll helper
// ----------------------------------------------------------------------------

// pollUntil calls cond every 500 ms until it returns true or timeout elapses.
// It calls t.Fatal on timeout.
func pollUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("condition not satisfied within %s", timeout)
}

// ----------------------------------------------------------------------------
// Kubernetes helpers
// ----------------------------------------------------------------------------

var configMapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

// getConfigMap fetches a ConfigMap; returns nil if not found.
func getConfigMap(dynKube dynamic.Interface, namespace, name string) (*unstructured.Unstructured, error) {
	obj, err := dynKube.Resource(configMapGVR).Namespace(namespace).Get(
		context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return obj, err
}

// createConfigMapDirect creates a ConfigMap directly on the Kind cluster and
// registers deletion in t.Cleanup.
func createConfigMapDirect(t *testing.T, dynKube dynamic.Interface, namespace, name string) {
	t.Helper()
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"data": map[string]interface{}{"pre": "existing"},
		},
	}
	_, err := dynKube.Resource(configMapGVR).Namespace(namespace).Create(
		context.Background(), obj, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("createConfigMapDirect %s/%s: %v", namespace, name, err)
	}
	t.Cleanup(func() {
		_ = dynKube.Resource(configMapGVR).Namespace(namespace).Delete(
			context.Background(), name, metav1.DeleteOptions{})
	})
}

// ----------------------------------------------------------------------------
// DynamoDB spec writers
//
// In production, specs tables are written by an external backend and are
// read-only for kube-applier. In tests we use a single DynamoDB client with
// full access, so we write spec items directly using PutItem.
// ----------------------------------------------------------------------------

// writeApplyDesireSpec marshals a desire struct into a flat DynamoDB item and
// writes it to the given table.
func writeApplyDesireSpec(t *testing.T, dbClient *dynamodb.Client, specsPrefix string, d *kubeapplier.ApplyDesire) {
	t.Helper()
	tableName := specsPrefix + database.TableSuffixApplyDesires

	var kubeContentStr string
	if d.Spec.ServerSideApply != nil && d.Spec.ServerSideApply.KubeContent != nil {
		kubeContentStr = string(d.Spec.ServerSideApply.KubeContent.Raw)
	}

	item := map[string]dbtypes.AttributeValue{
		"documentID": &dbtypes.AttributeValueMemberS{Value: d.DocumentID},
		"version":    &dbtypes.AttributeValueMemberN{Value: "1"},
		"updateTime": &dbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
		"createTime": &dbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
		"spec": &dbtypes.AttributeValueMemberM{Value: map[string]dbtypes.AttributeValue{
			"managementCluster": &dbtypes.AttributeValueMemberS{Value: d.Spec.ManagementCluster},
			"clusterID":         &dbtypes.AttributeValueMemberS{Value: d.Spec.ClusterID},
			"type":              &dbtypes.AttributeValueMemberS{Value: string(d.Spec.Type)},
			"targetItem": &dbtypes.AttributeValueMemberM{Value: map[string]dbtypes.AttributeValue{
				"group":     &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Group},
				"version":   &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Version},
				"resource":  &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Resource},
				"namespace": &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Namespace},
				"name":      &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Name},
			}},
		}},
	}
	if kubeContentStr != "" {
		item["spec_kubeContent"] = &dbtypes.AttributeValueMemberS{Value: kubeContentStr}
	}

	if _, err := dbClient.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	}); err != nil {
		t.Fatalf("writeApplyDesireSpec PutItem %s/%s: %v", tableName, d.DocumentID, err)
	}
}

// sendSQSNotification publishes a SpecNotification message directly to the SQS
// queue so the poller picks it up and enqueues the document ID for reconciliation.
func sendSQSNotification(t *testing.T, sqsClient *sqs.Client, queueURL, documentID, tableSuffix string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"documentID":  documentID,
		"tableSuffix": tableSuffix,
	})
	if err != nil {
		t.Fatalf("json.Marshal SQS notification: %v", err)
	}
	if _, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
}

// TestIntegration_DeleteDesire pre-creates a ConfigMap on Kind, writes an
// ApplyDesire (Type=Delete) spec to DynamoDB, starts the app, and asserts:
//  1. The ConfigMap is deleted from the Kind cluster.
//  2. The status document records Successful=True.
func TestIntegration_DeleteDesire(t *testing.T) {
	localstackEndpoint, kubeconfigPath := requireIntegration(t)
	f := newFixture(t, localstackEndpoint, kubeconfigPath)

	const (
		documentID = "inttest--delete-cm"
		cmName     = "inttest-delete-cm"
		namespace  = "default"
	)

	// Pre-create the ConfigMap.
	createConfigMapDirect(t, f.dynKube, namespace, cmName)

	writeApplyDesireSpec(t, f.dynDB, f.specsPrefix, &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: documentID},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "inttest",
			ClusterID:         "inttest",
			Type:              kubeapplier.ApplyDesireTypeDelete,
			TargetItem: kubeapplier.ResourceReference{
				Version:   "v1",
				Resource:  "configmaps",
				Namespace: namespace,
				Name:      cmName,
			},
		},
	})

	cancel, errCh := startApp(t, f)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	// Send SQS notification so the poller enqueues the document for reconciliation.
	sendSQSNotification(t, f.sqsClient, f.sqsQueueURL, documentID, database.TableSuffixApplyDesires)

	// 1. ConfigMap must be gone.
	pollUntil(t, 60*time.Second, func() bool {
		obj, _ := getConfigMap(f.dynKube, namespace, cmName)
		return obj == nil
	})
	t.Logf("ConfigMap %s/%s deleted by ApplyDesire (Type=Delete) controller", namespace, cmName)

	// 2. Status must show Successful=True.
	pollUntil(t, 30*time.Second, func() bool {
		status, err := f.dbClient.ApplyDesireStatus().Get(context.Background(), documentID)
		if err != nil {
			return false
		}
		for _, c := range status.Status.Conditions {
			if c.Type == kubeapplier.ConditionTypeSuccessful && c.Status == metav1.ConditionTrue {
				return true
			}
		}
		return false
	})
	t.Logf("ApplyDesire (Type=Delete) status Successful=True in DynamoDB")
}

func writeReadDesireSpec(t *testing.T, dbClient *dynamodb.Client, specsPrefix string, d *kubeapplier.ReadDesire) {
	t.Helper()
	tableName := specsPrefix + database.TableSuffixReadDesires
	item := map[string]dbtypes.AttributeValue{
		"documentID": &dbtypes.AttributeValueMemberS{Value: d.DocumentID},
		"version":    &dbtypes.AttributeValueMemberN{Value: "1"},
		"updateTime": &dbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
		"createTime": &dbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
		"spec": &dbtypes.AttributeValueMemberM{Value: map[string]dbtypes.AttributeValue{
			"managementCluster": &dbtypes.AttributeValueMemberS{Value: d.Spec.ManagementCluster},
			"clusterID":         &dbtypes.AttributeValueMemberS{Value: d.Spec.ClusterID},
			"targetItem": &dbtypes.AttributeValueMemberM{Value: map[string]dbtypes.AttributeValue{
				"group":     &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Group},
				"version":   &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Version},
				"resource":  &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Resource},
				"namespace": &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Namespace},
				"name":      &dbtypes.AttributeValueMemberS{Value: d.Spec.TargetItem.Name},
			}},
		}},
	}
	if _, err := dbClient.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	}); err != nil {
		t.Fatalf("writeReadDesireSpec PutItem %s/%s: %v", tableName, d.DocumentID, err)
	}
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

// TestIntegration_ApplyDesire writes an ApplyDesire spec to the DynamoDB specs
// table, starts the full app stack (informers + controllers + leader election),
// sends an SQS notification, and asserts that:
//  1. The corresponding ConfigMap appears in the Kind cluster.
//  2. The status document in DynamoDB records Successful=True.
func TestIntegration_ApplyDesire(t *testing.T) {
	localstackEndpoint, kubeconfigPath := requireIntegration(t)
	f := newFixture(t, localstackEndpoint, kubeconfigPath)

	const (
		documentID = "inttest--apply-cm"
		cmName     = "inttest-apply-cm"
		namespace  = "default"
	)

	// Ensure the ConfigMap doesn't already exist from a previous run.
	_ = f.dynKube.Resource(configMapGVR).Namespace(namespace).Delete(
		context.Background(), cmName, metav1.DeleteOptions{})

	// Build the ConfigMap JSON that the controller will SSA-apply.
	cmJSON, err := json.Marshal(map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]interface{}{"name": cmName, "namespace": namespace},
		"data":       map[string]interface{}{"hello": "world"},
	})
	if err != nil {
		t.Fatalf("json.Marshal ConfigMap: %v", err)
	}

	// Write the spec to the specs table BEFORE starting the app so the initial
	// Scan (List path) picks it up.
	writeApplyDesireSpec(t, f.dynDB, f.specsPrefix, &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: documentID},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "inttest",
			ClusterID:         "inttest",
			TargetItem: kubeapplier.ResourceReference{
				Version:   "v1",
				Resource:  "configmaps",
				Namespace: namespace,
				Name:      cmName,
			},
			ServerSideApply: &kubeapplier.ServerSideApplyConfig{KubeContent: &runtime.RawExtension{Raw: cmJSON}},
		},
	})

	cancel, errCh := startApp(t, f)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	// Send SQS notification so the poller enqueues the document for reconciliation.
	sendSQSNotification(t, f.sqsClient, f.sqsQueueURL, documentID, database.TableSuffixApplyDesires)

	// 1. ConfigMap must appear in the Kind cluster.
	pollUntil(t, 60*time.Second, func() bool {
		obj, _ := getConfigMap(f.dynKube, namespace, cmName)
		return obj != nil
	})
	t.Logf("ConfigMap %s/%s created by ApplyDesire controller", namespace, cmName)
	t.Cleanup(func() {
		_ = f.dynKube.Resource(configMapGVR).Namespace(namespace).Delete(
			context.Background(), cmName, metav1.DeleteOptions{})
	})

	// 2. Status document must show Successful=True.
	pollUntil(t, 30*time.Second, func() bool {
		status, err := f.dbClient.ApplyDesireStatus().Get(context.Background(), documentID)
		if err != nil {
			return false
		}
		for _, c := range status.Status.Conditions {
			if c.Type == kubeapplier.ConditionTypeSuccessful && c.Status == metav1.ConditionTrue {
				return true
			}
		}
		return false
	})
	t.Logf("ApplyDesire status Successful=True in DynamoDB")
}

// TestIntegration_ReadDesire pre-creates a ConfigMap on Kind, writes a
// ReadDesire spec to DynamoDB, starts the app, sends an SQS notification, and
// asserts that the status document's KubeContent field is populated with the
// live object JSON.
func TestIntegration_ReadDesire(t *testing.T) {
	localstackEndpoint, kubeconfigPath := requireIntegration(t)
	f := newFixture(t, localstackEndpoint, kubeconfigPath)

	const (
		documentID = "inttest--read-cm"
		cmName     = "inttest-read-cm"
		namespace  = "default"
	)

	// Pre-create the ConfigMap so the read controller can find it.
	createConfigMapDirect(t, f.dynKube, namespace, cmName)

	writeReadDesireSpec(t, f.dynDB, f.specsPrefix, &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: documentID},
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: "inttest",
			ClusterID:         "inttest",
			TargetItem: kubeapplier.ResourceReference{
				Version:   "v1",
				Resource:  "configmaps",
				Namespace: namespace,
				Name:      cmName,
			},
		},
	})

	cancel, errCh := startApp(t, f)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	// Send SQS notification so the poller enqueues the document for reconciliation.
	sendSQSNotification(t, f.sqsClient, f.sqsQueueURL, documentID, database.TableSuffixReadDesires)

	// Status.KubeContent must be populated.
	pollUntil(t, 60*time.Second, func() bool {
		status, err := f.dbClient.ReadDesireStatus().Get(context.Background(), documentID)
		if err != nil {
			return false
		}
		return status.Status.KubeContent != nil && len(status.Status.KubeContent.Raw) > 0
	})
	t.Logf("ReadDesire status.kubeContent populated in DynamoDB")
}

// TestIntegration_OptimisticConcurrency starts the app, waits for the
// ApplyDesire controller to create the initial status document, then races two
// concurrent Replace calls with the same version. It asserts that exactly one
// call wins and exactly one returns ErrPreconditionFailed.
func TestIntegration_OptimisticConcurrency(t *testing.T) {
	localstackEndpoint, kubeconfigPath := requireIntegration(t)
	f := newFixture(t, localstackEndpoint, kubeconfigPath)

	const (
		documentID = "inttest--concurrency-cm"
		cmName     = "inttest-concurrency-cm"
		namespace  = "default"
	)

	cmJSON, _ := json.Marshal(map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]interface{}{"name": cmName, "namespace": namespace},
		"data":       map[string]interface{}{"k": "v"},
	})

	writeApplyDesireSpec(t, f.dynDB, f.specsPrefix, &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: documentID},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "inttest",
			ClusterID:         "inttest",
			TargetItem: kubeapplier.ResourceReference{
				Version:   "v1",
				Resource:  "configmaps",
				Namespace: namespace,
				Name:      cmName,
			},
			ServerSideApply: &kubeapplier.ServerSideApplyConfig{KubeContent: &runtime.RawExtension{Raw: cmJSON}},
		},
	})

	cancel, errCh := startApp(t, f)
	defer func() {
		cancel()
		<-errCh
	}()
	t.Cleanup(func() {
		_ = f.dynKube.Resource(configMapGVR).Namespace(namespace).Delete(
			context.Background(), cmName, metav1.DeleteOptions{})
	})

	// Send SQS notification so the poller triggers initial reconciliation.
	sendSQSNotification(t, f.sqsClient, f.sqsQueueURL, documentID, database.TableSuffixApplyDesires)

	statusCRUD := f.dbClient.ApplyDesireStatus()

	// Wait for the controller to create the initial status document.
	var initial *kubeapplier.ApplyDesire
	pollUntil(t, 60*time.Second, func() bool {
		var err error
		initial, err = statusCRUD.Get(context.Background(), documentID)
		return err == nil && initial != nil
	})
	t.Logf("initial status doc created, version=%d", initial.Version)

	// Race two Replace calls that both present the same version.
	copy1 := initial.DeepCopy()
	copy2 := initial.DeepCopy()
	// Mutate each copy so the writes differ (otherwise equality check may no-op).
	copy1.Spec.ManagementCluster = "inttest-a"
	copy2.Spec.ManagementCluster = "inttest-b"

	type result struct{ err error }
	ch := make(chan result, 2)
	go func() { _, err := statusCRUD.Replace(context.Background(), copy1); ch <- result{err} }()
	go func() { _, err := statusCRUD.Replace(context.Background(), copy2); ch <- result{err} }()

	r1, r2 := <-ch, <-ch

	precondFailed := 0
	for _, r := range []result{r1, r2} {
		if r.err != nil {
			if database.IsPreconditionFailedError(r.err) {
				precondFailed++
			} else {
				t.Errorf("unexpected error (not ErrPreconditionFailed): %v", r.err)
			}
		}
	}
	if precondFailed != 1 {
		t.Errorf("expected exactly 1 ErrPreconditionFailed, got %d (errs: %v / %v)",
			precondFailed, r1.err, r2.err)
	}
	t.Logf("optimistic concurrency: exactly one Replace won, one got ErrPreconditionFailed")
}
