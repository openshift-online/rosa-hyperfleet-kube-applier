package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	sigsyaml "sigs.k8s.io/yaml"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/desireid"
)

var (
	flagAWSRegion    string
	flagEndpointURL  string
	flagSpecsPrefix  string
	flagStatusPrefix string
)

func main() {
	root := &cobra.Command{
		Use:   "desire-tool",
		Short: "CLI for creating and inspecting kube-applier desires in DynamoDB",
	}

	root.PersistentFlags().StringVar(&flagAWSRegion, "aws-region", "us-east-1", "AWS region")
	root.PersistentFlags().StringVar(&flagEndpointURL, "aws-endpoint-url", "", "Optional AWS endpoint URL override (e.g. http://localhost:4566)")
	root.PersistentFlags().StringVar(&flagSpecsPrefix, "specs-table", "mc-dev-local-specs", "DynamoDB table name prefix for spec tables")
	root.PersistentFlags().StringVar(&flagStatusPrefix, "status-table", "mc-dev-local-status", "DynamoDB table name prefix for status tables")

	root.AddCommand(
		newCreateApplyCmd(),
		newCreateDeleteCmd(),
		newCreateReadCmd(),
		newUpdateApplyCmd(),
		newUpdateReadCmd(),
		newGetApplyCmd(),
		newGetDeleteCmd(),
		newGetReadCmd(),
		newListApplyCmd(),
		newListDeleteCmd(),
		newListReadCmd(),
		newDeleteApplyCmd(),
		newDeleteDeleteCmd(),
		newDeleteReadCmd(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// newDynamoDBClient creates a DynamoDB client using the persistent flags.
func newDynamoDBClient(ctx context.Context) *dynamodb.Client {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(flagAWSRegion),
	}
	if flagEndpointURL != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(flagEndpointURL))
	}
	// Allow test/LocalStack use with dummy credentials when none are configured.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && flagEndpointURL != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to load AWS config: %v\n", err)
		os.Exit(1)
	}
	return dynamodb.NewFromConfig(cfg)
}

// twoDBClients creates two KubeApplierDBClient instances — one backed by the
// specs tables and one by the status tables. Each uses the same DynamoDB
// client for both the spec-reader and status-CRUD slots, so the desire-tool
// can perform full CRUD on either set of tables via .XxxDesireStatus().
func twoDBClients(ctx context.Context) (specsDB, statusDB database.KubeApplierDBClient, cleanup func()) {
	client := newDynamoDBClient(ctx)
	specsDB = database.NewDynamoDBKubeApplierDBClient(client, client, flagSpecsPrefix, flagSpecsPrefix)
	statusDB = database.NewDynamoDBKubeApplierDBClient(client, client, flagStatusPrefix, flagStatusPrefix)
	return specsDB, statusDB, func() { specsDB.Close(); statusDB.Close() }
}

// --- create ---

func newCreateApplyCmd() *cobra.Command {
	var taskKey, clusterID, nodePool, filePath string

	cmd := &cobra.Command{
		Use:   "create-apply",
		Short: "Create an ApplyDesire from a YAML/JSON manifest file",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()

			raw, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("reading manifest file: %w", err)
			}

			raw, err = sigsyaml.YAMLToJSON(raw)
			if err != nil {
				return fmt.Errorf("converting manifest to JSON: %w", err)
			}

			ref, err := resourceRefFromManifest(raw)
			if err != nil {
				return fmt.Errorf("parsing manifest: %w", err)
			}

			docID := desireid.NewDocumentID(taskKey, ref.Group, ref.Version, ref.Resource, ref.Namespace, ref.Name)

			desire := &kubeapplier.ApplyDesire{
				Spec: kubeapplier.ApplyDesireSpec{
					ManagementCluster: "dev-local",
					ClusterID:         clusterID,
					NodePoolName:      nodePool,
					TargetItem:        ref,
					ServerSideApply:   &kubeapplier.ServerSideApplyConfig{KubeContent: &runtime.RawExtension{Raw: raw}},
				},
			}
			desire.SetDocumentID(docID)

			created, err := specsDB.ApplyDesireStatus().Create(ctx, desire)
			if err != nil {
				return fmt.Errorf("creating ApplyDesire: %w", err)
			}

			fmt.Printf("Created ApplyDesire: %s\n", created.GetDocumentID())
			fmt.Printf("  Target: %s/%s %s/%s\n", ref.Version, ref.Resource, ref.Namespace, ref.Name)
			return nil
		},
	}

	cmd.Flags().StringVar(&taskKey, "task-key", "", "Task key for UUID v5 document ID generation (required)")
	cmd.Flags().StringVar(&clusterID, "cluster-id", "", "Cluster ID (required)")
	cmd.Flags().StringVar(&nodePool, "node-pool", "", "Node pool name (optional)")
	cmd.Flags().StringVar(&filePath, "file", "", "Path to YAML/JSON manifest (required)")
	cmd.MarkFlagRequired("task-key")
	cmd.MarkFlagRequired("cluster-id")
	cmd.MarkFlagRequired("file")

	return cmd
}

func newCreateDeleteCmd() *cobra.Command {
	var taskKey, clusterID, nodePool string
	var group, version, resource, namespace, resourceName string

	cmd := &cobra.Command{
		Use:   "create-delete",
		Short: "Create an ApplyDesire (Type=Delete) targeting a specific resource",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()

			docID := desireid.NewDocumentID(taskKey, group, version, resource, namespace, resourceName)

			desire := &kubeapplier.ApplyDesire{
				Spec: kubeapplier.ApplyDesireSpec{
					ManagementCluster: "dev-local",
					ClusterID:         clusterID,
					NodePoolName:      nodePool,
					Type:              kubeapplier.ApplyDesireTypeDelete,
					TargetItem: kubeapplier.ResourceReference{
						Group:     group,
						Version:   version,
						Resource:  resource,
						Namespace: namespace,
						Name:      resourceName,
					},
				},
			}
			desire.SetDocumentID(docID)

			created, err := specsDB.ApplyDesireStatus().Create(ctx, desire)
			if err != nil {
				return fmt.Errorf("creating ApplyDesire (Delete): %w", err)
			}

			fmt.Printf("Created ApplyDesire (Type=Delete): %s\n", created.GetDocumentID())
			fmt.Printf("  Target: %s/%s %s/%s\n", version, resource, namespace, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVar(&taskKey, "task-key", "", "Task key for UUID v5 document ID generation (required)")
	cmd.Flags().StringVar(&clusterID, "cluster-id", "", "Cluster ID (required)")
	cmd.Flags().StringVar(&nodePool, "node-pool", "", "Node pool name (optional)")
	cmd.Flags().StringVar(&group, "group", "", "API group (empty for core)")
	cmd.Flags().StringVar(&version, "version", "", "API version (required)")
	cmd.Flags().StringVar(&resource, "resource", "", "Resource type (required)")
	cmd.Flags().StringVar(&namespace, "namespace", "", "Namespace (empty for cluster-scoped)")
	cmd.Flags().StringVar(&resourceName, "resource-name", "", "Resource name (required)")
	cmd.MarkFlagRequired("task-key")
	cmd.MarkFlagRequired("cluster-id")
	cmd.MarkFlagRequired("version")
	cmd.MarkFlagRequired("resource")
	cmd.MarkFlagRequired("resource-name")

	return cmd
}

func newCreateReadCmd() *cobra.Command {
	var taskKey, clusterID, nodePool string
	var group, version, resource, namespace, resourceName string

	cmd := &cobra.Command{
		Use:   "create-read",
		Short: "Create a ReadDesire targeting a specific resource",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()

			docID := desireid.NewDocumentID(taskKey, group, version, resource, namespace, resourceName)

			desire := &kubeapplier.ReadDesire{
				Spec: kubeapplier.ReadDesireSpec{
					ManagementCluster: "dev-local",
					ClusterID:         clusterID,
					NodePoolName:      nodePool,
					TargetItem: kubeapplier.ResourceReference{
						Group:     group,
						Version:   version,
						Resource:  resource,
						Namespace: namespace,
						Name:      resourceName,
					},
				},
			}
			desire.SetDocumentID(docID)

			created, err := specsDB.ReadDesireStatus().Create(ctx, desire)
			if err != nil {
				return fmt.Errorf("creating ReadDesire: %w", err)
			}

			fmt.Printf("Created ReadDesire: %s\n", created.GetDocumentID())
			fmt.Printf("  Target: %s/%s %s/%s\n", version, resource, namespace, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVar(&taskKey, "task-key", "", "Task key for UUID v5 document ID generation (required)")
	cmd.Flags().StringVar(&clusterID, "cluster-id", "", "Cluster ID (required)")
	cmd.Flags().StringVar(&nodePool, "node-pool", "", "Node pool name (optional)")
	cmd.Flags().StringVar(&group, "group", "", "API group (empty for core)")
	cmd.Flags().StringVar(&version, "version", "", "API version (required)")
	cmd.Flags().StringVar(&resource, "resource", "", "Resource type (required)")
	cmd.Flags().StringVar(&namespace, "namespace", "", "Namespace (empty for cluster-scoped)")
	cmd.Flags().StringVar(&resourceName, "resource-name", "", "Resource name (required)")
	cmd.MarkFlagRequired("task-key")
	cmd.MarkFlagRequired("cluster-id")
	cmd.MarkFlagRequired("version")
	cmd.MarkFlagRequired("resource")
	cmd.MarkFlagRequired("resource-name")

	return cmd
}

// --- update ---

func newUpdateApplyCmd() *cobra.Command {
	var f docIDFlags
	var filePath string

	cmd := &cobra.Command{
		Use:   "update-apply",
		Short: "Update an existing ApplyDesire's manifest (read-modify-write with optimistic concurrency)",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()

			existing, err := specsDB.ApplyDesireStatus().Get(ctx, docID)
			if err != nil {
				return fmt.Errorf("getting ApplyDesire %s: %w", docID, err)
			}

			raw, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("reading manifest file: %w", err)
			}

			raw, err = sigsyaml.YAMLToJSON(raw)
			if err != nil {
				return fmt.Errorf("converting manifest to JSON: %w", err)
			}

			ref, err := resourceRefFromManifest(raw)
			if err != nil {
				return fmt.Errorf("parsing manifest: %w", err)
			}

			existing.SetSpecKubeContent(&runtime.RawExtension{Raw: raw})
			existing.Spec.TargetItem = ref

			updated, err := specsDB.ApplyDesireStatus().Replace(ctx, existing)
			if err != nil {
				return fmt.Errorf("updating ApplyDesire: %w", err)
			}

			fmt.Printf("Updated ApplyDesire: %s\n", updated.GetDocumentID())
			fmt.Printf("  Target: %s/%s %s/%s\n", ref.Version, ref.Resource, ref.Namespace, ref.Name)
			return nil
		},
	}

	f.addFlags(cmd)
	cmd.Flags().StringVar(&filePath, "file", "", "Path to updated YAML/JSON manifest (required)")
	cmd.MarkFlagRequired("file")

	return cmd
}

func newUpdateReadCmd() *cobra.Command {
	var f docIDFlags
	var newGroup, newVersion, newResource, newNamespace, newResourceName string

	cmd := &cobra.Command{
		Use:   "update-read",
		Short: "Update an existing ReadDesire's target (read-modify-write with optimistic concurrency)",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()

			existing, err := specsDB.ReadDesireStatus().Get(ctx, docID)
			if err != nil {
				return fmt.Errorf("getting ReadDesire %s: %w", docID, err)
			}

			existing.Spec.TargetItem = kubeapplier.ResourceReference{
				Group:     newGroup,
				Version:   newVersion,
				Resource:  newResource,
				Namespace: newNamespace,
				Name:      newResourceName,
			}

			updated, err := specsDB.ReadDesireStatus().Replace(ctx, existing)
			if err != nil {
				return fmt.Errorf("updating ReadDesire: %w", err)
			}

			fmt.Printf("Updated ReadDesire: %s\n", updated.GetDocumentID())
			fmt.Printf("  Target: %s/%s %s/%s\n", newVersion, newResource, newNamespace, newResourceName)
			return nil
		},
	}

	f.addFlags(cmd)
	cmd.Flags().StringVar(&newGroup, "new-group", "", "New API group (empty for core)")
	cmd.Flags().StringVar(&newVersion, "new-version", "", "New API version (required)")
	cmd.Flags().StringVar(&newResource, "new-resource", "", "New resource type (required)")
	cmd.Flags().StringVar(&newNamespace, "new-namespace", "", "New namespace (empty for cluster-scoped)")
	cmd.Flags().StringVar(&newResourceName, "new-resource-name", "", "New resource name (required)")
	cmd.MarkFlagRequired("new-version")
	cmd.MarkFlagRequired("new-resource")
	cmd.MarkFlagRequired("new-resource-name")

	return cmd
}

// --- get ---

func newGetApplyCmd() *cobra.Command {
	var f docIDFlags

	cmd := &cobra.Command{
		Use:   "get-apply",
		Short: "Get a single ApplyDesire (spec from specs-table, status from status-table)",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, statusDB, cleanup := twoDBClients(ctx)
			defer cleanup()
			return getApply(ctx, specsDB, statusDB, docID)
		},
	}

	f.addFlags(cmd)
	return cmd
}

func newGetDeleteCmd() *cobra.Command {
	var f docIDFlags

	cmd := &cobra.Command{
		Use:   "get-delete",
		Short: "Get a single DeleteDesire (spec from specs-table, status from status-table)",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, statusDB, cleanup := twoDBClients(ctx)
			defer cleanup()
			return getDelete(ctx, specsDB, statusDB, docID)
		},
	}

	f.addFlags(cmd)
	return cmd
}

func newGetReadCmd() *cobra.Command {
	var f docIDFlags

	cmd := &cobra.Command{
		Use:   "get-read",
		Short: "Get a single ReadDesire (spec from specs-table, status from status-table)",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, statusDB, cleanup := twoDBClients(ctx)
			defer cleanup()
			return getRead(ctx, specsDB, statusDB, docID)
		},
	}

	f.addFlags(cmd)
	return cmd
}

func getApply(ctx context.Context, specsDB, statusDB database.KubeApplierDBClient, docID string) error {
	spec, err := specsDB.ApplyDesireSpecs().Get(ctx, docID)
	if err != nil {
		return fmt.Errorf("getting ApplyDesire spec: %w", err)
	}
	printDesireHeader("ApplyDesire", spec.GetDocumentID(), spec.GetUpdateTime().String(), spec.GetCreateTime().String())
	fmt.Printf("  Cluster ID:   %s\n", spec.Spec.ClusterID)
	if spec.Spec.NodePoolName != "" {
		fmt.Printf("  Node Pool:    %s\n", spec.Spec.NodePoolName)
	}
	printTargetItem(spec.Spec.TargetItem)
	if spec.GetSpecKubeContent() != nil {
		fmt.Printf("  KubeContent:\n")
		printIndentedJSON(spec.GetSpecKubeContent().Raw, 4)
	}

	status, err := statusDB.ApplyDesireStatus().Get(ctx, docID)
	if err != nil {
		if database.IsNotFoundError(err) {
			fmt.Printf("  Status:       (no status document yet)\n")
			return nil
		}
		return fmt.Errorf("getting ApplyDesire status: %w", err)
	}
	if !status.Status.ObservedDesireUpdateTime.IsZero() {
		fmt.Printf("  ObservedDesireUpdateTime: %s\n", status.Status.ObservedDesireUpdateTime)
	}
	if status.Status.AppliedResourceGeneration != 0 {
		fmt.Printf("  AppliedResourceGeneration: %d\n", status.Status.AppliedResourceGeneration)
	}
	printConditions(status.Status.Conditions)
	return nil
}

func getDelete(ctx context.Context, specsDB, statusDB database.KubeApplierDBClient, docID string) error {
	spec, err := specsDB.ApplyDesireSpecs().Get(ctx, docID)
	if err != nil {
		return fmt.Errorf("getting ApplyDesire (Delete) spec: %w", err)
	}
	printDesireHeader("ApplyDesire (Type=Delete)", spec.GetDocumentID(), spec.GetUpdateTime().String(), spec.GetCreateTime().String())
	fmt.Printf("  Cluster ID:   %s\n", spec.Spec.ClusterID)
	if spec.Spec.NodePoolName != "" {
		fmt.Printf("  Node Pool:    %s\n", spec.Spec.NodePoolName)
	}
	printTargetItem(spec.Spec.TargetItem)

	status, err := statusDB.ApplyDesireStatus().Get(ctx, docID)
	if err != nil {
		if database.IsNotFoundError(err) {
			fmt.Printf("  Status:       (no status document yet)\n")
			return nil
		}
		return fmt.Errorf("getting ApplyDesire (Delete) status: %w", err)
	}
	if !status.Status.ObservedDesireUpdateTime.IsZero() {
		fmt.Printf("  ObservedDesireUpdateTime: %s\n", status.Status.ObservedDesireUpdateTime)
	}
	printConditions(status.Status.Conditions)
	return nil
}

func getRead(ctx context.Context, specsDB, statusDB database.KubeApplierDBClient, docID string) error {
	spec, err := specsDB.ReadDesireSpecs().Get(ctx, docID)
	if err != nil {
		return fmt.Errorf("getting ReadDesire spec: %w", err)
	}
	printDesireHeader("ReadDesire", spec.GetDocumentID(), spec.GetUpdateTime().String(), spec.GetCreateTime().String())
	fmt.Printf("  Cluster ID:   %s\n", spec.Spec.ClusterID)
	if spec.Spec.NodePoolName != "" {
		fmt.Printf("  Node Pool:    %s\n", spec.Spec.NodePoolName)
	}
	printTargetItem(spec.Spec.TargetItem)

	status, err := statusDB.ReadDesireStatus().Get(ctx, docID)
	if err != nil {
		if database.IsNotFoundError(err) {
			fmt.Printf("  Status:       (no status document yet)\n")
			return nil
		}
		return fmt.Errorf("getting ReadDesire status: %w", err)
	}
	if !status.Status.ObservedDesireUpdateTime.IsZero() {
		fmt.Printf("  ObservedDesireUpdateTime: %s\n", status.Status.ObservedDesireUpdateTime)
	}
	printConditions(status.Status.Conditions)
	if status.Status.KubeContent != nil && len(status.Status.KubeContent.Raw) > 0 {
		fmt.Printf("  KubeContent (observed):\n")
		printIndentedJSON(status.Status.KubeContent.Raw, 4)
	}
	return nil
}

// --- list ---

func newListApplyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-apply",
		Short: "List all ApplyDesires (from specs tables)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()
			return listApply(ctx, specsDB)
		},
	}
}

func newListDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-delete",
		Short: "List all DeleteDesires (from specs tables)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()
			return listDelete(ctx, specsDB)
		},
	}
}

func newListReadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-read",
		Short: "List all ReadDesires (from specs tables)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			specsDB, _, cleanup := twoDBClients(ctx)
			defer cleanup()
			return listRead(ctx, specsDB)
		},
	}
}

func listApply(ctx context.Context, specsDB database.KubeApplierDBClient) error {
	desires, err := specsDB.ApplyDesireSpecs().List(ctx)
	if err != nil {
		return fmt.Errorf("listing ApplyDesires: %w", err)
	}
	if len(desires) == 0 {
		fmt.Println("No ApplyDesires found.")
		return nil
	}
	fmt.Printf("%-40s %-30s\n", "DOCUMENT ID", "TARGET")
	for _, d := range desires {
		target := fmt.Sprintf("%s/%s %s/%s", d.Spec.TargetItem.Version, d.Spec.TargetItem.Resource, d.Spec.TargetItem.Namespace, d.Spec.TargetItem.Name)
		fmt.Printf("%-40s %-30s\n", d.GetDocumentID(), target)
	}
	return nil
}

func listDelete(ctx context.Context, specsDB database.KubeApplierDBClient) error {
	all, err := specsDB.ApplyDesireSpecs().List(ctx)
	if err != nil {
		return fmt.Errorf("listing ApplyDesires: %w", err)
	}
	var desires []*kubeapplier.ApplyDesire
	for _, d := range all {
		if d.Spec.Type == kubeapplier.ApplyDesireTypeDelete {
			desires = append(desires, d)
		}
	}
	if len(desires) == 0 {
		fmt.Println("No ApplyDesires (Type=Delete) found.")
		return nil
	}
	fmt.Printf("%-40s %-30s\n", "DOCUMENT ID", "TARGET")
	for _, d := range desires {
		target := fmt.Sprintf("%s/%s %s/%s", d.Spec.TargetItem.Version, d.Spec.TargetItem.Resource, d.Spec.TargetItem.Namespace, d.Spec.TargetItem.Name)
		fmt.Printf("%-40s %-30s\n", d.GetDocumentID(), target)
	}
	return nil
}

func listRead(ctx context.Context, specsDB database.KubeApplierDBClient) error {
	desires, err := specsDB.ReadDesireSpecs().List(ctx)
	if err != nil {
		return fmt.Errorf("listing ReadDesires: %w", err)
	}
	if len(desires) == 0 {
		fmt.Println("No ReadDesires found.")
		return nil
	}
	fmt.Printf("%-40s %-30s\n", "DOCUMENT ID", "TARGET")
	for _, d := range desires {
		target := fmt.Sprintf("%s/%s %s/%s", d.Spec.TargetItem.Version, d.Spec.TargetItem.Resource, d.Spec.TargetItem.Namespace, d.Spec.TargetItem.Name)
		fmt.Printf("%-40s %-30s\n", d.GetDocumentID(), target)
	}
	return nil
}

// --- delete ---

func newDeleteApplyCmd() *cobra.Command {
	var f docIDFlags

	cmd := &cobra.Command{
		Use:   "delete-apply",
		Short: "Delete an ApplyDesire document from both specs and status tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, statusDB, cleanup := twoDBClients(ctx)
			defer cleanup()

			var errs []string
			if err := specsDB.ApplyDesireStatus().Delete(ctx, docID); err != nil {
				if !database.IsNotFoundError(err) {
					errs = append(errs, fmt.Sprintf("specs-table: %v", err))
				}
			}
			if err := statusDB.ApplyDesireStatus().Delete(ctx, docID); err != nil {
				if !database.IsNotFoundError(err) {
					errs = append(errs, fmt.Sprintf("status-table: %v", err))
				}
			}
			if len(errs) > 0 {
				return fmt.Errorf("deleting ApplyDesire: %s", strings.Join(errs, "; "))
			}
			fmt.Printf("Deleted ApplyDesire %s\n", docID)
			return nil
		},
	}

	f.addFlags(cmd)
	return cmd
}

func newDeleteDeleteCmd() *cobra.Command {
	var f docIDFlags

	cmd := &cobra.Command{
		Use:   "delete-delete",
		Short: "Delete an ApplyDesire (Type=Delete) document from both specs and status tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, statusDB, cleanup := twoDBClients(ctx)
			defer cleanup()

			var errs []string
			if err := specsDB.ApplyDesireStatus().Delete(ctx, docID); err != nil {
				if !database.IsNotFoundError(err) {
					errs = append(errs, fmt.Sprintf("specs-table: %v", err))
				}
			}
			if err := statusDB.ApplyDesireStatus().Delete(ctx, docID); err != nil {
				if !database.IsNotFoundError(err) {
					errs = append(errs, fmt.Sprintf("status-table: %v", err))
				}
			}
			if len(errs) > 0 {
				return fmt.Errorf("deleting ApplyDesire (Delete): %s", strings.Join(errs, "; "))
			}
			fmt.Printf("Deleted ApplyDesire (Type=Delete) %s\n", docID)
			return nil
		},
	}

	f.addFlags(cmd)
	return cmd
}

func newDeleteReadCmd() *cobra.Command {
	var f docIDFlags

	cmd := &cobra.Command{
		Use:   "delete-read",
		Short: "Delete a ReadDesire document from both specs and status tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			docID, err := f.resolve()
			if err != nil {
				return err
			}
			ctx := context.Background()
			specsDB, statusDB, cleanup := twoDBClients(ctx)
			defer cleanup()

			var errs []string
			if err := specsDB.ReadDesireStatus().Delete(ctx, docID); err != nil {
				if !database.IsNotFoundError(err) {
					errs = append(errs, fmt.Sprintf("specs-table: %v", err))
				}
			}
			if err := statusDB.ReadDesireStatus().Delete(ctx, docID); err != nil {
				if !database.IsNotFoundError(err) {
					errs = append(errs, fmt.Sprintf("status-table: %v", err))
				}
			}
			if len(errs) > 0 {
				return fmt.Errorf("deleting ReadDesire: %s", strings.Join(errs, "; "))
			}
			fmt.Printf("Deleted ReadDesire %s\n", docID)
			return nil
		},
	}

	f.addFlags(cmd)
	return cmd
}

// --- doc-id resolution ---

type docIDFlags struct {
	docID        string
	taskKey      string
	group        string
	version      string
	resource     string
	namespace    string
	resourceName string
}

func (f *docIDFlags) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.docID, "doc-id", "", "Document ID (UUID); alternative to --task-key + GVR flags")
	cmd.Flags().StringVar(&f.taskKey, "task-key", "", "Task key for UUID v5 document ID derivation")
	cmd.Flags().StringVar(&f.group, "group", "", "API group (empty for core)")
	cmd.Flags().StringVar(&f.version, "version", "", "API version")
	cmd.Flags().StringVar(&f.resource, "resource", "", "Resource type")
	cmd.Flags().StringVar(&f.namespace, "namespace", "", "Namespace (empty for cluster-scoped)")
	cmd.Flags().StringVar(&f.resourceName, "resource-name", "", "Resource name")
}

func (f *docIDFlags) resolve() (string, error) {
	if f.docID != "" {
		return f.docID, nil
	}
	if f.taskKey == "" || f.version == "" || f.resource == "" || f.resourceName == "" {
		return "", fmt.Errorf("provide either --doc-id or all of --task-key, --version, --resource, --resource-name")
	}
	return desireid.NewDocumentID(f.taskKey, f.group, f.version, f.resource, f.namespace, f.resourceName), nil
}

// --- helpers ---

func resourceRefFromManifest(raw []byte) (kubeapplier.ResourceReference, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return kubeapplier.ResourceReference{}, fmt.Errorf("manifest must be JSON: %w", err)
	}

	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	metadata, _ := obj["metadata"].(map[string]any)

	if apiVersion == "" || kind == "" {
		return kubeapplier.ResourceReference{}, fmt.Errorf("manifest must have apiVersion and kind")
	}

	ref := kubeapplier.ResourceReference{
		Name: strFromMap(metadata, "name"),
	}
	if ns := strFromMap(metadata, "namespace"); ns != "" {
		ref.Namespace = ns
	}

	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 2 {
		ref.Group = parts[0]
		ref.Version = parts[1]
	} else {
		ref.Version = apiVersion
	}

	ref.Resource = guessResource(kind)

	return ref, nil
}

func strFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

func guessResource(kind string) string {
	switch kind {
	case "ConfigMap":
		return "configmaps"
	case "Secret":
		return "secrets"
	case "Namespace":
		return "namespaces"
	case "Service":
		return "services"
	case "ServiceAccount":
		return "serviceaccounts"
	case "Deployment":
		return "deployments"
	case "StatefulSet":
		return "statefulsets"
	case "DaemonSet":
		return "daemonsets"
	case "Job":
		return "jobs"
	case "CronJob":
		return "cronjobs"
	case "Pod":
		return "pods"
	case "ClusterRole":
		return "clusterroles"
	case "ClusterRoleBinding":
		return "clusterrolebindings"
	case "Role":
		return "roles"
	case "RoleBinding":
		return "rolebindings"
	default:
		return fmt.Sprintf("%ss", toLowerFirst(kind))
	}
}

func toLowerFirst(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'A' && b[0] <= 'Z' {
		b[0] += 'a' - 'A'
	}
	return string(b)
}

func printDesireHeader(kind, docID, updateTime, createTime string) {
	fmt.Printf("%s: %s\n", kind, docID)
	fmt.Printf("  Created:      %s\n", createTime)
	fmt.Printf("  Updated:      %s\n", updateTime)
}

func printTargetItem(ref kubeapplier.ResourceReference) {
	fmt.Printf("  Target Item:\n")
	if ref.Group != "" {
		fmt.Printf("    Group:      %s\n", ref.Group)
	}
	fmt.Printf("    Version:    %s\n", ref.Version)
	fmt.Printf("    Resource:   %s\n", ref.Resource)
	if ref.Namespace != "" {
		fmt.Printf("    Namespace:  %s\n", ref.Namespace)
	}
	fmt.Printf("    Name:       %s\n", ref.Name)
}

func printConditions(conditions []metav1.Condition) {
	if len(conditions) == 0 {
		fmt.Printf("  Conditions:   (none)\n")
		return
	}
	fmt.Printf("  Conditions:\n")
	for _, c := range conditions {
		fmt.Printf("    - Type: %s, Status: %s, Reason: %s\n", c.Type, string(c.Status), c.Reason)
		if c.Message != "" {
			fmt.Printf("      Message: %s\n", c.Message)
		}
		if !c.LastTransitionTime.IsZero() {
			fmt.Printf("      LastTransition: %s\n", c.LastTransitionTime.Time.String())
		}
	}
}

func printIndentedJSON(raw []byte, indent int) {
	var obj any
	if err := json.Unmarshal(raw, &obj); err != nil {
		fmt.Printf("%*s%s\n", indent, "", string(raw))
		return
	}
	formatted, err := json.MarshalIndent(obj, fmt.Sprintf("%*s", indent, ""), "  ")
	if err != nil {
		fmt.Printf("%*s%s\n", indent, "", string(raw))
		return
	}
	fmt.Printf("%*s%s\n", indent, "", string(formatted))
}
