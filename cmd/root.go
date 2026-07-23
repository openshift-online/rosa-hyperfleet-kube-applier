package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"k8s.io/klog/v2"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/informers"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/statussnspublisher"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/pkg/app"
)

// KubeApplierRootCmdFlags collects the user-facing flags for the kube-applier
// binary. Required values must be supplied as flags; the binary does not read
// from environment variables so a misconfigured pod fails fast.
type KubeApplierRootCmdFlags struct {
	Kubeconfig                 string
	KubeNamespace              string
	ManagementCluster          string
	AWSRegion                  string
	AWSEndpointURL             string
	SQSQueueURL                string
	SNSStatusTopicARN          string
	SpecsTablePrefix           string
	StatusTablePrefix          string
	MetricsServerListenAddress string
	HealthzServerListenAddress string
	LeaderElectionID           string
	LogVerbosity               int
	ExitOnPanic                bool
	KubeQPS                    float32
	KubeBurst                  int
}

func (f *KubeApplierRootCmdFlags) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.Kubeconfig, "kubeconfig", f.Kubeconfig,
		"Absolute path to the kubeconfig file. Empty selects the in-cluster config.")
	cmd.Flags().StringVar(&f.KubeNamespace, "namespace", f.KubeNamespace,
		"Kubernetes namespace that hosts the leader-election lease.")
	cmd.Flags().StringVar(&f.ManagementCluster, "management-cluster", f.ManagementCluster,
		"Name of the management cluster this pod runs in.")
	cmd.Flags().StringVar(&f.AWSRegion, "aws-region", f.AWSRegion,
		"AWS region where DynamoDB tables and SQS queue reside.")
	cmd.Flags().StringVar(&f.AWSEndpointURL, "aws-endpoint-url", f.AWSEndpointURL,
		"Optional AWS endpoint URL override (e.g. http://localhost:4566 for LocalStack).")
	cmd.Flags().StringVar(&f.SQSQueueURL, "sqs-queue-url", f.SQSQueueURL,
		"SQS queue URL for receiving spec change notifications from the hyperfleet-operator (required).")
	cmd.Flags().StringVar(&f.SNSStatusTopicARN, "sns-status-topic-arn", f.SNSStatusTopicARN,
		"SNS topic ARN for publishing status change notifications to the hyperfleet-operator (optional). "+
			"When set, a notification is published after every successful status write so the operator "+
			"is woken immediately rather than waiting for its 5-minute safety-net poll.")
	cmd.Flags().StringVar(&f.SpecsTablePrefix, "specs-table", f.SpecsTablePrefix,
		"DynamoDB table name prefix for spec tables (e.g. 'mc-dev-specs'; full table names are prefix+'-applydesires' etc.).")
	cmd.Flags().StringVar(&f.StatusTablePrefix, "status-table", f.StatusTablePrefix,
		"DynamoDB table name prefix for status tables (e.g. 'mc-dev-status').")
	cmd.Flags().StringVar(&f.MetricsServerListenAddress, "metrics-listen-address", f.MetricsServerListenAddress,
		"Address on which to expose Prometheus metrics.")
	cmd.Flags().StringVar(&f.HealthzServerListenAddress, "healthz-listen-address", f.HealthzServerListenAddress,
		"Address on which to expose the /healthz endpoint.")
	cmd.Flags().StringVar(&f.LeaderElectionID, "leader-election-id", f.LeaderElectionID,
		"Lease name used for leader election within --namespace.")
	cmd.Flags().IntVar(&f.LogVerbosity, "log-verbosity", f.LogVerbosity,
		"Log verbosity. 0 is INFO; higher values are more verbose.")
	cmd.Flags().BoolVar(&f.ExitOnPanic, "exit-on-panic", f.ExitOnPanic,
		"If set, the process exits on any goroutine panic via apimachinery's HandleCrash.")
	cmd.Flags().Float32Var(&f.KubeQPS, "kube-qps", f.KubeQPS,
		"Maximum QPS to the kube-apiserver from the dynamic client.")
	cmd.Flags().IntVar(&f.KubeBurst, "kube-burst", f.KubeBurst,
		"Maximum burst for throttle on requests to the kube-apiserver from the dynamic client.")

	for _, name := range []string{"namespace", "management-cluster", "aws-region", "sqs-queue-url"} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(fmt.Errorf("MarkFlagRequired(%q): %w", name, err))
		}
	}
}

func (f *KubeApplierRootCmdFlags) validate() error {
	if len(f.ManagementCluster) == 0 {
		return fmt.Errorf("--management-cluster must not be empty")
	}
	if len(f.AWSRegion) == 0 {
		return fmt.Errorf("--aws-region must not be empty")
	}
	if len(f.KubeNamespace) == 0 {
		return fmt.Errorf("--namespace must not be empty")
	}
	if len(f.SQSQueueURL) == 0 {
		return fmt.Errorf("--sqs-queue-url must not be empty")
	}
	if len(f.LeaderElectionID) == 0 {
		return fmt.Errorf("--leader-election-id must not be empty")
	}
	if f.LogVerbosity < 0 {
		return fmt.Errorf("--log-verbosity must be >= 0")
	}
	return nil
}

// ToKubeApplierOptions resolves flags into wired Options that the app layer
// consumes. Each external dependency is constructed here so that Run() never
// sees raw flag values.
func (f *KubeApplierRootCmdFlags) ToKubeApplierOptions(ctx context.Context) (*app.Options, error) {
	kubeconfig, err := app.NewKubeconfig(f.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes configuration: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	leaderElectionLock, err := app.NewLeaderElectionLock(hostname, kubeconfig, f.KubeNamespace, f.LeaderElectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to create leader election lock: %w", err)
	}

	specsPrefix := f.SpecsTablePrefix
	if specsPrefix == "" {
		specsPrefix = "mc-" + f.ManagementCluster + "-specs"
	}
	statusPrefix := f.StatusTablePrefix
	if statusPrefix == "" {
		statusPrefix = "mc-" + f.ManagementCluster + "-status"
	}

	awsCfg, err := app.NewDynamoDBConfig(ctx, f.AWSRegion, f.AWSEndpointURL)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	specsClient := app.NewDynamoDBClient(awsCfg)
	statusClient := app.NewDynamoDBClient(awsCfg)
	sqsClient := app.NewSQSClient(awsCfg)

	dbClient := database.NewDynamoDBKubeApplierDBClient(specsClient, statusClient, specsPrefix, statusPrefix)
	dynamoDBInformers := informers.NewKubeApplierInformers(specsClient, specsPrefix)

	dyn, err := app.NewDynamicClient(kubeconfig, f.KubeQPS, f.KubeBurst)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Build the status SNS publisher if a topic ARN was provided.
	// It is intentionally optional — omitting the flag disables status
	// notifications and the operator falls back to 5-minute safety polling.
	var snsPublisher *statussnspublisher.Publisher
	if f.SNSStatusTopicARN != "" {
		snsClient := app.NewSNSClient(awsCfg)
		snsPublisher = statussnspublisher.New(snsClient, f.SNSStatusTopicARN)
	}

	return &app.Options{
		ManagementCluster:          f.ManagementCluster,
		LeaderElectionLock:         leaderElectionLock,
		KubeApplierDBClient:        dbClient,
		Informers:                  dynamoDBInformers,
		DynamicClient:              dyn,
		SQSClient:                  sqsClient,
		SQSQueueURL:                f.SQSQueueURL,
		StatusSNSPublisher:         snsPublisher,
		MetricsServerListenAddress: f.MetricsServerListenAddress,
		HealthzServerListenAddress: f.HealthzServerListenAddress,
		ExitOnPanic:                f.ExitOnPanic,
	}, nil
}

func NewKubeApplierRootCmdFlags() *KubeApplierRootCmdFlags {
	return &KubeApplierRootCmdFlags{
		MetricsServerListenAddress: ":8081",
		HealthzServerListenAddress: ":8083",
		LeaderElectionID:           "kube-applier",
		LogVerbosity:               0,
		ExitOnPanic:                true,
		KubeQPS:                    250,
		KubeBurst:                  500,
	}
}

func NewCmdRoot() *cobra.Command {
	processName := filepath.Base(os.Args[0])

	flags := NewKubeApplierRootCmdFlags()

	cmd := &cobra.Command{
		Use:   processName,
		Args:  cobra.NoArgs,
		Short: app.AppShortDescriptionName,
		Long: fmt.Sprintf(`%s

  The kube-applier reconciles ApplyDesire, DeleteDesire, and ReadDesire
  documents stored in DynamoDB against the management cluster's local
  kube-apiserver.

  # Run kube-applier locally pointing at a LocalStack DynamoDB and the
  # in-cluster kubeconfig.
  %s --management-cluster ${MC_NAME} \
      --aws-region ${AWS_REGION} \
      --namespace ${NAMESPACE} \
      --sqs-queue-url ${SQS_QUEUE_URL}
`, app.AppShortDescriptionName, processName),
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunRootCmd(cmd, flags)
		},
		SilenceErrors: true,
	}

	cmd.SetErrPrefix(cmd.Short + " error:")
	flags.AddFlags(cmd)

	return cmd
}

func RunRootCmd(cmd *cobra.Command, flags *KubeApplierRootCmdFlags) error {
	if err := flags.validate(); err != nil {
		return fmt.Errorf("flags validation failed: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Configure klog verbosity directly so V(n) calls work without any
	// slog/logr bridge that can silently swallow verbose log levels.
	klog.InitFlags(nil)
	if err := flag.Set("v", fmt.Sprintf("%d", flags.LogVerbosity)); err != nil {
		return fmt.Errorf("failed to set klog verbosity: %w", err)
	}

	options, err := flags.ToKubeApplierOptions(ctx)
	if err != nil {
		return fmt.Errorf("failed to convert flags to options: %w", err)
	}

	if err := options.Run(ctx); err != nil {
		return fmt.Errorf("failed to run kube-applier: %w", err)
	}
	return nil
}
