// Package app wires the kube-applier binary together. It is invoked from
// cmd after flags have been parsed and external dependencies (kubeconfig,
// leader-election lock, DynamoDB clients) have been constructed.
package app

import (
	"github.com/prometheus/client_golang/prometheus"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/informers"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/sqspoller"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/statussnspublisher"
)

const AppShortDescriptionName = "AWS HCP kube-applier"

const (
	threadsApply       = 4
	threadsReadManager = 1
)

// Options is the wired bundle of dependencies the kube-applier needs to run.
type Options struct {
	ManagementCluster string

	LeaderElectionLock  resourcelock.Interface
	KubeApplierDBClient database.KubeApplierDBClient
	Informers           informers.KubeApplierInformers
	DynamicClient       dynamic.Interface

	// SQSClient and SQSQueueURL configure the SQS poller that receives spec
	// change notifications from the hyperfleet-operator. The poller starts
	// after the informer caches have synced and enqueues document IDs into
	// the controller workqueues for incremental reconciliation.
	SQSClient   sqspoller.SQSClient
	SQSQueueURL string

	// StatusSNSPublisher, when non-nil, wraps the status CRUD clients so that
	// a lightweight SNS notification is published after every successful status
	// write. The hyperfleet-operator's per-replica SQS queues subscribe to
	// this topic for immediate change notification. When nil, status writes
	// proceed normally and the operator falls back to 5-minute safety polling.
	StatusSNSPublisher *statussnspublisher.Publisher

	MetricsServerListenAddress string
	HealthzServerListenAddress string

	MetricsRegisterer prometheus.Registerer
	MetricsGatherer   prometheus.Gatherer

	ExitOnPanic bool
}
