// Package sqspoller polls an SQS queue for spec change notifications published
// by the hyperfleet-operator after writing a desire document to DynamoDB.
// On each notification the poller enqueues the document ID into the appropriate
// controller workqueue so the controller re-reads the full spec from DynamoDB
// and reconciles it.
//
// This replaces the DynamoDB Streams watcher as the incremental change
// notification mechanism. The startup full Scan (list side of the informer)
// is unchanged.
package sqspoller

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"k8s.io/klog/v2"
)

const (
	// maxMessages is the maximum number of SQS messages to retrieve per poll.
	// SQS allows up to 10 per ReceiveMessage call.
	maxMessages = 10

	// waitTimeSeconds is the SQS long-poll duration. The call blocks up to this
	// many seconds when the queue is empty, avoiding a busy loop.
	waitTimeSeconds = 20

	// retryDelay is the pause between retries after an SQS error.
	retryDelay = 5 * time.Second
)

// SQSClient is the subset of the AWS SQS API used by Poller.
// It is a narrow interface so that tests can substitute a mock.
type SQSClient interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// SpecNotification is the JSON payload delivered by SNS (via SQS) after the
// operator writes a desire document to DynamoDB. It matches the message format
// published by hyperfleet-operator/internal/dynamo/snspublisher.
type SpecNotification struct {
	DocumentID  string `json:"documentID"`
	TableSuffix string `json:"tableSuffix"` // e.g. "-applydesires" or "-readdesires"
}

// Poller receives SpecNotification messages from an SQS queue and enqueues
// the affected document IDs into the appropriate controller workqueue.
type Poller struct {
	client       SQSClient
	queueURL     string
	enqueueApply func(documentID string)
	enqueueRead  func(documentID string)
}

// New returns a Poller. enqueueApply is called for apply-desire document IDs;
// enqueueRead is called for read-desire document IDs.
func New(
	client SQSClient,
	queueURL string,
	enqueueApply func(documentID string),
	enqueueRead func(documentID string),
) *Poller {
	return &Poller{
		client:       client,
		queueURL:     queueURL,
		enqueueApply: enqueueApply,
		enqueueRead:  enqueueRead,
	}
}

// Run polls the SQS queue continuously until ctx is cancelled. It should be
// started in a goroutine after the informer caches have synced so that the
// startup full-scan wave completes before incremental SQS notifications are
// processed.
func (p *Poller) Run(ctx context.Context) {
	logger := klog.FromContext(ctx).WithName("SQSPoller").WithValues("queueURL", p.queueURL)
	logger.Info("SQS poller started")
	defer logger.Info("SQS poller stopped")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := p.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(p.queueURL),
			MaxNumberOfMessages: maxMessages,
			WaitTimeSeconds:     waitTimeSeconds,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error(err, "Failed to receive SQS messages; retrying", "retryDelay", retryDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
			continue
		}

		for _, msg := range msgs.Messages {
			p.handleMessage(ctx, logger, msg.Body, msg.ReceiptHandle)
		}
	}
}

func (p *Poller) handleMessage(ctx context.Context, logger klog.Logger, body *string, receiptHandle *string) {
	if body == nil {
		return
	}

	var notification SpecNotification
	if err := json.Unmarshal([]byte(*body), &notification); err != nil {
		logger.Error(err, "Failed to unmarshal SQS message; skipping", "body", *body)
		// Delete the malformed message so it doesn't poison the queue.
		p.deleteMessage(ctx, logger, receiptHandle)
		return
	}

	if notification.DocumentID == "" {
		logger.Info("Received SQS message with empty documentID; skipping")
		p.deleteMessage(ctx, logger, receiptHandle)
		return
	}

	switch {
	case strings.HasSuffix(notification.TableSuffix, "applydesires"):
		logger.V(4).Info("Enqueuing apply desire from SQS", "documentID", notification.DocumentID)
		p.enqueueApply(notification.DocumentID)
	case strings.HasSuffix(notification.TableSuffix, "readdesires"):
		logger.V(4).Info("Enqueuing read desire from SQS", "documentID", notification.DocumentID)
		p.enqueueRead(notification.DocumentID)
	default:
		logger.Info("Unknown tableSuffix in SQS notification; skipping",
			"tableSuffix", notification.TableSuffix,
			"documentID", notification.DocumentID,
		)
	}

	// Delete the message after enqueue. If the process crashes before this
	// point the message becomes visible again after the visibility timeout
	// and will be re-delivered — enqueue is idempotent.
	p.deleteMessage(ctx, logger, receiptHandle)
}

func (p *Poller) deleteMessage(ctx context.Context, logger klog.Logger, receiptHandle *string) {
	if receiptHandle == nil {
		return
	}
	if _, err := p.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(p.queueURL),
		ReceiptHandle: receiptHandle,
	}); err != nil {
		if ctx.Err() == nil {
			logger.Error(err, "Failed to delete SQS message", "receiptHandle", *receiptHandle)
		}
	}
}
