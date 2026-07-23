// Package statussnspublisher publishes status change notifications to SNS after
// kube-applier writes a status document to DynamoDB. The hyperfleet-operator
// polls its own per-replica SQS queue (subscribed to this SNS topic) to learn
// about status changes without tailing DynamoDB Streams.
package statussnspublisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// SNSClient is the subset of the AWS SNS API used by Publisher.
// It is a narrow interface so that tests can substitute a mock.
type SNSClient interface {
	Publish(ctx context.Context, in *sns.PublishInput, opts ...func(*sns.Options)) (*sns.PublishOutput, error)
}

// StatusNotification is the JSON payload sent to SNS after a status write.
// The hyperfleet-operator decodes this from its SQS queue to know which
// document changed and re-reads from DynamoDB.
type StatusNotification struct {
	DocumentID  string `json:"documentID"`
	TableSuffix string `json:"tableSuffix"` // e.g. "-applydesires" or "-readdesires"
}

// Publisher sends StatusNotification messages to a pre-configured SNS topic.
type Publisher struct {
	client   SNSClient
	topicARN string
}

// New returns a Publisher that publishes to topicARN.
func New(client SNSClient, topicARN string) *Publisher {
	return &Publisher{
		client:   client,
		topicARN: topicARN,
	}
}

// TopicARN returns the SNS topic ARN this publisher publishes to.
func (p *Publisher) TopicARN() string {
	return p.topicARN
}

// Publish sends a StatusNotification to the SNS topic. It is best-effort: on
// failure it returns an error that callers should log but need not propagate —
// the operator's 5-minute safety-net poll covers missed notifications.
func (p *Publisher) Publish(ctx context.Context, documentID, tableSuffix string) error {
	notification := StatusNotification{
		DocumentID:  documentID,
		TableSuffix: tableSuffix,
	}
	body, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("marshal SNS status notification: %w", err)
	}

	_, err = p.client.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(p.topicARN),
		Message:  aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("sns publish status to %s: %w", p.topicARN, err)
	}
	return nil
}
