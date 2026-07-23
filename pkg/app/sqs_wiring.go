package app

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// NewSQSClient creates an SQS client from the provided AWS config.
// The same aws.Config used for DynamoDB is reused — same region and
// credentials, no additional configuration needed.
func NewSQSClient(cfg aws.Config) *sqs.Client {
	return sqs.NewFromConfig(cfg)
}
