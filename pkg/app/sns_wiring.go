package app

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// NewSNSClient creates an SNS client from the provided AWS config.
// The same aws.Config used for DynamoDB and SQS is reused — same region and
// credentials, no additional configuration needed.
func NewSNSClient(cfg aws.Config) *sns.Client {
	return sns.NewFromConfig(cfg)
}
