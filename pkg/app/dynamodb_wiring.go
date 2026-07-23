package app

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewDynamoDBConfig loads the default AWS SDK configuration. region is
// required. If endpointURL is non-empty it overrides the service endpoint
// (useful for LocalStack or other non-AWS DynamoDB-compatible endpoints).
func NewDynamoDBConfig(ctx context.Context, region, endpointURL string) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}
	if endpointURL != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(endpointURL))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return cfg, nil
}

// NewDynamoDBClient creates a DynamoDB client from the provided AWS config.
func NewDynamoDBClient(cfg aws.Config) *dynamodb.Client {
	return dynamodb.NewFromConfig(cfg)
}
