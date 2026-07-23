package statussnspublisher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type spySNSClient struct {
	publishedInputs []*sns.PublishInput
	err             error
}

func (s *spySNSClient) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	s.publishedInputs = append(s.publishedInputs, in)
	return &sns.PublishOutput{}, s.err
}

func TestPublish_SendsCorrectPayload(t *testing.T) {
	spy := &spySNSClient{}
	p := New(spy, "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications")

	if err := p.Publish(context.Background(), "doc-abc", "-applydesires"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(spy.publishedInputs) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(spy.publishedInputs))
	}

	in := spy.publishedInputs[0]
	if got := *in.TopicArn; got != "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications" {
		t.Errorf("TopicArn = %q, want %q", got, "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications")
	}

	var notif StatusNotification
	if err := json.Unmarshal([]byte(*in.Message), &notif); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	if notif.DocumentID != "doc-abc" {
		t.Errorf("DocumentID = %q, want %q", notif.DocumentID, "doc-abc")
	}
	if notif.TableSuffix != "-applydesires" {
		t.Errorf("TableSuffix = %q, want %q", notif.TableSuffix, "-applydesires")
	}
}

func TestPublish_ReadDesireSuffix(t *testing.T) {
	spy := &spySNSClient{}
	p := New(spy, "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications")

	if err := p.Publish(context.Background(), "doc-xyz", "-readdesires"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var notif StatusNotification
	if err := json.Unmarshal([]byte(*spy.publishedInputs[0].Message), &notif); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if notif.TableSuffix != "-readdesires" {
		t.Errorf("TableSuffix = %q, want -readdesires", notif.TableSuffix)
	}
}

func TestPublish_ReturnsErrorOnSNSFailure(t *testing.T) {
	snsErr := errors.New("sns unavailable")
	spy := &spySNSClient{err: snsErr}
	p := New(spy, "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications")

	err := p.Publish(context.Background(), "doc-1", "-applydesires")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, snsErr) {
		t.Errorf("error = %v, want to wrap %v", err, snsErr)
	}
}

func TestTopicARN(t *testing.T) {
	p := New(nil, "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications")
	if got := p.TopicARN(); got != "arn:aws:sns:us-east-1:123456789012:mc01-status-notifications" {
		t.Errorf("TopicARN() = %q", got)
	}
}
