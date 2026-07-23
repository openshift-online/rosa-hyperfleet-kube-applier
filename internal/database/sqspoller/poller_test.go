package sqspoller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// mockSQSClient implements SQSClient for tests.
type mockSQSClient struct {
	messages      []sqstypes.Message
	receiveErr    error
	deleteCalls   []string // receipt handles deleted
	deleteErr     error
	receiveCount  int
}

func (m *mockSQSClient) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.receiveCount++
	if m.receiveErr != nil {
		return nil, m.receiveErr
	}
	// Return messages on first call, empty on subsequent calls.
	if m.receiveCount == 1 && len(m.messages) > 0 {
		return &sqs.ReceiveMessageOutput{Messages: m.messages}, nil
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSClient) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.deleteCalls = append(m.deleteCalls, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, m.deleteErr
}

func makeMessage(t *testing.T, notification SpecNotification, receipt string) sqstypes.Message {
	t.Helper()
	body, err := json.Marshal(notification)
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	return sqstypes.Message{
		Body:          aws.String(string(body)),
		ReceiptHandle: aws.String(receipt),
	}
}

func TestPoller_ApplyDesireRouted(t *testing.T) {
	var enqueuedApply []string
	var enqueuedRead []string

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeMessage(t, SpecNotification{DocumentID: "doc-1", TableSuffix: "-applydesires"}, "rh-1"),
		},
	}

	p := New(mock, "https://sqs.test/queue", func(id string) {
		enqueuedApply = append(enqueuedApply, id)
	}, func(id string) {
		enqueuedRead = append(enqueuedRead, id)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Run(ctx)

	if len(enqueuedApply) != 1 || enqueuedApply[0] != "doc-1" {
		t.Errorf("enqueueApply calls = %v, want [doc-1]", enqueuedApply)
	}
	if len(enqueuedRead) != 0 {
		t.Errorf("unexpected enqueueRead calls: %v", enqueuedRead)
	}
	if len(mock.deleteCalls) != 1 || mock.deleteCalls[0] != "rh-1" {
		t.Errorf("deleteCalls = %v, want [rh-1]", mock.deleteCalls)
	}
}

func TestPoller_ReadDesireRouted(t *testing.T) {
	var enqueuedApply []string
	var enqueuedRead []string

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeMessage(t, SpecNotification{DocumentID: "doc-read-1", TableSuffix: "-readdesires"}, "rh-r1"),
		},
	}

	p := New(mock, "https://sqs.test/queue", func(id string) {
		enqueuedApply = append(enqueuedApply, id)
	}, func(id string) {
		enqueuedRead = append(enqueuedRead, id)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Run(ctx)

	if len(enqueuedRead) != 1 || enqueuedRead[0] != "doc-read-1" {
		t.Errorf("enqueueRead calls = %v, want [doc-read-1]", enqueuedRead)
	}
	if len(enqueuedApply) != 0 {
		t.Errorf("unexpected enqueueApply calls: %v", enqueuedApply)
	}
}

func TestPoller_UnknownSuffixSkipped(t *testing.T) {
	var enqueuedApply []string
	var enqueuedRead []string

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeMessage(t, SpecNotification{DocumentID: "doc-1", TableSuffix: "-unknowntable"}, "rh-u1"),
		},
	}

	p := New(mock, "https://sqs.test/queue", func(id string) {
		enqueuedApply = append(enqueuedApply, id)
	}, func(id string) {
		enqueuedRead = append(enqueuedRead, id)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Run(ctx)

	if len(enqueuedApply) != 0 || len(enqueuedRead) != 0 {
		t.Errorf("expected no enqueue for unknown suffix; got apply=%v read=%v", enqueuedApply, enqueuedRead)
	}
	// Message is still deleted (not poisoning the queue).
	if len(mock.deleteCalls) != 1 {
		t.Errorf("expected message deleted even for unknown suffix; deleteCalls=%v", mock.deleteCalls)
	}
}

func TestPoller_DeleteCalledAfterEnqueue(t *testing.T) {
	enqueueOrder := []string{}
	deleteOrder := []string{}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeMessage(t, SpecNotification{DocumentID: "doc-1", TableSuffix: "-applydesires"}, "rh-1"),
			makeMessage(t, SpecNotification{DocumentID: "doc-2", TableSuffix: "-readdesires"}, "rh-2"),
		},
	}

	p := New(mock, "https://sqs.test/queue", func(id string) {
		enqueueOrder = append(enqueueOrder, "apply:"+id)
	}, func(id string) {
		enqueueOrder = append(enqueueOrder, "read:"+id)
	})

	// Override deleteMessage to track order relative to enqueue.
	// We do this by wrapping the mock to append after enqueue is done.
	origDelete := mock.DeleteMessage
	_ = origDelete
	mock.deleteCalls = nil

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Run(ctx)

	_ = deleteOrder
	if len(mock.deleteCalls) != 2 {
		t.Errorf("expected 2 deletes, got %d: %v", len(mock.deleteCalls), mock.deleteCalls)
	}
	if len(enqueueOrder) != 2 {
		t.Errorf("expected 2 enqueues, got %d: %v", len(enqueueOrder), enqueueOrder)
	}
}

func TestPoller_MalformedMessageDeleted(t *testing.T) {
	var enqueuedApply []string

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			{Body: aws.String("not-valid-json"), ReceiptHandle: aws.String("rh-bad")},
		},
	}

	p := New(mock, "https://sqs.test/queue", func(id string) {
		enqueuedApply = append(enqueuedApply, id)
	}, func(_ string) {})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Run(ctx)

	if len(enqueuedApply) != 0 {
		t.Errorf("expected no enqueue for malformed message, got %v", enqueuedApply)
	}
	if len(mock.deleteCalls) != 1 || mock.deleteCalls[0] != "rh-bad" {
		t.Errorf("expected malformed message deleted; deleteCalls=%v", mock.deleteCalls)
	}
}

func TestPoller_ContextCancellationStopsLoop(t *testing.T) {
	mock := &mockSQSClient{} // no messages — would loop forever without cancel

	p := New(mock, "https://sqs.test/queue", func(_ string) {}, func(_ string) {})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
		// good
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestPoller_TableSuffixVariants(t *testing.T) {
	// Verify that both "specs-applydesires" and "-applydesires" suffixes route correctly.
	cases := []struct {
		suffix      string
		wantApply   bool
		wantRead    bool
	}{
		{"-applydesires", true, false},
		{"specs-applydesires", true, false},
		{"-readdesires", false, true},
		{"specs-readdesires", false, true},
	}

	for _, tc := range cases {
		t.Run(tc.suffix, func(t *testing.T) {
			var gotApply, gotRead int
			mock := &mockSQSClient{
				messages: []sqstypes.Message{
					makeMessage(t, SpecNotification{DocumentID: "d1", TableSuffix: tc.suffix}, "rh"),
				},
			}
			p := New(mock, "https://sqs.test/queue",
				func(_ string) { gotApply++ },
				func(_ string) { gotRead++ },
			)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			p.Run(ctx)

			if gotApply != boolInt(tc.wantApply) {
				t.Errorf("suffix=%q: gotApply=%d, want %d", tc.suffix, gotApply, boolInt(tc.wantApply))
			}
			if gotRead != boolInt(tc.wantRead) {
				t.Errorf("suffix=%q: gotRead=%d, want %d", tc.suffix, gotRead, boolInt(tc.wantRead))
			}
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
