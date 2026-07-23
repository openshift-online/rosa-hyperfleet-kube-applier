package statussnspublisher

import (
	"context"

	"k8s.io/klog/v2"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
)

// notifier is the minimal interface from Publisher used by notifyingCRUD,
// allowing tests to substitute a spy without the full SNS dependency.
type notifier interface {
	Publish(ctx context.Context, documentID, tableSuffix string) error
}

// docIDer is the constraint that the generic type parameter T must satisfy:
// a pointer to T must expose GetDocumentID(). All desire types (ApplyDesire,
// ReadDesire) satisfy this via the embedded DynamoDBMetadata struct.
type docIDer[T any] interface {
	*T
	database.DynamoDBMetadataAccessor
}

// notifyingCRUD wraps a database.ResourceCRUD[T] and publishes a status
// change notification to SNS after every successful Create or Replace call.
// Get, List, and Delete are pure delegations with no notification side-effect.
//
// The documentID is read from the *input* object, not the returned value,
// because it is immutable and is always set before the write.
//
// Publish failures are logged but never propagated — the operator's 5-minute
// safety-net poll covers any missed notifications.
type notifyingCRUD[T any, PT docIDer[T]] struct {
	inner       database.ResourceCRUD[T]
	publisher   notifier
	tableSuffix string
}

// NewNotifyingCRUD wraps inner with SNS publish-on-write behaviour.
// tableSuffix identifies the table in the notification payload, e.g.
// "-applydesires" or "-readdesires".
func NewNotifyingCRUD[T any, PT docIDer[T]](
	inner database.ResourceCRUD[T],
	publisher notifier,
	tableSuffix string,
) database.ResourceCRUD[T] {
	return &notifyingCRUD[T, PT]{
		inner:       inner,
		publisher:   publisher,
		tableSuffix: tableSuffix,
	}
}

func (n *notifyingCRUD[T, PT]) Create(ctx context.Context, obj *T) (*T, error) {
	docID := PT(obj).GetDocumentID()
	result, err := n.inner.Create(ctx, obj)
	if err != nil {
		return nil, err
	}
	n.publish(ctx, docID)
	return result, nil
}

func (n *notifyingCRUD[T, PT]) Replace(ctx context.Context, obj *T) (*T, error) {
	docID := PT(obj).GetDocumentID()
	result, err := n.inner.Replace(ctx, obj)
	if err != nil {
		return nil, err
	}
	n.publish(ctx, docID)
	return result, nil
}

func (n *notifyingCRUD[T, PT]) Get(ctx context.Context, documentID string) (*T, error) {
	return n.inner.Get(ctx, documentID)
}

func (n *notifyingCRUD[T, PT]) List(ctx context.Context) ([]*T, error) {
	return n.inner.List(ctx)
}

func (n *notifyingCRUD[T, PT]) Delete(ctx context.Context, documentID string) error {
	return n.inner.Delete(ctx, documentID)
}

func (n *notifyingCRUD[T, PT]) publish(ctx context.Context, documentID string) {
	if err := n.publisher.Publish(ctx, documentID, n.tableSuffix); err != nil {
		klog.FromContext(ctx).Error(err, "Failed to publish status SNS notification (non-fatal)",
			"documentID", documentID,
			"tableSuffix", n.tableSuffix,
		)
	}
}
