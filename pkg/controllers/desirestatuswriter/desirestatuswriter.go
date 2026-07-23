// Package desirestatuswriter is the generic "read-mutate-replace" helper that
// writes back the .status section of kube-applier *Desire Firestore documents
// (ApplyDesire, DeleteDesire, ReadDesire) to the status database.
//
// With the two-database architecture, the status document may not exist yet
// when a spec is first reconciled. The helper uses create-or-replace: it
// fetches the status doc, and if not found, creates a new one with the
// mutated status; otherwise it replaces the existing one.
package desirestatuswriter

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/klog/v2"

	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
)

func init() {
	// FirestoreMetadata embeds time.Time which has unexported fields.
	// equality.Semantic.DeepEqual panics on unexported fields unless a
	// custom comparator is registered.
	if err := equality.Semantic.AddFunc(func(a, b time.Time) bool {
		return a.Equal(b)
	}); err != nil {
		panic(err)
	}
}

// Fetcher reads the current state of a single desire by a controller-defined
// typed key. For status updates, this fetches from the status database.
type Fetcher[T any, K comparable] interface {
	Fetch(ctx context.Context, key K) (*T, error)
}

// Replacer writes back a fully-populated desire to the status database.
type Replacer[T any] interface {
	Replace(ctx context.Context, desired *T) error
}

// Creator creates a new status document when one doesn't exist yet.
type Creator[T any] interface {
	Create(ctx context.Context, obj *T) error
}

// DeepCopyable is the constraint on the pointer-type parameter that lets the
// StatusWriter clone the value it receives from the Fetcher without knowing
// T's concrete shape.
type DeepCopyable[T any] interface {
	*T
	DeepCopy() *T
}

// MutateFunc deep-mutates a desire to record the latest controller observation.
// It must not perform IO; precompute everything first.
type MutateFunc[T any] func(*T)

// StatusWriter computes the next desired state via mutate and writes it back
// once. It returns nil for a no-op (status already up-to-date) and for an
// UpdateTime conflict (the informer will requeue when the new revision lands).
type StatusWriter[T any, K comparable] interface {
	UpdateStatus(ctx context.Context, key K, mutate MutateFunc[T]) error
}

// New returns a StatusWriter that fetches via fetcher, writes via replacer,
// and creates new status documents via creator when the status doc doesn't
// exist yet.
func New[T any, K comparable, PT DeepCopyable[T]](
	fetcher Fetcher[T, K], replacer Replacer[T], creator Creator[T],
) StatusWriter[T, K] {
	return &writer[T, K, PT]{fetcher: fetcher, replacer: replacer, creator: creator}
}

type writer[T any, K comparable, PT DeepCopyable[T]] struct {
	fetcher  Fetcher[T, K]
	replacer Replacer[T]
	creator  Creator[T]
}

func (w *writer[T, K, PT]) UpdateStatus(ctx context.Context, key K, mutate MutateFunc[T]) error {
	existing, err := w.fetcher.Fetch(ctx, key)
	if err != nil {
		if database.IsNotFoundError(err) {
			return w.createInitialStatus(ctx, key, mutate)
		}
		return fmt.Errorf("fetch %v: %w", key, err)
	}
	if existing == nil {
		return w.createInitialStatus(ctx, key, mutate)
	}

	desired := PT(existing).DeepCopy()
	mutate(desired)

	if equality.Semantic.DeepEqual(existing, desired) {
		klog.FromContext(ctx).V(4).Info("Status unchanged; skipping write and SNS publish",
			"key", key,
		)
		return nil
	}

	if err := w.replacer.Replace(ctx, desired); err != nil {
		return fmt.Errorf("replace status for %v: %w", key, err)
	}
	return nil
}

func (w *writer[T, K, PT]) createInitialStatus(ctx context.Context, key K, mutate MutateFunc[T]) error {
	var initial T
	mutate(&initial)
	if err := w.creator.Create(ctx, &initial); err != nil {
		return fmt.Errorf("create initial status for %v: %w", key, err)
	}
	return nil
}
