package statussnspublisher

import (
	"context"
	"errors"
	"testing"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listertesting"
)

// spyNotifier records Publish calls for assertions.
type spyNotifier struct {
	calls []publishCall
	err   error
}

type publishCall struct {
	documentID  string
	tableSuffix string
}

func (s *spyNotifier) Publish(_ context.Context, documentID, tableSuffix string) error {
	s.calls = append(s.calls, publishCall{documentID, tableSuffix})
	return s.err
}

func newApplyDesire(docID string) *kubeapplier.ApplyDesire {
	d := &kubeapplier.ApplyDesire{}
	d.SetDocumentID(docID)
	return d
}

func TestNotifyingCRUD_Create_PublishesOnSuccess(t *testing.T) {
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	obj := newApplyDesire("doc-1")
	if _, err := crud.Create(context.Background(), obj); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if len(spy.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(spy.calls))
	}
	if spy.calls[0].documentID != "doc-1" {
		t.Errorf("documentID = %q, want doc-1", spy.calls[0].documentID)
	}
	if spy.calls[0].tableSuffix != "-applydesires" {
		t.Errorf("tableSuffix = %q, want -applydesires", spy.calls[0].tableSuffix)
	}
}

func TestNotifyingCRUD_Replace_PublishesOnSuccess(t *testing.T) {
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	obj := newApplyDesire("doc-2")
	if _, err := inner.Create(context.Background(), obj); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	spy.calls = nil // reset after seed

	// Replace needs the current version from the store
	stored, _ := inner.Get(context.Background(), "doc-2")
	if _, err := crud.Replace(context.Background(), stored); err != nil {
		t.Fatalf("Replace: %v", err)
	}

	if len(spy.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(spy.calls))
	}
	if spy.calls[0].documentID != "doc-2" {
		t.Errorf("documentID = %q, want doc-2", spy.calls[0].documentID)
	}
}

func TestNotifyingCRUD_Create_NoPublishOnError(t *testing.T) {
	// inner will fail because docID is empty
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	obj := newApplyDesire("") // empty ID causes FakeCRUD to return error
	_, err := crud.Create(context.Background(), obj)
	if err == nil {
		t.Fatal("expected error from Create with empty docID")
	}
	if len(spy.calls) != 0 {
		t.Errorf("expected no publish calls on error, got %d", len(spy.calls))
	}
}

func TestNotifyingCRUD_PublishErrorIsNonFatal(t *testing.T) {
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{err: errors.New("sns down")}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	obj := newApplyDesire("doc-3")
	// Create should succeed even though publish fails
	_, err := crud.Create(context.Background(), obj)
	if err != nil {
		t.Errorf("Create returned error despite publish failure: %v", err)
	}
}

func TestNotifyingCRUD_Get_NoPublish(t *testing.T) {
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	// seed
	if _, err := inner.Create(context.Background(), newApplyDesire("doc-4")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := crud.Get(context.Background(), "doc-4"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(spy.calls) != 0 {
		t.Errorf("Get must not publish; got %d calls", len(spy.calls))
	}
}

func TestNotifyingCRUD_List_NoPublish(t *testing.T) {
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	if _, err := crud.List(context.Background()); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(spy.calls) != 0 {
		t.Errorf("List must not publish; got %d calls", len(spy.calls))
	}
}

func TestNotifyingCRUD_Delete_NoPublish(t *testing.T) {
	inner := listertesting.NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	spy := &spyNotifier{}
	crud := NewNotifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](inner, spy, "-applydesires")

	if _, err := inner.Create(context.Background(), newApplyDesire("doc-5")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := crud.Delete(context.Background(), "doc-5"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(spy.calls) != 0 {
		t.Errorf("Delete must not publish; got %d calls", len(spy.calls))
	}
}

// Compile-time check: notifyingCRUD implements database.ResourceCRUD.
var _ database.ResourceCRUD[kubeapplier.ApplyDesire] = (*notifyingCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire])(nil)
