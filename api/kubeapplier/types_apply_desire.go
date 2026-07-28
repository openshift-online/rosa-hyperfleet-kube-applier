package kubeapplier

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ApplyDesireType is the discriminator for the ApplyDesire union.
// +k8s:union
type ApplyDesireType string

const (
	// ApplyDesireTypeServerSideApply indicates the desire is a server-side-apply
	// of .spec.serverSideApply.kubeContent to the management cluster.
	// +k8s:unionMember
	ApplyDesireTypeServerSideApply ApplyDesireType = "ServerSideApply"

	// ApplyDesireTypeDelete indicates the desire is a deletion of .spec.targetItem
	// from the management cluster.
	// +k8s:unionMember
	ApplyDesireTypeDelete ApplyDesireType = "Delete"
)

// ApplyDesire holds a single intent to either server-side-apply a Kubernetes
// object or delete one from the management cluster's apiserver. The
// .spec.type field discriminates between the two operations.
//
// Each ApplyDesire targets exactly one kube object — there is no list form.
//
// Deleting an ApplyDesire from DynamoDB has no effect on the kube object that
// was applied or deleted. To stop reconciliation, remove the desire document.
type ApplyDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             ApplyDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           ApplyDesireStatus `json:"status" dynamodbav:"status"`
}

// ApplyDesireSpec is the specification for an ApplyDesire. It uses a
// discriminated union via the Type field:
//
// - Type=ServerSideApply: the ServerSideApply field must be non-nil and
// contains the KubeContent to apply.
// - Type=Delete: the ServerSideApply field must be nil. The controller
// deletes .spec.targetItem and waits for finalizers.
//
// +k8s:discriminator=Type
type ApplyDesireSpec struct {
	ManagementCluster string `json:"managementCluster"       dynamodbav:"managementCluster"`
	ClusterID         string `json:"clusterID"               dynamodbav:"clusterID"`
	NodePoolName      string `json:"nodePoolName,omitempty"  dynamodbav:"nodePoolName,omitempty"`

	// Type discriminates the operation: ServerSideApply or Delete.
	// +k8s:union
	Type ApplyDesireType `json:"type,omitempty" dynamodbav:"type,omitempty"`

	// TargetItem identifies the GVR (and optionally the namespace) of the
	// target Kubernetes resource. For ServerSideApply, the controller uses
	// Group + Resource verbatim rather than guessing a plural form from
	// KubeContent's kind. Name and Namespace must agree with
	// KubeContent.metadata.{name,namespace}; the controller does not
	// re-derive them from the manifest. For Delete, TargetItem identifies
	// the single kube object to delete.
	TargetItem ResourceReference `json:"targetItem" dynamodbav:"targetItem"`

	// ServerSideApply holds the configuration for the ServerSideApply variant.
	// Must be non-nil when Type=ServerSideApply; must be nil when Type=Delete.
	// +k8s:unionMember=ServerSideApply
	ServerSideApply *ServerSideApplyConfig `json:"serverSideApply,omitempty" dynamodbav:"serverSideApply,omitempty"`
}

// ServerSideApplyConfig holds fields specific to the ServerSideApply variant
// of ApplyDesire.
type ServerSideApplyConfig struct {
	// KubeContent is a single Kubernetes object (not a List) to be applied
	// via server-side-apply (Force=true). It must be valid JSON and the
	// metadata.name / metadata.namespace inside it must match TargetItem.
	KubeContent *runtime.RawExtension `json:"kubeContent,omitempty" dynamodbav:"-"`
}

// ApplyDesireStatus records the outcome of the most recent reconcile.
//
// For Type=ServerSideApply: Successful=True means the SSA patch was accepted.
// For Type=Delete: Successful=True means the target object is gone from the
// cluster. While finalizers are running, Successful=False with reason
// WaitingForDeletion is set.
type ApplyDesireStatus struct {
	Conditions                []metav1.Condition `json:"conditions,omitempty"               dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime  time.Time          `json:"observedDesireUpdateTime,omitempty"  dynamodbav:"observedDesireUpdateTime,omitempty"`
	AppliedResourceGeneration int64              `json:"appliedResourceGeneration,omitempty" dynamodbav:"appliedResourceGeneration,omitempty"`
}

func (d *ApplyDesire) GetSpecKubeContent() *runtime.RawExtension {
	if d.Spec.ServerSideApply == nil {
		return nil
	}
	return d.Spec.ServerSideApply.KubeContent
}

func (d *ApplyDesire) SetSpecKubeContent(ext *runtime.RawExtension) {
	if d.Spec.ServerSideApply == nil {
		d.Spec.ServerSideApply = &ServerSideApplyConfig{}
	}
	d.Spec.ServerSideApply.KubeContent = ext
}

func (d *ApplyDesire) GetStatusKubeContent() *runtime.RawExtension  { return nil }
func (d *ApplyDesire) SetStatusKubeContent(_ *runtime.RawExtension) {}
