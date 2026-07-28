package kubeapplier

const (
	ConditionTypeSuccessful = "Successful"
	ConditionTypeDegraded   = "Degraded"
)

const (
	ConditionReasonKubeAPIError   = "KubeAPIError"
	ConditionReasonPreCheckFailed = "PreCheckFailed"

	// ConditionReasonWaitingForDeletion is set on an ApplyDesire with Type=Delete when the
	// target item still exists in the cluster, either because finalizers are running or
	// the delete call has just been issued.
	ConditionReasonWaitingForDeletion = "WaitingForDeletion"

	ConditionReasonNoErrors = "NoErrors"
	ConditionReasonFailed   = "Failed"
)
