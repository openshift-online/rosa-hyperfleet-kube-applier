package database

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"k8s.io/apimachinery/pkg/runtime"
)

// rawExtField names — top-level DynamoDB string attributes stored alongside
// the marshaled desire struct.
const (
	rawExtFieldSpecKubeContent   = "spec_kubeContent"
	rawExtFieldStatusKubeContent = "status_kubeContent"
)

// KubeContentAccessor provides access to the RawExtension fields that are
// tagged dynamodbav:"-" and need manual serialization. Types that carry no
// KubeContent (DeleteDesire) return nil from both getters; the CRUD layer
// skips serialization when both are nil.
type KubeContentAccessor interface {
	GetSpecKubeContent() *runtime.RawExtension
	SetSpecKubeContent(*runtime.RawExtension)
	GetStatusKubeContent() *runtime.RawExtension
	SetStatusKubeContent(*runtime.RawExtension)
}

// rawExtToString converts a RawExtension's JSON bytes into a compact JSON
// string suitable for DynamoDB S attribute storage.
func rawExtToString(ext *runtime.RawExtension) (string, error) {
	if ext == nil || len(ext.Raw) == 0 {
		return "", nil
	}
	// Re-marshal through map to normalise (compact, sorted keys).
	var m any
	if err := json.Unmarshal(ext.Raw, &m); err != nil {
		return "", fmt.Errorf("unmarshal RawExtension: %w", err)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal RawExtension: %w", err)
	}
	return string(b), nil
}

// stringToRawExt converts a JSON string from a DynamoDB S attribute back into
// a RawExtension.
func stringToRawExt(s string) (*runtime.RawExtension, error) {
	if s == "" {
		return nil, nil
	}
	// Validate that it is valid JSON before wrapping.
	if !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("stored kubeContent is not valid JSON")
	}
	return &runtime.RawExtension{Raw: []byte(s)}, nil
}

// kubeContentAttributeValues returns additional DynamoDB AttributeValue entries
// for the spec_kubeContent and status_kubeContent fields. These are merged
// into the item map before a PutItem call.
func kubeContentAttributeValues(acc KubeContentAccessor) (map[string]types.AttributeValue, error) {
	result := make(map[string]types.AttributeValue)
	if ext := acc.GetSpecKubeContent(); ext != nil {
		s, err := rawExtToString(ext)
		if err != nil {
			return nil, err
		}
		if s != "" {
			result[rawExtFieldSpecKubeContent] = &types.AttributeValueMemberS{Value: s}
		}
	}
	if ext := acc.GetStatusKubeContent(); ext != nil {
		s, err := rawExtToString(ext)
		if err != nil {
			return nil, err
		}
		if s != "" {
			result[rawExtFieldStatusKubeContent] = &types.AttributeValueMemberS{Value: s}
		}
	}
	return result, nil
}

// kubeContentReadFromItem reads the manually-stored RawExtension fields from a
// DynamoDB item map and sets them on the desire object.
func kubeContentReadFromItem(acc KubeContentAccessor, item map[string]types.AttributeValue) error {
	if av, ok := item[rawExtFieldSpecKubeContent]; ok {
		if sv, ok := av.(*types.AttributeValueMemberS); ok {
			ext, err := stringToRawExt(sv.Value)
			if err != nil {
				return fmt.Errorf("read %s: %w", rawExtFieldSpecKubeContent, err)
			}
			acc.SetSpecKubeContent(ext)
		}
	}
	if av, ok := item[rawExtFieldStatusKubeContent]; ok {
		if sv, ok := av.(*types.AttributeValueMemberS); ok {
			ext, err := stringToRawExt(sv.Value)
			if err != nil {
				return fmt.Errorf("read %s: %w", rawExtFieldStatusKubeContent, err)
			}
			acc.SetStatusKubeContent(ext)
		}
	}
	return nil
}
