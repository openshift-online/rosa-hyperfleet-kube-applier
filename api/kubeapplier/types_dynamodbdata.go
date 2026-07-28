package kubeapplier

import "time"

// DynamoDBMetadata holds the per-item metadata fields that every desire type
// carries. DocumentID is the DynamoDB partition key and is stored as a
// top-level attribute named "documentID"; the dynamodbav:"-" tag keeps the
// attributevalue marshaller from double-writing it when the struct is embedded
// inside a desire type that is marshaled as a whole. Version is the optimistic
// concurrency counter: Replace increments it and conditions on the previous
// value. UpdateTime and CreateTime are stored as ISO-8601 strings via the
// attributevalue time.Time codec.
type DynamoDBMetadata struct {
	DocumentID string    `json:"documentID"           dynamodbav:"-"`
	Version    int64     `json:"version"              dynamodbav:"version"`
	UpdateTime time.Time `json:"updateTime"           dynamodbav:"updateTime"`
	CreateTime time.Time `json:"createTime,omitempty" dynamodbav:"createTime,omitempty"`
}

func (m *DynamoDBMetadata) GetDocumentID() string    { return m.DocumentID }
func (m *DynamoDBMetadata) GetVersion() int64        { return m.Version }
func (m *DynamoDBMetadata) GetUpdateTime() time.Time { return m.UpdateTime }
func (m *DynamoDBMetadata) GetCreateTime() time.Time { return m.CreateTime }

func (m *DynamoDBMetadata) SetDocumentID(id string)   { m.DocumentID = id }
func (m *DynamoDBMetadata) SetVersion(v int64)        { m.Version = v }
func (m *DynamoDBMetadata) SetUpdateTime(t time.Time) { m.UpdateTime = t }
func (m *DynamoDBMetadata) SetCreateTime(t time.Time) { m.CreateTime = t }
