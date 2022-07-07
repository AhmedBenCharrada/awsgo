package dynamodb

import (
	"context"
)

// KeyType represents the allowed dynamodb types.
//
// For more information, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
type KeyType int

const (
	String KeyType = iota
	Number
	Binary
)

// DynamoAttribute represents the data for a dynamodb attribute.
type DynamoAttribute struct {
	KeyName   string
	ValueType KeyType
	KeyValue  interface{}
}

// DynamoPrimaryKey represents the data for a dynamodb partition key.
type DynamoPrimaryKey struct {
	PartitionKey DynamoAttribute
	SortKey      *DynamoAttribute
}

// TableConfig represents a dynamodb table configuration.
type TableConfig struct {
	TableName string
	DynamoPrimaryKey
}

// Queries ...
type Queries[T any] interface {
	Get(context.Context, DynamoPrimaryKey) (T, error)
	GetByIDs(context.Context, []DynamoPrimaryKey) ([]T, error)
}

// Commands ..
type Commands[T any] interface {
	Create(context.Context, T) (DynamoPrimaryKey, error)
	Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error
}
