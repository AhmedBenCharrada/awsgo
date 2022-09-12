package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DBKeyType represents the allowed dynamodb types.
//
// For more information, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
type DBKeyType int

const (
	String DBKeyType = iota
	Number
	Boolean
)

// DBKey custom type for dynamo DB key name
type DBKey string

// DynamoAttribute represents the data for a dynamodb attribute.
type DynamoAttribute struct {
	KeyName DBKey
	KeyType DBKeyType
	Value   interface{}
}

// DynamoPrimaryKey represents the data for a dynamodb partition key.
type DynamoPrimaryKey struct {
	PartitionKey DynamoAttribute
	SortKey      *DynamoAttribute
}

// EntityMarshaler describes the methods an entity should implement.
type EntityMarshaler[T any] interface {
	Marshal() (map[string]*dynamodb.AttributeValue, error)
	UnMarshal(map[string]*dynamodb.AttributeValue) (T, error)
}

// Queries ...
type Queries[T EntityMarshaler[T]] interface {
	Get(context.Context, DynamoPrimaryKey) (*T, error)
	GetByIDs(context.Context, []DynamoPrimaryKey) ([]T, error)
}

// Commands ..
type Commands[T EntityMarshaler[T]] interface {
	Create(context.Context, T) (DynamoPrimaryKey, error)
	Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error
	Delete(context.Context, DynamoPrimaryKey) error
}
