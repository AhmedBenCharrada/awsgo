package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type EntityMarshaler[T any] interface {
	Marshal() (map[string]*dynamodb.AttributeValue, error)
	UnMarshal(map[string]*dynamodb.AttributeValue) (T, error)
}

// KeyType represents the allowed dynamodb types.
//
// For more information, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
type KeyType int

const (
	String KeyType = iota
	Number
	Boolean
)

var (
	ErrKeyNotFound    = fmt.Errorf("key not found")
	ErrInvalidKeyType = fmt.Errorf("invalid key type")
)

// DBKey custom type for dynamo DB key name
type DBKey string

type DynamoKeyMetadata struct {
	Name      DBKey
	ValueType KeyType
}

// DynamoAttribute represents the data for a dynamodb attribute.
type DynamoAttribute struct {
	KeyName   DBKey
	ValueType KeyType
	Value     interface{}
}

// DynamoPrimaryKey represents the data for a dynamodb partition key.
type DynamoPrimaryKey struct {
	PartitionKey DynamoAttribute
	SortKey      *DynamoAttribute
}

// Queries ...
type Queries[T EntityMarshaler[T]] interface {
	Get(context.Context, DynamoPrimaryKey) (T, error)
	GetByIDs(context.Context, []DynamoPrimaryKey) ([]T, error)
}

// Commands ..
type Commands[T EntityMarshaler[T]] interface {
	Create(context.Context, T) (DynamoPrimaryKey, error)
	Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error
	Delete(context.Context, DynamoPrimaryKey) error
}

type _entity struct{}

func (_entity) Marshal() (map[string]*dynamodb.AttributeValue, error) {
	panic("unimplemented")
}

func (_entity) UnMarshal(map[string]*dynamodb.AttributeValue) (_entity, error) {
	panic("unimplemented")
}
