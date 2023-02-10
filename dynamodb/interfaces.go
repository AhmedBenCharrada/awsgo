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

// PageRequest pagination request.
//
// size: the max item per page
// LastEvaluatedKey: the last retrieved key.
type PageRequest struct {
	Size             int
	LastEvaluatedKey *DynamoPrimaryKey
}

// Page a page of retrieved items.
type Page[T EntityMarshaler[T]] struct {
	Items            []T
	LastEvaluatedKey *DynamoPrimaryKey
}

// Queries ...
type Queries[T EntityMarshaler[T]] interface {
	// Find retrieves all items that match the provided condition(s).
	// Todo: Add pagination
	Find(ctx context.Context, req PageRequest, conditions ...Criteria) (Page[T], error)

	// GetItem extracts and returns an item by its (partition, (sort)?) key.
	GetItem(ctx context.Context, primaryKey DynamoPrimaryKey) (item *T, err error)

	// GetItems returns extracted items by their (partition, (sort)?) keys, the unprocessed keys and/or the error if any.
	GetItems(ctx context.Context, keys []DynamoPrimaryKey) (items []T, unprocessedKeys []DynamoPrimaryKey, err error)
}

// Commands ..
type Commands[T EntityMarshaler[T]] interface {
	// Create inserts a new item to dynamo table and returns the item's (partition, (sort)?) key.
	Create(context.Context, T) (DynamoPrimaryKey, error)
	// Update updates a dynamo existing item.
	Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error
	// Delete deletes a dynamo item
	Delete(context.Context, DynamoPrimaryKey) error
}
