package dynamodb

import (
	"context"
)

// Get implements Queries
func (*DynamodbWrapper[T]) Get(context.Context, DynamoPrimaryKey) (T, error) {
	panic("unimplemented")
}

// GetByIDs implements Queries
func (*DynamodbWrapper[T]) GetByIDs(context.Context, []DynamoPrimaryKey) ([]T, error) {
	panic("unimplemented")
}

var _ Queries[_entity] = (*DynamodbWrapper[_entity])(nil)
