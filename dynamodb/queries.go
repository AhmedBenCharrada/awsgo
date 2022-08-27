package dynamodb

import (
	"context"
)

// Get implements Queries
func (*dynamodbWrapper[T]) Get(context.Context, DynamoPrimaryKey) (T, error) {
	panic("unimplemented")
}

// GetByIDs implements Queries
func (*dynamodbWrapper[T]) GetByIDs(context.Context, []DynamoPrimaryKey) ([]T, error) {
	panic("unimplemented")
}

var _ Queries[_entity] = (*dynamodbWrapper[_entity])(nil)
