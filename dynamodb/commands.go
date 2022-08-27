package dynamodb

import (
	"context"
)

// Create implements Commands
func (d *dynamodbWrapper[T]) Create(ctx context.Context, entity T) (DynamoPrimaryKey, error) {
	panic("unimplemented")
}

// Update implements Commands
func (*dynamodbWrapper[T]) Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error {
	panic("unimplemented")
}

var _ Commands[_entity] = (*dynamodbWrapper[_entity])(nil)
