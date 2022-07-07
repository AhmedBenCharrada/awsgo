package dynamodb

import "context"

// Create implements Commands
func (*DynamodbWrapper[T]) Create(context.Context, T) (DynamoPrimaryKey, error) {
	panic("unimplemented")
}

// Update implements Commands
func (*DynamodbWrapper[T]) Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error {
	panic("unimplemented")
}

var _ Commands[interface{}] = (*DynamodbWrapper[interface{}])(nil)
