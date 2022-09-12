package dynamodb

import (
	"context"
)

// Get implements Queries
func (d *dynamodbWrapper[T]) Get(ctx context.Context, primaryKey DynamoPrimaryKey) (*T, error) {
	// prepare the partition and the sort keys
	partKey, sortKey, err := preparePartSortKey(primaryKey)
	if err != nil {
		return nil, err
	}

	// initialize the expression builder
	builder := NewExpressionBuilder(d.conf.TableInfo.TableName, partKey, sortKey)

	// create the get item input
	req, err := builder.BuildGetItemInput()
	if err != nil {
		return nil, err
	}

	// call dynamo delete item
	res, err := d.client.GetItemWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(res.Item) < 1 {
		return nil, ErrNotFound
	}

	// unmarshal the found item
	entity, err := (*new(T)).UnMarshal(res.Item)
	return &entity, err
}

// GetByIDs implements Queries
func (*dynamodbWrapper[T]) GetByIDs(context.Context, []DynamoPrimaryKey) ([]T, error) {
	panic("unimplemented")
}
