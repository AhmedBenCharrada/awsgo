package dynamodb

import (
	"awsgo/utils"
	"context"
)

type resp[T any] struct {
	data            []T
	unprocessedKeys []DynamoPrimaryKey
	err             error
}

// Get implements Queries
func (d *dynamodbWrapper[T]) Get(ctx context.Context, primaryKey DynamoPrimaryKey) (*T, error) {
	// prepare the partition and the sort keys
	partKey, sortKey, err := preparePartSortKey(primaryKey)
	if err != nil {
		return nil, err
	}

	// initialize the expression builder
	builder := NewExpressionBuilder(d.conf.TableInfo.TableName).WithPartitionKey(partKey).WithSortKey(sortKey)

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
func (d *dynamodbWrapper[T]) GetByIDs(ctx context.Context, ids []DynamoPrimaryKey) ([]T, error) {
	partitions := utils.Partition(ids, 25)

	ch := make(chan resp[T])
	defer close(ch)

	for part := range partitions {
		go d.load(ctx, ch, part...)
	}

	res := make([]T, 0, len(ids))
	for out := range ch {
		if out.err != nil {
			return nil, out.err
		}

		res = append(res, out.data...)
	}

	return res, nil
}

func (d *dynamodbWrapper[T]) load(ctx context.Context, ch chan<- resp[T], ids ...DynamoPrimaryKey) {
	// build the batch get item query
	query, err := NewExpressionBuilder(d.conf.TableInfo.TableName).BuildBatchGetItemInput(ids...)
	if err != nil {
		ch <- resp[T]{
			err: err,
		}
		return
	}

	// trigger the query
	out, err := d.client.BatchGetItemWithContext(ctx, query)
	if err != nil {
		ch <- resp[T]{
			err: err,
		}
		return
	}

	// parse response and accumulate returned items
	data := make([]T, 0, len(out.Responses[d.conf.TableInfo.TableName]))
	for _, item := range out.Responses[d.conf.TableInfo.TableName] {
		entity, err := (*new(T)).UnMarshal(item)
		if err != nil {
			ch <- resp[T]{
				err: err,
			}
			return
		}

		data = append(data, entity)
	}

	res := resp[T]{
		data: data,
	}

	if out.UnprocessedKeys != nil && len(out.UnprocessedKeys[d.conf.TableInfo.TableName].Keys) > 0 {
		// Todo convert unprocessed keys to DynamoPrimaryKey
		res.unprocessedKeys = []DynamoPrimaryKey{}
	}

	ch <- res
}
