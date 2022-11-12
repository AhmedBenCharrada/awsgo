package dynamodb

import (
	"awsgo/utils"
	"context"
	"sync"
)

type resp[T any] struct {
	data            []T
	unprocessedKeys []DynamoPrimaryKey
	err             error
}

// Get implements Queries
func (d *dynamodbWrapper[T]) GetItem(ctx context.Context, primaryKey DynamoPrimaryKey) (*T, error) {
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
func (d *dynamodbWrapper[T]) GetItems(ctx context.Context, ids []DynamoPrimaryKey) ([]T, []DynamoPrimaryKey, error) {
	partitions := utils.Partition(ids, 25)

	ch := make(chan resp[T])

	wg := &sync.WaitGroup{}

	for part := range partitions {
		wg.Add(1)
		go d.load(ctx, wg, ch, part...)
	}

	go func(wg *sync.WaitGroup, ch chan resp[T]) {
		wg.Wait()
		close(ch)
	}(wg, ch)

	res := make([]T, 0, len(ids))
	unprocessedKeys := make([]DynamoPrimaryKey, 0, len(ids))

	for out := range ch {
		if out.err != nil {
			return nil, ids, out.err
		}

		res = append(res, out.data...)
		unprocessedKeys = append(unprocessedKeys, out.unprocessedKeys...)
	}

	return res, unprocessedKeys, nil
}

func (d *dynamodbWrapper[T]) load(ctx context.Context, wg *sync.WaitGroup, ch chan<- resp[T], ids ...DynamoPrimaryKey) {
	defer wg.Done()

	// return if no id is provided.
	if len(ids) == 0 {
		ch <- resp[T]{}
		return
	}

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

	// Todo: add test coverage
	if out.UnprocessedKeys != nil && len(out.UnprocessedKeys[d.conf.TableInfo.TableName].Keys) > 0 {
		partKeyMeta := extractMetadata(&ids[0].PartitionKey)
		sortKeyMeta := extractMetadata(ids[0].SortKey)
		res.unprocessedKeys, res.err = extractUnprocessedKeys(out.UnprocessedKeys[d.conf.TableInfo.TableName].Keys, *partKeyMeta, sortKeyMeta)
	}

	ch <- res
}
