package dy

import (
	"context"
	"fmt"
	"sync"

	"github.com/AhmedBenCharrada/awsgo/utils"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type db[T Entity] struct {
	client DynamoClient
	conf   DBConfig
}

type findOutput struct {
	Items            []map[string]types.AttributeValue
	LastEvaluatedKey map[string]types.AttributeValue
}

type resp[T Entity] struct {
	data            []T
	unprocessedKeys []DynamoPrimaryKey
	err             error
}

func (d *db[T]) Find(ctx context.Context, req Request) (Page[T], error) {
	if req.Size == 0 {
		return Page[T]{}, nil
	}

	out, err := find(ctx, d.client, d.conf.TableInfo.TableName, req)
	if err != nil {
		return Page[T]{}, err
	}

	// parse response and accumulate returned items
	data := make([]T, 0, len(out.Items))
	for _, item := range out.Items {
		var entity T
		if err := attributevalue.UnmarshalMap(item, &entity); err != nil {
			return Page[T]{}, err
		}

		if entity.IsEmpty() {
			return Page[T]{}, nil
		}
		data = append(data, entity)
	}

	lastEvaluatedKey, err := extractPrimaryKey(
		out.LastEvaluatedKey,
		d.conf.TableInfo.PrimaryKey.PartitionKey,
		d.conf.TableInfo.PrimaryKey.SortKey,
	)
	if err != nil {
		return Page[T]{}, err
	}

	return Page[T]{
		Items:            data,
		LastEvaluatedKey: lastEvaluatedKey,
	}, nil
}

func (d *db[T]) GetItem(ctx context.Context, primaryKey DynamoPrimaryKey) (*T, error) {
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

	res, err := d.client.GetItem(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(res.Item) < 1 {
		return nil, ErrNotFound
	}

	// unmarshal the found item
	var entity T
	err = attributevalue.UnmarshalMap(res.Item, &entity)
	return &entity, err
}

func (d *db[T]) GetItems(ctx context.Context, ids []DynamoPrimaryKey) ([]T, []DynamoPrimaryKey, error) {
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

func find(ctx context.Context, client DynamoClient, table string, req Request) (*findOutput, error) {
	cb := mergeConditions(req.Conditions)

	// initialize the expression builder
	builder := NewExpressionBuilder(table)

	if req.PartitionKey == nil {
		in, err := builder.BuildScanInput(req.Index, cb, req.LastEvaluatedKey, int32(req.Size))
		if err != nil {
			return nil, err
		}

		out, err := client.Scan(ctx, in)
		if err != nil {
			return nil, err
		}

		return &findOutput{
			Items:            out.Items,
			LastEvaluatedKey: out.LastEvaluatedKey,
		}, err
	}

	in, err := builder.BuildQueryInput(req.Index, *req.PartitionKey, cb, req.LastEvaluatedKey, int32(req.Size))
	if err != nil {
		return nil, err
	}

	out, err := client.Query(ctx, in)
	if err != nil {
		return nil, err
	}

	return &findOutput{
		Items:            out.Items,
		LastEvaluatedKey: out.LastEvaluatedKey,
	}, err
}

func (d *db[T]) load(ctx context.Context, wg *sync.WaitGroup, ch chan<- resp[T], ids ...DynamoPrimaryKey) {
	defer wg.Done()

	// build the batch get item query
	query, err := NewExpressionBuilder(d.conf.TableInfo.TableName).BuildBatchGetItemInput(ids...)
	if err != nil {
		ch <- resp[T]{
			err: err,
		}
		return
	}

	out, err := d.client.BatchGetItem(ctx, query)
	if err != nil {
		ch <- resp[T]{
			err: err,
		}
		return
	}

	// parse response and accumulate returned items
	data, err := d.parse(out.Responses[d.conf.TableInfo.TableName])
	if err != nil {
		ch <- resp[T]{
			err: err,
		}
		return
	}

	res := resp[T]{
		data: data,
	}

	if out.UnprocessedKeys == nil {
		ch <- res
		return
	}

	if len(out.UnprocessedKeys[d.conf.TableInfo.TableName].Keys) == 0 {
		ch <- res
		return
	}

	// Todo: check whether it is better to use d.conf.TableInfo.PrimaryKey
	partKeyMeta := extractMetadata(&ids[0].PartitionKey)
	sortKeyMeta := extractMetadata(ids[0].SortKey)
	res.unprocessedKeys, res.err = extractUnprocessedKeys(out.UnprocessedKeys[d.conf.TableInfo.TableName].Keys, *partKeyMeta, sortKeyMeta)

	ch <- res
}

func (d *db[T]) parse(items []map[string]types.AttributeValue) ([]T, error) {
	// parse response and accumulate returned items
	data := make([]T, 0, len(items))
	for _, item := range items {
		var entity T
		if err := attributevalue.UnmarshalMap(item, &entity); err != nil {
			return nil, err
		}

		if entity.IsEmpty() {
			return nil, fmt.Errorf("failed to parse response")
		}

		data = append(data, entity)
	}

	return data, nil
}

func mergeConditions(conditions []Criteria) *Criteria {
	if len(conditions) == 0 {
		return nil
	}

	if len(conditions) == 1 {
		return &conditions[0]
	}

	cond := conditions[0]
	return cond.Merge(conditions[1:]...)
}
