package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Create implements Commands
func (d *dynamodbWrapper[T]) Create(ctx context.Context, entity T) (DynamoPrimaryKey, error) {
	dbMap, _ := entity.Marshal()
	partKey, err := addPrimaryKey(dbMap, d.conf.TableInfo.PrimaryKey.PartitionKey)

	if err != nil {
		return DynamoPrimaryKey{}, err
	}

	var sortKey *DynamoAttribute
	// if the table config mandate a sort key and the sort key is not provided then we create it
	if d.conf.TableInfo.PrimaryKey.SortKey != nil {
		sKey, err := addPrimaryKey(dbMap, *d.conf.TableInfo.PrimaryKey.SortKey)
		if err != nil {
			return DynamoPrimaryKey{}, err
		}
		sortKey = &sKey
	}

	// create the put request
	input := dynamodb.PutItemInput{
		Item:      dbMap,
		TableName: aws.String(d.conf.TableInfo.TableName),
	}

	// triggering the put operation
	_, err = d.client.PutItemWithContext(ctx, &input)
	if err != nil {
		return DynamoPrimaryKey{}, err
	}

	return DynamoPrimaryKey{
		PartitionKey: partKey,
		SortKey:      sortKey,
	}, nil
}

// Update implements Commands
func (d *dynamodbWrapper[T]) Update(ctx context.Context, primaryKey DynamoPrimaryKey, values []DynamoAttribute) error {
	// prepare the partition and the sort keys
	partKey, sortKey, err := preparePartSortKey(primaryKey)
	if err != nil {
		return err
	}

	// initialize the update-item input builder
	builder := NewExpressionBuilder(d.conf.TableInfo.TableName).WithPartitionKey(partKey).WithSortKey(sortKey)
	// populate the update data
	for _, attr := range values {
		builder.WithUpdateField(string(attr.KeyName), attr.Value)
	}

	// create the update item input
	req, err := builder.BuildUpdateItemInput()
	if err != nil {
		return err
	}

	// trigger the update request
	_, err = d.client.UpdateItemWithContext(ctx, req)
	return err
}

// Delete implements Commands
func (d *dynamodbWrapper[T]) Delete(ctx context.Context, primaryKey DynamoPrimaryKey) error {
	// prepare the partition and the sort keys
	partKey, sortKey, err := preparePartSortKey(primaryKey)
	if err != nil {
		return err
	}

	// initialize the expression builder
	builder := NewExpressionBuilder(d.conf.TableInfo.TableName).WithPartitionKey(partKey).WithSortKey(sortKey)

	// create the delete item input
	req, err := builder.BuildDeleteItemInput()
	if err != nil {
		return err
	}

	// call dynamo delete item
	_, err = d.client.DeleteItemWithContext(ctx, req)
	return err
}
