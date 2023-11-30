package dy

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (d *db[T]) Create(ctx context.Context, entity T) (DynamoPrimaryKey, error) {
	// dbMap, _ := entity.Marshal()
	dbMap, err := attributevalue.Marshal(entity)
	if err != nil {
		return DynamoPrimaryKey{}, err
	}

	m, ok := dbMap.(*types.AttributeValueMemberM)
	if !ok {
		return DynamoPrimaryKey{}, fmt.Errorf("failed to marshal entity")
	}

	entityAsMap := toLowerCaseKeys(m.Value)
	partKey, err := addPrimaryKey(entityAsMap, d.conf.TableInfo.PrimaryKey.PartitionKey)

	if err != nil {
		return DynamoPrimaryKey{}, err
	}

	var sortKey *DynamoAttribute
	// if the table config mandate a sort key and the sort key is not provided then we create it
	if d.conf.TableInfo.PrimaryKey.SortKey != nil {
		sKey, err := addPrimaryKey(entityAsMap, *d.conf.TableInfo.PrimaryKey.SortKey)
		if err != nil {
			return DynamoPrimaryKey{}, err
		}
		sortKey = &sKey
	}

	// create the put request
	input := dynamodb.PutItemInput{
		Item:      m.Value,
		TableName: aws.String(d.conf.TableInfo.TableName),
	}

	// triggering the put operation
	_, err = d.client.PutItem(ctx, &input)
	if err != nil {
		return DynamoPrimaryKey{}, err
	}

	return DynamoPrimaryKey{
		PartitionKey: partKey,
		SortKey:      sortKey,
	}, nil
}

func (d *db[T]) Update(ctx context.Context, primaryKey DynamoPrimaryKey, values []DynamoAttribute) error {
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
	_, err = d.client.UpdateItem(ctx, req)
	return err
}

func (d *db[T]) Delete(ctx context.Context, primaryKey DynamoPrimaryKey) error {
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
	_, err = d.client.DeleteItem(ctx, req)
	return err
}