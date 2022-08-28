package dynamodb

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
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
func (*dynamodbWrapper[T]) Update(context.Context, DynamoPrimaryKey, []DynamoAttribute) error {
	panic("unimplemented")
}

func addPrimaryKey(dbMap map[string]*dynamodb.AttributeValue, metadata DynamoKeyMetadata) (DynamoAttribute, error) {
	dynamoAttrib := DynamoAttribute{
		KeyName:   metadata.Name,
		ValueType: metadata.ValueType,
	}
	// getting the partition key
	k, ok := dbMap[string(metadata.Name)]

	// if partition key read from the config is not found then it returns an error
	if !ok {
		return dynamoAttrib, fmt.Errorf("key not found")
	}

	// if the partition key is nil then we create it
	if k != nil {
		partKey, val, err := createDynamoKey(metadata)
		if err != nil {
			return dynamoAttrib, err
		}

		dynamoAttrib.Value = val
		dbMap[string(metadata.Name)] = partKey
		return dynamoAttrib, nil
	}

	dynamoAttrib.Value = getValueOf(*k, metadata.ValueType)
	return dynamoAttrib, nil
}

func createDynamoKey(attribute DynamoKeyMetadata) (*dynamodb.AttributeValue, string, error) {
	switch attribute.ValueType {
	case String:
		val := uuid.NewString()
		return &dynamodb.AttributeValue{
			S: aws.String(val),
		}, val, nil
	case Number:
		val := strconv.FormatInt(time.Now().UnixNano(), 10)
		return &dynamodb.AttributeValue{
			N: aws.String(val),
		}, val, nil
	case Boolean:
		val := randBool()
		return &dynamodb.AttributeValue{
			BOOL: aws.Bool(val),
		}, strconv.FormatBool(val), nil
	}

	return nil, "", fmt.Errorf("invalid key type")
}

func getValueOf(attribute dynamodb.AttributeValue, keyType KeyType) interface{} {
	switch keyType {
	case String, Number:
		return attribute.S
	case Boolean:
		return attribute.BOOL
	}

	return nil
}

func randBool() bool {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) == 1
}

var _ Commands[_entity] = (*dynamodbWrapper[_entity])(nil)
