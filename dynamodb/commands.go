package dynamodb

import (
	"context"
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
func (d *dynamodbWrapper[T]) Update(ctx context.Context, primaryKey DynamoPrimaryKey, values []DynamoAttribute) error {
	// prepare the partition and the sort keys
	partKey, sortKey, err := preparePartSortKey(primaryKey)
	if err != nil {
		return err
	}

	// initialize the update-item input builder
	builder := NewExpressionBuilder(d.conf.TableInfo.TableName, partKey, sortKey)
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
	builder := NewExpressionBuilder(d.conf.TableInfo.TableName, partKey, sortKey)

	// create the delete item input
	req, err := builder.BuildDeleteItemInput()
	if err != nil {
		return err
	}

	// call dynamo delete item
	_, err = d.client.DeleteItemWithContext(ctx, req)
	return err
}

func preparePartSortKey(primaryKey DynamoPrimaryKey) (partKey DynamoAttr, sortKey *DynamoAttr, err error) {
	partKey, err = createDynamoAttribute(string(primaryKey.PartitionKey.KeyName), primaryKey.PartitionKey.Value,
		primaryKey.PartitionKey.ValueType,
	)
	if err != nil {
		return
	}

	if primaryKey.SortKey == nil {
		return
	}

	sKey, sKerErr := createDynamoAttribute(string(primaryKey.SortKey.KeyName), primaryKey.SortKey.Value,
		primaryKey.PartitionKey.ValueType,
	)

	err = sKerErr
	sortKey = &sKey
	return
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
		return dynamoAttrib, ErrKeyNotFound
	}

	val, empty := getValueOf(*k, metadata.ValueType)

	if !empty {
		dynamoAttrib.Value = val
		return dynamoAttrib, nil
	}

	// if the key is nil then we create it
	key, val, err := initDynamoKeyValue(metadata)
	if err != nil {
		return dynamoAttrib, err
	}

	dynamoAttrib.Value = val
	dbMap[string(metadata.Name)] = key
	return dynamoAttrib, nil
}

func initDynamoKeyValue(attribute DynamoKeyMetadata) (*dynamodb.AttributeValue, interface{}, error) {
	switch attribute.ValueType {
	case String:
		val := uuid.NewString()
		dynamoValue, err := newDynamoAttributeValue(val, String)
		return dynamoValue, val, err
	case Number:
		val := strconv.FormatInt(time.Now().UnixNano(), 10)
		dynamoValue, err := newDynamoAttributeValue(val, Number)
		return dynamoValue, val, err
	case Boolean:
		val := randBool()
		dynamoValue, err := newDynamoAttributeValue(val, Boolean)
		return dynamoValue, val, err
	}

	return nil, nil, ErrInvalidKeyType
}

func createDynamoAttribute(name string, value interface{}, valueType KeyType) (DynamoAttr, error) {
	dynamoValue, err := newDynamoAttributeValue(value, valueType)
	if err != nil {
		return DynamoAttr{}, err
	}

	return DynamoAttr{
		Name:      string(name),
		ValueType: valueType,
		Value:     dynamoValue,
	}, nil
}

func newDynamoAttributeValue(value interface{}, valueType KeyType) (*dynamodb.AttributeValue, error) {
	switch valueType {
	case String:
		return &dynamodb.AttributeValue{
			S: aws.String(value.(string)),
		}, nil
	case Number:

		return &dynamodb.AttributeValue{
			N: aws.String(value.(string)),
		}, nil
	case Boolean:
		return &dynamodb.AttributeValue{
			BOOL: aws.Bool(value.(bool)),
		}, nil
	}

	return nil, ErrInvalidKeyType
}

func getValueOf(attribute dynamodb.AttributeValue, keyType KeyType) (val interface{}, empty bool) {
	switch keyType {
	case String, Number:
		return attribute.S, attribute.S == nil || *attribute.S == ""
	case Boolean:
		return attribute.BOOL, attribute.BOOL == nil
	}

	return nil, true
}

func randBool() bool {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) == 1
}

var _ Commands[_entity] = (*dynamodbWrapper[_entity])(nil)
