package dynamodb

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

func preparePartSortKey(primaryKey DynamoPrimaryKey) (partKey DynamoAttr, sortKey *DynamoAttr, err error) {
	partKey, err = createDynamoAttribute(string(primaryKey.PartitionKey.KeyName), primaryKey.PartitionKey.Value,
		primaryKey.PartitionKey.KeyType,
	)
	if err != nil {
		return
	}

	if primaryKey.SortKey == nil {
		return
	}

	sKey, sKerErr := createDynamoAttribute(string(primaryKey.SortKey.KeyName), primaryKey.SortKey.Value,
		primaryKey.SortKey.KeyType,
	)

	err = sKerErr
	sortKey = &sKey
	return
}

func addPrimaryKey(dbMap map[string]*dynamodb.AttributeValue, metadata DynamoKeyMetadata) (DynamoAttribute, error) {
	dynamoAttrib := DynamoAttribute{
		KeyName: metadata.Name,
		KeyType: metadata.KeyType,
	}
	// getting the partition key
	k, ok := dbMap[string(metadata.Name)]

	// if partition key read from the config is not found then it returns an error
	if !ok {
		return dynamoAttrib, ErrKeyNotFound
	}

	val, empty := getValueOf(*k, metadata.KeyType)

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
	switch attribute.KeyType {
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

	return nil, nil, ErrInvalidDBKeyType
}

func createDynamoAttribute(name string, value interface{}, KeyType DBKeyType) (DynamoAttr, error) {
	dynamoValue, err := newDynamoAttributeValue(value, KeyType)
	if err != nil {
		return DynamoAttr{}, err
	}

	return DynamoAttr{
		Name:    string(name),
		KeyType: KeyType,
		Value:   dynamoValue,
	}, nil
}

func newDynamoAttributeValue(value interface{}, KeyType DBKeyType) (*dynamodb.AttributeValue, error) {
	switch KeyType {
	case String:
		return &dynamodb.AttributeValue{
			S: aws.String(fmt.Sprintf("%v", value)),
		}, nil
	case Number:
		return &dynamodb.AttributeValue{
			N: aws.String(fmt.Sprintf("%v", value)),
		}, nil
	case Boolean:
		return &dynamodb.AttributeValue{
			BOOL: aws.Bool(value.(bool)),
		}, nil
	}

	return nil, ErrInvalidDBKeyType
}

func extractUnprocessedKeys(keys []map[string]*dynamodb.AttributeValue, partitionKey DynamoKeyMetadata, sortKeyMeta *DynamoKeyMetadata) ([]DynamoPrimaryKey, error) {
	primaryKeys := make([]DynamoPrimaryKey, 0)
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}

		primaryKey, err := extractPrimaryKey(key, partitionKey, sortKeyMeta)
		if err != nil {
			return nil, err
		}

		primaryKeys = append(primaryKeys, *primaryKey)
	}

	return primaryKeys, nil
}

func extractPrimaryKey(keys map[string]*dynamodb.AttributeValue, partitionKey DynamoKeyMetadata, sortKeyMeta *DynamoKeyMetadata) (*DynamoPrimaryKey, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	partKey := getDynamoAttribute(keys, partitionKey)
	var sortKey *DynamoAttribute

	if sortKeyMeta != nil {
		sortKey = getDynamoAttribute(keys, *sortKeyMeta)
	}

	if partKey == nil || (sortKey == nil && sortKeyMeta != nil) {
		return nil, fmt.Errorf("error while extracting keys")
	}

	return &DynamoPrimaryKey{
		PartitionKey: *partKey,
		SortKey:      sortKey,
	}, nil
}

func getDynamoAttribute(attributes map[string]*dynamodb.AttributeValue, meta DynamoKeyMetadata) *DynamoAttribute {
	attr := attributes[string(meta.Name)]
	if attr == nil {
		return nil
	}

	val, _ := getValueOf(*attr, meta.KeyType)
	return &DynamoAttribute{
		KeyName: meta.Name,
		KeyType: meta.KeyType,
		Value:   val,
	}
}

func extractMetadata(attrib *DynamoAttribute) *DynamoKeyMetadata {
	if attrib == nil {
		return nil
	}

	return &DynamoKeyMetadata{
		Name:    attrib.KeyName,
		KeyType: attrib.KeyType,
	}
}

func getValueOf(attribute dynamodb.AttributeValue, DBKeyType DBKeyType) (val interface{}, empty bool) {
	switch DBKeyType {
	case String:
		return attribute.S, attribute.S == nil || *attribute.S == ""
	case Number:
		return attribute.N, attribute.N == nil || *attribute.N == ""
	case Boolean:
		return attribute.BOOL, attribute.BOOL == nil
	}

	return nil, true
}

func randBool() bool {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) == 1
}
