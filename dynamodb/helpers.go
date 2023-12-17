package dy

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

func preparePartSortKey(primaryKey DynamoPrimaryKey) (partKey DynamoAttr, sortKey *DynamoAttr, err error) {
	partKey, err = createDynamoAttribute(string(primaryKey.PartitionKey.KeyName), primaryKey.PartitionKey.Value,
		primaryKey.PartitionKey.Type,
	)
	if err != nil {
		return
	}

	if primaryKey.SortKey == nil {
		return
	}

	sKey, sKerErr := createDynamoAttribute(string(primaryKey.SortKey.KeyName), primaryKey.SortKey.Value,
		primaryKey.SortKey.Type,
	)

	err = sKerErr
	sortKey = &sKey
	return
}

func addPrimaryKey(dbMap map[string]types.AttributeValue, metadata DynamoKeyMetadata) (DynamoAttribute, error) {
	dynamoAttrib := DynamoAttribute{
		KeyName: metadata.Name,
		Type:    metadata.Type,
	}
	// getting the partition key
	k, ok := dbMap[strings.ToLower(string(metadata.Name))]

	// if partition key read from the config is not found then it returns an error
	if !ok {
		return dynamoAttrib, ErrKeyNotFound
	}

	val, empty := getValueOf(k, metadata.Type)

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

func initDynamoKeyValue(attribute DynamoKeyMetadata) (types.AttributeValue, interface{}, error) {
	switch attribute.Type {
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
		Name:  string(name),
		Type:  KeyType,
		Value: dynamoValue,
	}, nil
}

func newDynamoAttributeValue(value interface{}, KeyType DBKeyType) (types.AttributeValue, error) {
	switch KeyType {
	case String:
		return &types.AttributeValueMemberS{
			Value: fmt.Sprintf("%v", value),
		}, nil
	case Number:
		return &types.AttributeValueMemberN{
			Value: fmt.Sprintf("%v", value),
		}, nil
	case Boolean:
		b, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("%v cannot be casted to bool", value)
		}
		return &types.AttributeValueMemberBOOL{
			Value: b,
		}, nil
	}

	return nil, ErrInvalidDBKeyType
}

func extractUnprocessedKeys(keys []map[string]types.AttributeValue, partitionKey DynamoKeyMetadata, sortKeyMeta *DynamoKeyMetadata) ([]DynamoPrimaryKey, error) {
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

func extractPrimaryKey(keys map[string]types.AttributeValue, partitionKey DynamoKeyMetadata, sortKeyMeta *DynamoKeyMetadata) (*DynamoPrimaryKey, error) {
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

func getDynamoAttribute(attributes map[string]types.AttributeValue, meta DynamoKeyMetadata) *DynamoAttribute {
	attr := attributes[string(meta.Name)]
	if attr == nil {
		return nil
	}

	val, empty := getValueOf(attr, meta.Type)
	if empty {
		return nil
	}

	return &DynamoAttribute{
		KeyName: meta.Name,
		Type:    meta.Type,
		Value:   val,
	}
}

func extractMetadata(attrib *DynamoAttribute) *DynamoKeyMetadata {
	if attrib == nil {
		return nil
	}

	return &DynamoKeyMetadata{
		Name: attrib.KeyName,
		Type: attrib.Type,
	}
}

func getValueOf(attribute types.AttributeValue, DBKeyType DBKeyType) (val interface{}, empty bool) {
	switch DBKeyType {
	case String:
		v, ok := attribute.(*types.AttributeValueMemberS)
		if !ok {
			return "", true
		}
		return v.Value, v.Value == ""
	case Number:
		v, ok := attribute.(*types.AttributeValueMemberN)
		if !ok {
			return "", true
		}
		return v.Value, v.Value == ""
	case Boolean:
		v, ok := attribute.(*types.AttributeValueMemberBOOL)
		if !ok {
			return false, true
		}
		return v.Value, false
	}

	return nil, true
}

func randBool() bool {
	return rand.Intn(2) == 1
}

func toLowerCaseKeys(m map[string]types.AttributeValue) map[string]types.AttributeValue {
	newMap := make(map[string]types.AttributeValue)
	for k, v := range m {
		newMap[strings.ToLower(k)] = v
	}

	return newMap
}
