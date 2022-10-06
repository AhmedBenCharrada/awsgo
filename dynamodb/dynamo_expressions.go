package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// DynamoAttr ...
type DynamoAttr struct {
	Name    string
	KeyType DBKeyType
	Value   *dynamodb.AttributeValue
}

var (
	ErrInvalidPartitionKey = fmt.Errorf("invalid partition key")
	ErrInvalidSortKey      = fmt.Errorf("invalid sort key")
)

// IsEmpty checks whether a dynamo-value is nit or empty.
func (d *DynamoAttr) IsEmpty() bool {
	if d.Name == "" || d.Value == nil {
		return true
	}

	switch d.KeyType {
	case String, Number:
		return d.Value.S == nil || *d.Value.S == ""
	}

	return d.Value.S == nil
}

type dynamoExpressionBuilder struct {
	tableName string
	partKey   DynamoAttr
	sortKey   *DynamoAttr
	expression.UpdateBuilder
}

// NewExpressionBuilder creates a new dynamo update builder.
func NewExpressionBuilder(tableName string, partKey DynamoAttr, sortKey *DynamoAttr) dynamoExpressionBuilder {
	return dynamoExpressionBuilder{
		tableName:     tableName,
		partKey:       partKey,
		sortKey:       sortKey,
		UpdateBuilder: expression.UpdateBuilder{},
	}
}

// WithUpdateField sets an update field.
func (b *dynamoExpressionBuilder) WithUpdateField(name string, value interface{}) *dynamoExpressionBuilder {
	b.UpdateBuilder = b.UpdateBuilder.Set(
		expression.Name(name),
		expression.Value(value),
	)

	return b
}

// BuildUpdateItemInput builds the update item request.
// Todo: consider adding conditional update.
func (b *dynamoExpressionBuilder) BuildUpdateItemInput() (*dynamodb.UpdateItemInput, error) {
	if b.partKey.IsEmpty() {
		return nil, ErrInvalidPartitionKey
	}

	if b.sortKey != nil && b.sortKey.IsEmpty() {
		return nil, ErrInvalidSortKey
	}

	builder := expression.NewBuilder().WithUpdate(b.UpdateBuilder)

	expr, err := builder.Build()
	return &dynamodb.UpdateItemInput{
		Key:                       prepareDynamoKeys(b.partKey, b.sortKey),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		TableName:                 aws.String(b.tableName),
	}, err
}

// BuildDeleteItemInput builds the delete item request
// Todo: consider adding conditional delete
func (b *dynamoExpressionBuilder) BuildDeleteItemInput() (*dynamodb.DeleteItemInput, error) {
	if err := b.validateKeys(); err != nil {
		return nil, err
	}

	return &dynamodb.DeleteItemInput{
		Key:       prepareDynamoKeys(b.partKey, b.sortKey),
		TableName: aws.String(b.tableName),
	}, nil
}

// BuildGetItemInput builds the get item request
func (b *dynamoExpressionBuilder) BuildGetItemInput() (*dynamodb.GetItemInput, error) {
	if err := b.validateKeys(); err != nil {
		return nil, err
	}

	return &dynamodb.GetItemInput{
		Key:       prepareDynamoKeys(b.partKey, b.sortKey),
		TableName: aws.String(b.tableName),
	}, nil
}

func (b *dynamoExpressionBuilder) validateKeys() error {
	if b.partKey.IsEmpty() {
		return ErrInvalidPartitionKey
	}

	if b.sortKey != nil && b.sortKey.IsEmpty() {
		return ErrInvalidSortKey
	}

	return nil
}

func prepareDynamoKeys(partKey DynamoAttr, sortKey *DynamoAttr) map[string]*dynamodb.AttributeValue {
	keys := map[string]*dynamodb.AttributeValue{
		partKey.Name: partKey.Value,
	}

	if sortKey != nil {
		keys[sortKey.Name] = sortKey.Value
	}

	return keys
}
