package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// DynamoAttr ...
type DynamoAttr struct {
	Name  string
	Value *dynamodb.AttributeValue
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

// BuildUpdateItemInput builds the update request.
// Todo: consider adding conditional update.
func (b *dynamoExpressionBuilder) BuildUpdateItemInput() (*dynamodb.UpdateItemInput, error) {
	if b.partKey.Name == "" || b.partKey.Value == nil {
		return nil, fmt.Errorf("invalid partition key")
	}

	if b.sortKey != nil && (b.sortKey.Name == "" || b.sortKey.Value == nil) {
		return nil, fmt.Errorf("invalid sort key")
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

func prepareDynamoKeys(partKey DynamoAttr, sortKey *DynamoAttr) map[string]*dynamodb.AttributeValue {
	keys := map[string]*dynamodb.AttributeValue{
		partKey.Name: partKey.Value,
	}

	if sortKey != nil {
		keys[sortKey.Name] = sortKey.Value
	}

	return keys
}