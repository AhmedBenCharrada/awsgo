package dy

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoAttr ..
type DynamoAttr struct {
	Name    string
	KeyType DBKeyType
	Value   types.AttributeValue
}

// IsEmpty checks whether a dynamo-value is nit or empty.
func (d *DynamoAttr) IsEmpty() bool {
	if d.Name == "" || d.Value == nil {
		return true
	}

	switch d.KeyType {
	case String:
		s, ok := d.Value.(*types.AttributeValueMemberS)
		return !ok || s.Value == ""
	case Number:
		s, ok := d.Value.(*types.AttributeValueMemberN)
		b := !ok || s.Value == ""
		return b
	}

	return true
}

// DynamoExpressionBuilder dynamo expression builder.
type DynamoExpressionBuilder struct {
	tableName string
	partKey   DynamoAttr
	sortKey   *DynamoAttr
	expression.UpdateBuilder
}

// NewExpressionBuilder creates a new dynamo update builder.
func NewExpressionBuilder(tableName string) *DynamoExpressionBuilder {
	return &DynamoExpressionBuilder{
		tableName:     tableName,
		UpdateBuilder: expression.UpdateBuilder{},
	}
}

// WithPartitionKey sets the partition key
func (b *DynamoExpressionBuilder) WithPartitionKey(key DynamoAttr) *DynamoExpressionBuilder {
	b.partKey = key
	return b
}

// WithSortKey sets the sort key
func (b *DynamoExpressionBuilder) WithSortKey(key *DynamoAttr) *DynamoExpressionBuilder {
	b.sortKey = key
	return b
}

// WithUpdateField sets an update field.
func (b *DynamoExpressionBuilder) WithUpdateField(name string, value interface{}) *DynamoExpressionBuilder {
	b.UpdateBuilder = b.UpdateBuilder.Set(
		expression.Name(name),
		expression.Value(value),
	)

	return b
}

// BuildUpdateItemInput builds the update item request.
// Todo: consider adding conditional update.
func (b *DynamoExpressionBuilder) BuildUpdateItemInput() (*dynamodb.UpdateItemInput, error) {
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
func (b *DynamoExpressionBuilder) BuildDeleteItemInput() (*dynamodb.DeleteItemInput, error) {
	if err := b.validateKeys(); err != nil {
		return nil, err
	}

	return &dynamodb.DeleteItemInput{
		Key:       prepareDynamoKeys(b.partKey, b.sortKey),
		TableName: aws.String(b.tableName),
	}, nil
}

// BuildGetItemInput builds the get item request
func (b *DynamoExpressionBuilder) BuildGetItemInput() (*dynamodb.GetItemInput, error) {
	if err := b.validateKeys(); err != nil {
		return nil, err
	}

	return &dynamodb.GetItemInput{
		Key:       prepareDynamoKeys(b.partKey, b.sortKey),
		TableName: aws.String(b.tableName),
	}, nil
}

// BuildBatchGetItemInput builds batch get item input
func (b *DynamoExpressionBuilder) BuildBatchGetItemInput(keys ...DynamoPrimaryKey) (*dynamodb.BatchGetItemInput, error) {
	queries := make([]map[string]types.AttributeValue, 0, len(keys))

	for _, key := range keys {
		// prepare the partition and the sort keys
		partKey, sortKey, err := preparePartSortKey(key)
		if err != nil {
			return nil, err
		}

		// prepare the query input
		query := prepareDynamoKeys(partKey, sortKey)
		queries = append(queries, query)
	}

	// build batch get item input
	return &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			b.tableName: {
				Keys: queries,
			},
		},
	}, nil
}

// BuildScanInput builds the dynamo scan input.
func (b *DynamoExpressionBuilder) BuildScanInput(index *string, filter *Criteria, lastEvaluatedKey *DynamoPrimaryKey, limit int32) (*dynamodb.ScanInput, error) {
	startKey, err := getLastEvaluatedKey(lastEvaluatedKey)
	if err != nil {
		return nil, err
	}

	var size *int32
	if limit > 0 {
		size = aws.Int32(limit)
	}

	if filter == nil {
		return &dynamodb.ScanInput{
			TableName:         aws.String(b.tableName),
			Limit:             size,
			ExclusiveStartKey: startKey,
		}, nil
	}

	builder := expression.NewBuilder()
	builder = builder.WithFilter(filter.GetExpression())

	expr, err := builder.Build()

	return &dynamodb.ScanInput{
		TableName:                 aws.String(b.tableName),
		IndexName:                 index,
		Limit:                     size,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ExclusiveStartKey:         startKey,
	}, err
}

// BuildQueryInput builds dynamo query input.
func (b *DynamoExpressionBuilder) BuildQueryInput(index *string, partitionKey DynamoAttribute, filter *Criteria, lastEvaluatedKey *DynamoPrimaryKey, limit int32) (*dynamodb.QueryInput, error) {
	startKey, err := getLastEvaluatedKey(lastEvaluatedKey)
	if err != nil {
		return nil, err
	}

	var size *int32
	if limit > 0 {
		size = aws.Int32(limit)
	}

	builder := expression.NewBuilder()
	builder = builder.
		WithKeyCondition(
			expression.Key(string(partitionKey.KeyName)).Equal(expression.Value(partitionKey.Value)),
		)

	if filter != nil {
		builder.WithFilter(filter.GetExpression())
	}

	expr, err := builder.Build()
	return &dynamodb.QueryInput{
		TableName:                 aws.String(b.tableName),
		IndexName:                 index,
		Limit:                     size,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ExclusiveStartKey:         startKey,
	}, err
}

func (b *DynamoExpressionBuilder) validateKeys() error {
	if b.partKey.IsEmpty() {
		return ErrInvalidPartitionKey
	}

	if b.sortKey != nil && b.sortKey.IsEmpty() {
		return ErrInvalidSortKey
	}

	return nil
}

func getLastEvaluatedKey(lastEvaluatedKey *DynamoPrimaryKey) (map[string]types.AttributeValue, error) {
	var startKey map[string]types.AttributeValue
	if lastEvaluatedKey == nil {
		return startKey, nil
	}

	// prepare the partition and the sort keys
	partKey, sortKey, err := preparePartSortKey(*lastEvaluatedKey)
	if err != nil {
		return nil, err
	}

	startKey = prepareDynamoKeys(partKey, sortKey)
	return startKey, nil
}

func prepareDynamoKeys(partKey DynamoAttr, sortKey *DynamoAttr) map[string]types.AttributeValue {
	keys := map[string]types.AttributeValue{
		partKey.Name: partKey.Value,
	}

	if sortKey != nil {
		keys[sortKey.Name] = sortKey.Value
	}

	return keys
}
