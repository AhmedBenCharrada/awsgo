package dy

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DynamoClient defines the dynamodb client.
//
//go:generate mockery --name=DynamoClient --structname=DynamoClient --case underscore --output=../mocks/ --filename=dynamo_client.go
type DynamoClient interface {
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// Entity the dynamodb base entity interface.
type Entity interface {
	IsEmpty() bool
}

// Request the find items request.
type Request struct {
	Size             int
	Index            *string
	PartitionKey     *DynamoAttribute
	LastEvaluatedKey *DynamoPrimaryKey
	Conditions       []Criteria
}

// Page the page response for extracting items with a paginator.
type Page[T Entity] struct {
	Items            []T
	LastEvaluatedKey *DynamoPrimaryKey
}

// DynamoPrimaryKey represents the data for a dynamodb partition key.
type DynamoPrimaryKey struct {
	PartitionKey DynamoAttribute
	SortKey      *DynamoAttribute
}

// DynamoAttribute represents the data for a dynamodb attribute.
type DynamoAttribute struct {
	KeyName DBKey
	Type    DBKeyType
	Value   interface{}
}

// DBKeyType represents the allowed dynamodb types.
//
// For more information, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
type DBKeyType int

// The allowed dynamodb types.
const (
	String DBKeyType = iota
	Number
	Boolean
)

// DBKey custom type for dynamo DB key name
type DBKey string

// NewDynamoStringAttrib creates a new dynamodb string attribute.
func NewDynamoStringAttrib(name, value string) DynamoAttribute {
	return DynamoAttribute{
		KeyName: DBKey(name),
		Type:    String,
		Value:   value,
	}
}

// NewDynamoNumberAttrib creates a new dynamodb number attribute.
func NewDynamoNumberAttrib(name, value string) *DynamoAttribute {
	return &DynamoAttribute{
		KeyName: DBKey(name),
		Type:    Number,
		Value:   value,
	}
}

// NewDynamoBoolAttrib creates a new dynamodb boolean attribute.
func NewDynamoBoolAttrib(name string, value bool) DynamoAttribute {
	return DynamoAttribute{
		KeyName: DBKey(name),
		Type:    Boolean,
		Value:   value,
	}
}

// NewClient creates a new dynamodb client wrapper for the entity [Entity].
// The wrapper offers simplified ways to Create, Update, Delete, Find, GetItem and GetItems for the defined entity.
func NewClient[T Entity](client DynamoClient, config DBConfig) *DB[T] {
	return &DB[T]{
		conf:   config,
		client: client,
	}
}
