package dy

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DynamoClient ...
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

type Entity interface {
	IsEmpty() bool
}

type Request struct {
	Size             int
	Index            *string
	PartitionKey     *DynamoAttribute
	LastEvaluatedKey *DynamoPrimaryKey
	Conditions       []Criteria
}

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
	KeyType DBKeyType
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

func NewDynamoStringAttrib(name, value string) DynamoAttribute {
	return DynamoAttribute{
		KeyName: DBKey(name),
		KeyType: String,
		Value:   value,
	}
}

func NewDynamoNumberAttrib(name, value string) *DynamoAttribute {
	return &DynamoAttribute{
		KeyName: DBKey(name),
		KeyType: Number,
		Value:   value,
	}
}

func NewDynamoBoolAttrib(name string, value bool) DynamoAttribute {
	return DynamoAttribute{
		KeyName: DBKey(name),
		KeyType: Boolean,
		Value:   value,
	}
}

func NewClient[T Entity](client DynamoClient, config DBConfig) *db[T] {
	return &db[T]{
		conf:   config,
		client: client,
	}
}
