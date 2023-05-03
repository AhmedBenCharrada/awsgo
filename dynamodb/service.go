package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DBClient interface{ dynamodbiface.DynamoDBAPI }

// DynamodbWrapper provides the API methods for interacting with dynamo db
type dynamodbWrapper[T Entity[T]] struct {
	conf   DBConfig
	client DBClient
}

// NewDynamoWrapper creates a new dynamo wrapper.
func NewDynamoWrapper[T Entity[T]](client DBClient, config DBConfig) *dynamodbWrapper[T] {
	return &dynamodbWrapper[T]{
		conf:   config,
		client: client,
	}
}
