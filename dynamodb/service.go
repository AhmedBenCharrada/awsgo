package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DBClient defines the db client.
type DBClient interface{ dynamodbiface.DynamoDBAPI }

// DBWrapper provides the API methods for interacting with dynamo db
type DBWrapper[T Entity[T]] struct {
	conf   DBConfig
	client DBClient
}

// NewDynamoWrapper creates a new dynamo wrapper.
func NewDynamoWrapper[T Entity[T]](client DBClient, config DBConfig) *DBWrapper[T] {
	return &DBWrapper[T]{
		conf:   config,
		client: client,
	}
}
