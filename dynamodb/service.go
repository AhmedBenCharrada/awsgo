package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DBClient dynamodbiface.DynamoDBAPI

// DynamodbWrapper provides the API methods for interacting with dynamo db
type dynamodbWrapper[T EntityMarshaler[T]] struct {
	conf DBConfig
	DBClient
}

// NewDynamoWrapper creates a new dynamo wrapper.
func NewDynamoWrapper[T EntityMarshaler[T]](client DBClient, config DBConfig) *dynamodbWrapper[T] {
	return &dynamodbWrapper[T]{
		conf:     config,
		DBClient: client,
	}
}
