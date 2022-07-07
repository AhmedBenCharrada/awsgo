package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DynamodbWrapper provides the API methods for interacting with dynamo db
type DynamodbWrapper[T any] struct {
	dynamodbiface.DynamoDBAPI
}

// NewDynamoWrapper creates a new dynamo wrapper.
func NewDynamoWrapper[T any](client dynamodbiface.DynamoDBAPI) *DynamodbWrapper[T] {
	return &DynamodbWrapper[T]{
		DynamoDBAPI: client,
	}
}
