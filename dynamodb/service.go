package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DynamodbWrapper provides the API methods for interacting with dynamo db
type DynamodbWrapper[T EntityMarshaler[T]] struct {
	dynamodbiface.DynamoDBAPI
}

// NewDynamoWrapper creates a new dynamo wrapper.
func NewDynamoWrapper[T EntityMarshaler[T]](client dynamodbiface.DynamoDBAPI) *DynamodbWrapper[T] {
	return &DynamodbWrapper[T]{
		DynamoDBAPI: client,
	}
}
