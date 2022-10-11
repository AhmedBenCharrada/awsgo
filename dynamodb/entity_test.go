package dynamodb_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type entity struct {
	ID        string `json:"id"`
	GroupID   *int   `json:"group_id"`
	Enabled   *bool  `json:"enabled"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func (e entity) isEmpty() bool {
	return len(e.ID) == 0 && e.GroupID == nil && e.Enabled == nil && len(e.FirstName) == 0 && len(e.LastName) == 0
}

func (e entity) Marshal() (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(e)
}

func (e entity) UnMarshal(data map[string]*dynamodb.AttributeValue) (entity, error) {
	if err := dynamodbattribute.UnmarshalMap(data, &e); err != nil {
		return e, err
	}

	if e.isEmpty() {
		return e, fmt.Errorf("invalid data")
	}

	return e, nil
}
