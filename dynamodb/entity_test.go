package dynamodb_test

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type entity struct {
	ID        string `json:"id"`
	GroupID   int    `json:"group_id"`
	Enabled   *bool  `json:"enabled"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func (e entity) Marshal() (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(e)
}

func (e entity) UnMarshal(data map[string]*dynamodb.AttributeValue) (entity, error) {
	err := dynamodbattribute.UnmarshalMap(data, &e)
	return e, err
}
