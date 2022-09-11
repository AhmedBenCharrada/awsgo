package dynamodb_test

import (
	dynamo "awsgo/dynamodb"
	"awsgo/mocks"
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestCreate(t *testing.T) {
	validDbConfig := dynamo.DBConfig{
		TableInfo: dynamo.TableInfo{
			TableName: "tableName",
			PrimaryKey: dynamo.DBPrimaryKeyNames{
				PartitionKey: dynamo.DynamoKeyMetadata{
					Name:      "id",
					ValueType: dynamo.String,
				},
				SortKey: &dynamo.DynamoKeyMetadata{
					Name:      "group_id",
					ValueType: dynamo.Number,
				},
			},
		},
	}

	dbWithNoError := mocks.DBClient{}
	dbWithNoError.On("PutItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.PutItemOutput{}, nil)

	dbWithError := mocks.DBClient{}
	dbWithError.On("PutItemWithContext", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))

	validEntity := entity{
		ID:        "id-1",
		GroupID:   1,
		FirstName: "f1",
		LastName:  "L1",
	}

	cases := []struct {
		name     string
		dbConfig dynamo.DBConfig
		dbClient dynamo.DBClient
		entity   entity
		hasError bool
	}{
		{
			name:     "successfully",
			dbConfig: validDbConfig,
			dbClient: &dbWithNoError,
			entity:   validEntity,
		},
		{
			name:     "successfully with empty primary key",
			dbConfig: validDbConfig,
			dbClient: &dbWithNoError,
			entity: entity{
				FirstName: "name2",
				LastName:  "l2",
			},
		},
		{
			name:     "with db error",
			dbConfig: validDbConfig,
			dbClient: &dbWithError,
			entity:   validEntity,
			hasError: true,
		},
		{
			name: "with mismatched DB config",
			dbConfig: dynamo.DBConfig{
				TableInfo: dynamo.TableInfo{
					TableName: "tableName",
					PrimaryKey: dynamo.DBPrimaryKeyNames{
						PartitionKey: dynamo.DynamoKeyMetadata{
							Name:      "user_id",
							ValueType: dynamo.String,
						},
					},
				},
			},
			dbClient: &dbWithNoError,
			entity:   validEntity,
			hasError: true,
		},
		{
			name: "with wrong partition key metadata",
			dbConfig: dynamo.DBConfig{
				TableInfo: dynamo.TableInfo{
					TableName: "tableName",
					PrimaryKey: dynamo.DBPrimaryKeyNames{
						PartitionKey: dynamo.DynamoKeyMetadata{
							Name:      "id",
							ValueType: dynamo.KeyType(99),
						},
					},
				},
			},
			dbClient: &dbWithNoError,
			entity:   validEntity,
			hasError: true,
		},
		{
			name: "with wrong sort key metadata",
			dbConfig: dynamo.DBConfig{
				TableInfo: dynamo.TableInfo{
					TableName: "tableName",
					PrimaryKey: dynamo.DBPrimaryKeyNames{
						PartitionKey: dynamo.DynamoKeyMetadata{
							Name:      "id",
							ValueType: dynamo.String,
						},
						SortKey: &dynamo.DynamoKeyMetadata{
							Name:      "group_id",
							ValueType: dynamo.KeyType(99),
						},
					},
				},
			},
			dbClient: &dbWithNoError,
			entity:   validEntity,
			hasError: true,
		},
		// for coverage
		{
			name: "entity with a boolean partition key",
			dbConfig: dynamo.DBConfig{
				TableInfo: dynamo.TableInfo{
					TableName: "tableName",
					PrimaryKey: dynamo.DBPrimaryKeyNames{
						PartitionKey: dynamo.DynamoKeyMetadata{
							Name:      "enabled",
							ValueType: dynamo.Boolean,
						},
						SortKey: &dynamo.DynamoKeyMetadata{
							Name:      "id",
							ValueType: dynamo.String,
						},
					},
				},
			},
			dbClient: &dbWithNoError,
			entity:   validEntity,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dynamo.NewDynamoWrapper[entity](tc.dbClient, tc.dbConfig)

			_, err := db.Create(context.Background(), tc.entity)
			assert.Equal(t, !tc.hasError, err == nil)
		})
	}
}

func TestUpdate(t *testing.T) {
	validDbConfig := dynamo.DBConfig{
		TableInfo: dynamo.TableInfo{
			TableName: "tableName",
			PrimaryKey: dynamo.DBPrimaryKeyNames{
				PartitionKey: dynamo.DynamoKeyMetadata{
					Name:      "group_id",
					ValueType: dynamo.String,
				},
				SortKey: &dynamo.DynamoKeyMetadata{
					Name:      "id",
					ValueType: dynamo.Number,
				},
			},
		},
	}

	dbWithNoError := mocks.DBClient{}
	dbWithNoError.On("UpdateItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.UpdateItemOutput{}, nil)

	dbWithError := mocks.DBClient{}
	dbWithError.On("UpdateItemWithContext", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))

	validKeys := dynamo.DynamoPrimaryKey{
		PartitionKey: dynamo.DynamoAttribute{
			KeyName:   "group_id",
			ValueType: dynamo.String,
			Value:     "123",
		},
		SortKey: &dynamo.DynamoAttribute{
			KeyName:   "id",
			ValueType: dynamo.String,
			Value:     "12345",
		},
	}

	input := []dynamo.DynamoAttribute{
		{
			KeyName: "first_name",
			Value:   "newName",
		},
		{
			KeyName: "enabled",
			Value:   true,
		},
	}
	cases := []struct {
		name     string
		dbClient dynamo.DBClient
		input    []dynamo.DynamoAttribute
		keys     dynamo.DynamoPrimaryKey
		hasError bool
	}{
		{
			name:     "successfully (with partition and sort keys)",
			dbClient: &dbWithNoError,
			keys:     validKeys,
			input:    input,
		},
		{
			name:     "successfully (with partition key only)",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName:   "group_id",
					ValueType: dynamo.String,
					Value:     "123",
				},
			},
			input: input,
		},
		{
			name:     "with db error",
			dbClient: &dbWithError,
			keys:     validKeys,
			input:    input,
			hasError: true,
		},
		{
			name:     "with create partition key error",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName:   "group_id",
					ValueType: dynamo.KeyType(99), // invalid key type
					Value:     "123",
				},
			},
			input:    input,
			hasError: true,
		},
		{
			name:     "with empty update request",
			dbClient: &dbWithNoError,
			keys:     validKeys,
			input:    []dynamo.DynamoAttribute{},
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dynamo.NewDynamoWrapper[entity](tc.dbClient, validDbConfig)

			err := db.Update(context.Background(), tc.keys, tc.input)
			assert.Equal(t, !tc.hasError, err == nil)
		})
	}
}

func TestDelete(t *testing.T) {
	validDbConfig := dynamo.DBConfig{
		TableInfo: dynamo.TableInfo{
			TableName: "tableName",
			PrimaryKey: dynamo.DBPrimaryKeyNames{
				PartitionKey: dynamo.DynamoKeyMetadata{
					Name:      "group_id",
					ValueType: dynamo.String,
				},
				SortKey: &dynamo.DynamoKeyMetadata{
					Name:      "id",
					ValueType: dynamo.Number,
				},
			},
		},
	}

	dbWithNoError := mocks.DBClient{}
	dbWithNoError.On("DeleteItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.DeleteItemOutput{}, nil)

	dbWithError := mocks.DBClient{}
	dbWithError.On("DeleteItemWithContext", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))

	validKeys := dynamo.DynamoPrimaryKey{
		PartitionKey: dynamo.DynamoAttribute{
			KeyName:   "group_id",
			ValueType: dynamo.String,
			Value:     "123",
		},
		SortKey: &dynamo.DynamoAttribute{
			KeyName:   "id",
			ValueType: dynamo.String,
			Value:     "12345",
		},
	}

	cases := []struct {
		name     string
		dbClient dynamo.DBClient
		keys     dynamo.DynamoPrimaryKey
		hasError bool
	}{
		{
			name:     "successfully (with partition and sort keys)",
			dbClient: &dbWithNoError,
			keys:     validKeys,
		},
		{
			name:     "successfully (with partition key only)",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName:   "group_id",
					ValueType: dynamo.String,
					Value:     "123",
				},
			},
		},
		{
			name:     "with db error",
			dbClient: &dbWithError,
			keys:     validKeys,
			hasError: true,
		},
		{
			name:     "with create partition key error",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName:   "group_id",
					ValueType: dynamo.KeyType(99), // invalid key type
					Value:     "123",
				},
			},
			hasError: true,
		},
		{
			name:     "with empty partition key value)",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName:   "group_id",
					ValueType: dynamo.String,
					Value:     "",
				},
				SortKey: &dynamo.DynamoAttribute{
					KeyName:   "id",
					ValueType: dynamo.String,
					Value:     "",
				},
			},
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dynamo.NewDynamoWrapper[entity](tc.dbClient, validDbConfig)

			err := db.Delete(context.Background(), tc.keys)
			assert.Equal(t, !tc.hasError, err == nil)
		})
	}
}
