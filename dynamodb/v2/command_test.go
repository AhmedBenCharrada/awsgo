package dy_test

import (
	"awsgo/mocks"
	"context"
	"fmt"
	"testing"

	dy "awsgo/dynamodb/v2"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDynamodb_Create(t *testing.T) {
	dbWithNoError := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("PutItem", mock.Anything, mock.Anything).Return(&dynamodb.PutItemOutput{}, nil)
		return m
	}

	validEntity := entity{
		Id:        "id-1",
		GroupID:   aws.Int(1),
		FirstName: "f1",
		LastName:  "L1",
	}

	cases := []struct {
		name     string
		dbConfig dy.DBConfig
		dbClient func(*testing.T) dy.DynamoClient
		entity   entity
		hasError bool
	}{
		{
			name:     "successfully",
			dbConfig: dbConfig,
			dbClient: dbWithNoError,
			entity:   validEntity,
		},
		{
			name:     "successfully with empty primary key",
			dbConfig: dbConfig,
			dbClient: dbWithNoError,
			entity: entity{
				FirstName: "name2",
				LastName:  "l2",
			},
		},
		{
			name:     "with db error",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("PutItem", mock.Anything, mock.Anything).Return(&dynamodb.PutItemOutput{}, fmt.Errorf("error"))
				return m
			},
			entity:   validEntity,
			hasError: true,
		},
		{
			name: "with mismatched DB config",
			dbConfig: dy.DBConfig{
				TableInfo: dy.TableInfo{
					TableName: "tableName",
					PrimaryKey: dy.DBPrimaryKeyNames{
						PartitionKey: dy.DynamoKeyMetadata{
							Name:    "user_id",
							KeyType: dy.String,
						},
					},
				},
			},
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			entity:   validEntity,
			hasError: true,
		},
		{
			name: "with wrong partition key metadata",
			dbConfig: dy.DBConfig{
				TableInfo: dy.TableInfo{
					TableName: "tableName",
					PrimaryKey: dy.DBPrimaryKeyNames{
						PartitionKey: dy.DynamoKeyMetadata{
							Name:    "id",
							KeyType: dy.DBKeyType(99),
						},
					},
				},
			},
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			entity:   validEntity,
			hasError: true,
		},
		{
			name: "with wrong sort key metadata",
			dbConfig: dy.DBConfig{
				TableInfo: dy.TableInfo{
					TableName: "tableName",
					PrimaryKey: dy.DBPrimaryKeyNames{
						PartitionKey: dy.DynamoKeyMetadata{
							Name:    "id",
							KeyType: dy.String,
						},
						SortKey: &dy.DynamoKeyMetadata{
							Name:    "group_id",
							KeyType: dy.DBKeyType(99),
						},
					},
				},
			},
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			entity:   validEntity,
			hasError: true,
		},
		// for coverage
		{
			name: "entity with a boolean partition key",
			dbConfig: dy.DBConfig{
				TableInfo: dy.TableInfo{
					TableName: "tableName",
					PrimaryKey: dy.DBPrimaryKeyNames{
						PartitionKey: dy.DynamoKeyMetadata{
							Name:    "enabled",
							KeyType: dy.Boolean,
						},
						SortKey: &dy.DynamoKeyMetadata{
							Name:    "id",
							KeyType: dy.String,
						},
					},
				},
			},
			dbClient: dbWithNoError,
			entity:   validEntity,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dy.NewClient[entity](tc.dbClient(t), tc.dbConfig)
			_, err := db.Create(context.Background(), tc.entity)
			assert.Equal(t, !tc.hasError, err == nil)
		})
	}
}

func TestUpdate(t *testing.T) {
	dbWithNoError := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("UpdateItem", mock.Anything, mock.Anything).Return(&dynamodb.UpdateItemOutput{}, nil)
		return m
	}

	validKeys := dy.DynamoPrimaryKey{
		PartitionKey: dy.DynamoAttribute{
			KeyName: "group_id",
			KeyType: dy.String,
			Value:   "123",
		},
		SortKey: &dy.DynamoAttribute{
			KeyName: "id",
			KeyType: dy.String,
			Value:   "12345",
		},
	}

	input := []dy.DynamoAttribute{
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
		dbClient func(t *testing.T) dy.DynamoClient
		input    []dy.DynamoAttribute
		keys     dy.DynamoPrimaryKey
		hasError bool
	}{
		{
			name:     "successfully (with partition and sort keys)",
			dbClient: dbWithNoError,
			keys:     validKeys,
			input:    input,
		},
		{
			name:     "successfully (with partition key only)",
			dbClient: dbWithNoError,
			keys: dy.DynamoPrimaryKey{
				PartitionKey: dy.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dy.String,
					Value:   "123",
				},
			},
			input: input,
		},
		{
			name: "with db error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("UpdateItem", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))
				return m
			},
			keys:     validKeys,
			input:    input,
			hasError: true,
		},
		{
			name: "with create partition key error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			keys: dy.DynamoPrimaryKey{
				PartitionKey: dy.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dy.DBKeyType(99), // invalid key type
					Value:   "123",
				},
			},
			input:    input,
			hasError: true,
		},
		{
			name: "with empty update request",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			keys:     validKeys,
			input:    []dy.DynamoAttribute{},
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dy.NewClient[entity](tc.dbClient(t), dbConfig)
			err := db.Update(context.Background(), tc.keys, tc.input)
			assert.Equal(t, !tc.hasError, err == nil)
		})
	}
}

func TestDynamodb_Delete(t *testing.T) {
	dbWithNoError := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("DeleteItem", mock.Anything, mock.Anything).Return(&dynamodb.DeleteItemOutput{}, nil)
		return m
	}

	validKeys := dy.DynamoPrimaryKey{
		PartitionKey: dy.DynamoAttribute{
			KeyName: "group_id",
			KeyType: dy.String,
			Value:   "123",
		},
		SortKey: &dy.DynamoAttribute{
			KeyName: "id",
			KeyType: dy.String,
			Value:   "12345",
		},
	}

	cases := []struct {
		name     string
		dbClient func(*testing.T) dy.DynamoClient
		keys     dy.DynamoPrimaryKey
		hasError bool
	}{
		{
			name:     "successfully (with partition and sort keys)",
			dbClient: dbWithNoError,
			keys:     validKeys,
		},
		{
			name:     "successfully (with partition key only)",
			dbClient: dbWithNoError,
			keys: dy.DynamoPrimaryKey{
				PartitionKey: dy.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dy.String,
					Value:   "123",
				},
			},
		},
		{
			name: "with db error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("DeleteItem", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))
				return m
			},
			keys:     validKeys,
			hasError: true,
		},
		{
			name: "with create partition key error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			keys: dy.DynamoPrimaryKey{
				PartitionKey: dy.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dy.DBKeyType(99), // invalid key type
					Value:   "123",
				},
			},
			hasError: true,
		},
		{
			name: "with empty partition key value)",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			keys: dy.DynamoPrimaryKey{
				PartitionKey: dy.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dy.String,
					Value:   "",
				},
				SortKey: &dy.DynamoAttribute{
					KeyName: "id",
					KeyType: dy.String,
					Value:   "",
				},
			},
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dy.NewClient[entity](tc.dbClient(t), dbConfig)
			err := db.Delete(context.Background(), tc.keys)
			assert.Equal(t, !tc.hasError, err == nil)
		})
	}
}
