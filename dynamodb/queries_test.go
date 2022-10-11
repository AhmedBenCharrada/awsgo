package dynamodb_test

import (
	dynamo "awsgo/dynamodb"
	"awsgo/mocks"
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGet(t *testing.T) {
	validDbConfig := dynamo.DBConfig{
		TableInfo: dynamo.TableInfo{
			TableName: "tableName",
			PrimaryKey: dynamo.DBPrimaryKeyNames{
				PartitionKey: dynamo.DynamoKeyMetadata{
					Name:    "group_id",
					KeyType: dynamo.String,
				},
				SortKey: &dynamo.DynamoKeyMetadata{
					Name:    "id",
					KeyType: dynamo.Number,
				},
			},
		},
	}

	dbWithNoError := mocks.DBClient{}
	dbWithNoError.On("GetItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"id":         {S: aws.String("123")},
			"group_id":   {N: aws.String("1234")},
			"enabled":    {BOOL: aws.Bool(true)},
			"first_name": {S: aws.String("name")},
			"last_name":  {S: aws.String("l_name")},
		},
	}, nil)

	dbWithError := mocks.DBClient{}
	dbWithError.On("GetItemWithContext", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))

	dbWithNotFoundItem := mocks.DBClient{}
	dbWithNotFoundItem.On("GetItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.GetItemOutput{}, nil)

	validKeys := dynamo.DynamoPrimaryKey{
		PartitionKey: dynamo.DynamoAttribute{
			KeyName: "group_id",
			KeyType: dynamo.String,
			Value:   "123",
		},
		SortKey: &dynamo.DynamoAttribute{
			KeyName: "id",
			KeyType: dynamo.String,
			Value:   "12345",
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
					KeyName: "group_id",
					KeyType: dynamo.String,
					Value:   "123",
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
			name:     "with not found item",
			dbClient: &dbWithNotFoundItem,
			keys:     validKeys,
			hasError: true,
		},
		{
			name:     "with create partition key error",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dynamo.DBKeyType(99), // invalid key type
					Value:   "123",
				},
			},
			hasError: true,
		},
		{
			name:     "with empty partition key value)",
			dbClient: &dbWithNoError,
			keys: dynamo.DynamoPrimaryKey{
				PartitionKey: dynamo.DynamoAttribute{
					KeyName: "group_id",
					KeyType: dynamo.String,
					Value:   "",
				},
				SortKey: &dynamo.DynamoAttribute{
					KeyName: "id",
					KeyType: dynamo.String,
					Value:   "",
				},
			},
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dynamo.NewDynamoWrapper[entity](tc.dbClient, validDbConfig)

			item, err := db.GetItem(context.Background(), tc.keys)
			assert.Equal(t, !tc.hasError, err == nil)
			assert.Equal(t, tc.hasError, item == nil)
			if !tc.hasError {
				assert.Equal(t, "123", item.ID)
				assert.NotNil(t, item.GroupID)
				assert.Equal(t, 1234, *item.GroupID)
				assert.NotNil(t, item.Enabled)
				assert.Equal(t, true, *item.Enabled)
				assert.Equal(t, "name", item.FirstName)
				assert.Equal(t, "l_name", item.LastName)
			}
		})
	}
}

func TestGetByIDs(t *testing.T) {
	validDbConfig := dynamo.DBConfig{
		TableInfo: dynamo.TableInfo{
			TableName: "tableName",
			PrimaryKey: dynamo.DBPrimaryKeyNames{
				PartitionKey: dynamo.DynamoKeyMetadata{
					Name:    "group_id",
					KeyType: dynamo.String,
				},
				SortKey: &dynamo.DynamoKeyMetadata{
					Name:    "id",
					KeyType: dynamo.Number,
				},
			},
		},
	}

	dbWithNoError := mocks.DBClient{}
	dbWithNoError.On("BatchGetItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
		Responses: map[string][]map[string]*dynamodb.AttributeValue{
			validDbConfig.TableInfo.TableName: {
				map[string]*dynamodb.AttributeValue{
					"id":         {S: aws.String("123")},
					"group_id":   {N: aws.String("1234")},
					"enabled":    {BOOL: aws.Bool(true)},
					"first_name": {S: aws.String("name")},
					"last_name":  {S: aws.String("l_name")},
				},
			},
		},
	}, nil)

	dbWithError := mocks.DBClient{}
	dbWithError.On("BatchGetItemWithContext", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))

	dbWithNotFoundItem := mocks.DBClient{}
	dbWithNotFoundItem.On("BatchGetItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
		Responses: map[string][]map[string]*dynamodb.AttributeValue{
			validDbConfig.TableInfo.TableName: {},
		},
	}, nil)

	validKeys := []dynamo.DynamoPrimaryKey{
		{
			PartitionKey: dynamo.DynamoAttribute{
				KeyName: "group_id",
				KeyType: dynamo.String,
				Value:   "123",
			},
			SortKey: &dynamo.DynamoAttribute{
				KeyName: "id",
				KeyType: dynamo.String,
				Value:   "12345",
			},
		},
	}

	cases := []struct {
		name       string
		dbClient   dynamo.DBClient
		keys       []dynamo.DynamoPrimaryKey
		itemsCount int
		hasError   bool
	}{
		{
			name:       "successfully",
			dbClient:   &dbWithNoError,
			keys:       validKeys,
			itemsCount: 1,
		},
		{
			name:     "with db error",
			dbClient: &dbWithError,
			keys:     validKeys,
			hasError: true,
		},
		{
			name:       "with not found item",
			dbClient:   &dbWithNotFoundItem,
			keys:       validKeys,
			itemsCount: 0,
		},
		{
			name:     "with invalid key",
			dbClient: &dbWithNoError,
			keys: []dynamo.DynamoPrimaryKey{
				{
					PartitionKey: dynamo.DynamoAttribute{
						KeyName: "group_id",
						KeyType: dynamo.DBKeyType(99), // invalid key type
						Value:   "123",
					},
				},
			},
			hasError: true,
		},
		{
			name: "with wrong response structure",
			dbClient: func() dynamo.DBClient {
				m := mocks.DBClient{}
				m.On("BatchGetItemWithContext", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]*dynamodb.AttributeValue{
						validDbConfig.TableInfo.TableName: {
							map[string]*dynamodb.AttributeValue{
								"userID":    {S: aws.String("123")},
								"user_name": {S: aws.String("name")},
							},
						},
					},
				}, nil)

				return &m
			}(),
			keys:     validKeys,
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dynamo.NewDynamoWrapper[entity](tc.dbClient, validDbConfig)

			items, err := db.GetItems(context.Background(), tc.keys)
			assert.Equal(t, !tc.hasError, err == nil)
			assert.Equal(t, tc.itemsCount, len(items))
		})
	}
}
