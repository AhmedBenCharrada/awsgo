package dy_test

import (
	dy "awsgo/dynamodb/v2"
	"awsgo/mocks"
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var dbConfig = dy.DBConfig{
	TableInfo: dy.TableInfo{
		TableName: "tableName",
		PrimaryKey: dy.DBPrimaryKeyNames{
			PartitionKey: dy.DynamoKeyMetadata{
				Name:    "groupID",
				KeyType: dy.Number,
			},
			SortKey: &dy.DynamoKeyMetadata{
				Name:    "id",
				KeyType: dy.String,
			},
		},
	},
}

func TestDynamodb_Find_WithScan(t *testing.T) {
	dbWithNoError := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("Scan", mock.Anything, mock.Anything).Return(&dynamodb.ScanOutput{
			Items:            getItemAttributeValuesTestData(),
			LastEvaluatedKey: getLastEvaluatedKeysTestData(),
		}, nil)

		return m
	}

	validReq := dy.Request{
		Size: 3,
	}

	cases := []struct {
		name       string
		dbClient   func(*testing.T) dy.DynamoClient
		req        dy.Request
		itemsCount int
		hasError   bool
	}{
		{
			name:       "successfully",
			dbClient:   dbWithNoError,
			req:        validReq,
			itemsCount: 1,
		},
		{
			name:     "with last evaluated key",
			dbClient: dbWithNoError,
			req: dy.Request{
				Size: 3,
				LastEvaluatedKey: &dy.DynamoPrimaryKey{
					PartitionKey: dy.DynamoAttribute{
						KeyName: "id",
						KeyType: dy.String,
						Value:   "123",
					},
					SortKey: &dy.DynamoAttribute{
						KeyName: "groupID",
						KeyType: dy.Number,
						Value:   1234,
					},
				},
			},
			itemsCount: 1,
		},
		{
			name: "page size == 0",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			req: dy.Request{
				Size: 0,
			},
			itemsCount: 0,
		},
		{
			name: "with error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("Scan", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))
				return m
			},
			req:        validReq,
			itemsCount: 0,
			hasError:   true,
		},
		{
			name: "with empty condition",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			req: dy.Request{
				Size:             validReq.Size,
				LastEvaluatedKey: validReq.LastEvaluatedKey,
				Conditions:       []dy.Criteria{*dy.NewCriteria()},
			},
			itemsCount: 0,
			hasError:   true,
		},
		{
			name:     "with 1 condition",
			dbClient: dbWithNoError,
			req: dy.Request{
				Size:             validReq.Size,
				LastEvaluatedKey: validReq.LastEvaluatedKey,
				Conditions: []dy.Criteria{*dy.NewCriteria().
					And("firstName", "name", dy.EQUAL),
				},
			},
			itemsCount: 1,
		},
		{
			name:     "with 2 condition",
			dbClient: dbWithNoError,
			req: dy.Request{
				Size:             validReq.Size,
				LastEvaluatedKey: validReq.LastEvaluatedKey,
				Conditions: []dy.Criteria{*dy.NewCriteria().
					And("firstName", "name", dy.EQUAL),
					*dy.NewCriteria().
						And("lastName", "l_name", dy.GT)},
			},
			itemsCount: 1,
		},
		{
			name: "with unmarshal error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("Scan", mock.Anything, mock.Anything).Return(&dynamodb.ScanOutput{
					Items: []map[string]types.AttributeValue{
						{
							"name":       &types.AttributeValueMemberS{Value: "name"},
							"familyName": &types.AttributeValueMemberS{Value: "l_name"},
						},
					},
				}, nil)

				return m
			},
			req:        validReq,
			itemsCount: 0,
			hasError:   false,
		},
		{
			name: "with empty lastEvaluatedKey",
			dbClient: func(t *testing.T) dy.DynamoClient {
				db := mocks.NewDynamoClient(t)
				db.On("Scan", mock.Anything, mock.Anything).Return(&dynamodb.ScanOutput{
					Items: getItemAttributeValuesTestData(),
				}, nil)

				return db
			},
			req:        validReq,
			itemsCount: 1,
		},
		{
			name: "with wrong lastEvaluatedKey (for coverage)",
			dbClient: func(t *testing.T) dy.DynamoClient {
				db := mocks.NewDynamoClient(t)
				db.On("Scan", mock.Anything, mock.Anything).Return(&dynamodb.ScanOutput{
					Items: getItemAttributeValuesTestData(),
					LastEvaluatedKey: map[string]types.AttributeValue{
						"part": &types.AttributeValueMemberN{Value: "123"},
						"sort": &types.AttributeValueMemberS{Value: "1234"},
					},
				}, nil)

				return db
			},
			req:        validReq,
			itemsCount: 0,
			hasError:   true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dy.NewClient[entity](tc.dbClient(t), dbConfig)

			res, err := db.Find(context.Background(), tc.req)
			assert.Equal(t, !tc.hasError, err == nil, err)
			assert.Equal(t, tc.itemsCount, len(res.Items))
		})
	}
}

func TestDynamodb_WithQuery(t *testing.T) {
	validReq := dy.Request{
		Size: 3,
		PartitionKey: &dy.DynamoAttribute{
			KeyName: "groupID",
			KeyType: dy.String,
			Value:   "123",
		},
	}
	withNoErrorDB := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("Query", mock.Anything, mock.Anything).Return(&dynamodb.QueryOutput{
			Items:            getItemAttributeValuesTestData(),
			LastEvaluatedKey: getLastEvaluatedKeysTestData(),
		}, nil)

		return m
	}

	cases := []struct {
		name       string
		dbClient   func(*testing.T) dy.DynamoClient
		req        dy.Request
		itemsCount int
		hasError   bool
	}{
		{
			name:       "successfully",
			dbClient:   withNoErrorDB,
			req:        validReq,
			itemsCount: 1,
		},
		{
			name:     "with last evaluated key",
			dbClient: withNoErrorDB,
			req: dy.Request{
				Size:         3,
				PartitionKey: dy.NewDynamoNumberAttrib("groupID", "123"),
				LastEvaluatedKey: &dy.DynamoPrimaryKey{
					PartitionKey: *dy.NewDynamoNumberAttrib("groupID", "123"),
					SortKey:      dy.NewDynamoNumberAttrib("groupID", "1234"),
				},
			},
			itemsCount: 1,
		},
		{
			name: "page size == 0",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			req: dy.Request{
				Size: 0,
				PartitionKey: &dy.DynamoAttribute{
					KeyName: "groupID",
					KeyType: dy.Number,
					Value:   "123",
				},
			},
			itemsCount: 0,
		},
		{
			name: "with error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("Query", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))
				return m
			},
			req:        validReq,
			itemsCount: 0,
			hasError:   true,
		},
		{
			name: "with empty condition",
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			req: dy.Request{
				Size: validReq.Size,
				PartitionKey: &dy.DynamoAttribute{
					KeyName: "groupID",
					KeyType: dy.Number,
					Value:   "123",
				},
				LastEvaluatedKey: validReq.LastEvaluatedKey,
				Conditions:       []dy.Criteria{*dy.NewCriteria()},
			},
			itemsCount: 0,
			hasError:   true,
		},
		{
			name:     "with 1 condition",
			dbClient: withNoErrorDB,
			req: dy.Request{
				Size: validReq.Size,
				PartitionKey: &dy.DynamoAttribute{
					KeyName: "groupID",
					KeyType: dy.Number,
					Value:   "123",
				},
				LastEvaluatedKey: validReq.LastEvaluatedKey,
				Conditions: []dy.Criteria{*dy.NewCriteria().
					And("firstName", "name", dy.EQUAL),
				},
			},
			itemsCount: 1,
		},
		{
			name:     "with 2 condition",
			dbClient: withNoErrorDB,
			req: dy.Request{
				Size: validReq.Size,
				PartitionKey: &dy.DynamoAttribute{
					KeyName: "groupID",
					KeyType: dy.Number,
					Value:   "123",
				},
				LastEvaluatedKey: validReq.LastEvaluatedKey,
				Conditions: []dy.Criteria{*dy.NewCriteria().
					And("firstName", "name", dy.EQUAL),
					*dy.NewCriteria().
						And("lastName", "l_name", dy.GT)},
			},
			itemsCount: 1,
		},
		{
			name: "with unmarshal error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("Query", mock.Anything, mock.Anything).Return(&dynamodb.QueryOutput{
					Items: []map[string]types.AttributeValue{
						{
							"name":       &types.AttributeValueMemberS{Value: "name"},
							"familyName": &types.AttributeValueMemberS{Value: "l_name"},
						},
					},
				}, nil)

				return m
			},
			req:        validReq,
			itemsCount: 0,
			hasError:   false,
		},
		{
			name: "with empty lastEvaluatedKey",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("Query", mock.Anything, mock.Anything).Return(&dynamodb.QueryOutput{
					Items: getItemAttributeValuesTestData(),
				}, nil)

				return m
			},
			req:        validReq,
			itemsCount: 1,
		},
		{
			name: "with wrong lastEvaluatedKey (for coverage)",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("Query", mock.Anything, mock.Anything).Return(&dynamodb.QueryOutput{
					Items: getItemAttributeValuesTestData(),
					LastEvaluatedKey: map[string]types.AttributeValue{
						"part": &types.AttributeValueMemberN{Value: "123"},
						"sort": &types.AttributeValueMemberS{Value: "1234"},
					},
				}, nil)

				return m
			},
			req:        validReq,
			itemsCount: 0,
			hasError:   true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dy.NewClient[entity](tc.dbClient(t), dbConfig)

			res, err := db.Find(context.Background(), tc.req)
			assert.Equal(t, !tc.hasError, err == nil, err)
			assert.Equal(t, tc.itemsCount, len(res.Items))
		})
	}
}

func TestDynamodb_Get(t *testing.T) {
	dbWithNoError := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("GetItem", mock.Anything, mock.Anything).Return(&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"id":        &types.AttributeValueMemberS{Value: "123"},
				"groupid":   &types.AttributeValueMemberN{Value: "1234"},
				"enabled":   &types.AttributeValueMemberBOOL{Value: true},
				"firstName": &types.AttributeValueMemberS{Value: "name"},
				"lastName":  &types.AttributeValueMemberS{Value: "l_name"},
			},
		}, nil)
		return m
	}

	validKeys := dy.DynamoPrimaryKey{
		PartitionKey: dy.DynamoAttribute{
			KeyName: "groupID",
			KeyType: dy.Number,
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
					KeyName: "groupID",
					KeyType: dy.String,
					Value:   "123",
				},
			},
		},
		{
			name: "with db error",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("GetItem", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))
				return m
			},
			keys:     validKeys,
			hasError: true,
		},
		{
			name: "with not found item",
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("GetItem", mock.Anything, mock.Anything).Return(&dynamodb.GetItemOutput{}, nil)
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
					KeyName: "groupID",
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
					KeyName: "groupID",
					KeyType: dy.Number,
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

			item, err := db.GetItem(context.Background(), tc.keys)
			assert.Equal(t, !tc.hasError, err == nil)
			assert.Equal(t, tc.hasError, item == nil)
			if !tc.hasError {
				assert.Equal(t, "123", item.Id)
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

func TestDynamodb_GetItems(t *testing.T) {
	dbWithNoError := func(t *testing.T) dy.DynamoClient {
		m := mocks.NewDynamoClient(t)
		m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]types.AttributeValue{
				dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
			},
		}, nil)

		return m
	}

	validKeys := []dy.DynamoPrimaryKey{
		{
			PartitionKey: dy.DynamoAttribute{
				KeyName: "groupID",
				KeyType: dy.Number,
				Value:   "123",
			},
			SortKey: &dy.DynamoAttribute{
				KeyName: "id",
				KeyType: dy.String,
				Value:   "12345",
			},
		},
	}

	cases := []struct {
		name                string
		dbConfig            dy.DBConfig
		dbClient            func(*testing.T) dy.DynamoClient
		keys                []dy.DynamoPrimaryKey
		itemsCount          int
		remainingItemsCount int
		hasError            bool
	}{
		{
			name:       "successfully",
			dbConfig:   dbConfig,
			dbClient:   dbWithNoError,
			keys:       validKeys,
			itemsCount: 1,
		},
		{
			name: "n-successfully",
			dbConfig: dy.DBConfig{
				TableInfo: dy.TableInfo{
					TableName: "tableName",
					PrimaryKey: dy.DBPrimaryKeyNames{
						PartitionKey: dy.DynamoKeyMetadata{
							Name:    "id",
							KeyType: dy.String,
						},
					},
				},
			},
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: {
							map[string]types.AttributeValue{
								"id":      &types.AttributeValueMemberS{Value: "123"},
								"enabled": &types.AttributeValueMemberBOOL{Value: true},
							},
						},
					},
					UnprocessedKeys: map[string]types.KeysAndAttributes{
						dbConfig.TableInfo.TableName: {
							Keys: []map[string]types.AttributeValue{
								{
									"id": &types.AttributeValueMemberS{Value: "124"},
								},
							},
						},
					},
				}, nil)

				return m
			},
			keys: []dy.DynamoPrimaryKey{
				{
					PartitionKey: dy.DynamoAttribute{
						KeyName: "id",
						KeyType: dy.String,
						Value:   "123",
					},
				},
			},
			itemsCount:          1,
			remainingItemsCount: 1,
		},
		{
			name:     "with empty ids",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			keys:       make([]dy.DynamoPrimaryKey, 0),
			itemsCount: 0,
		},
		{
			name:     "with db error",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))

				return m
			},
			keys:                validKeys,
			remainingItemsCount: 1,
			hasError:            true,
		},
		{
			name:     "with not found item",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{}, nil)
				return m
			},
			keys:       validKeys,
			itemsCount: 0,
		},
		{
			name:     "with invalid key",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				return mocks.NewDynamoClient(t)
			},
			keys: []dy.DynamoPrimaryKey{
				{
					PartitionKey: dy.DynamoAttribute{
						KeyName: "group_id",
						KeyType: dy.DBKeyType(99), // invalid key type
						Value:   "123",
					},
				},
			},
			remainingItemsCount: 1,
			hasError:            true,
		},
		{
			name:     "with wrong response structure",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: {
							map[string]types.AttributeValue{
								"userID":    &types.AttributeValueMemberS{Value: "123"},
								"user_name": &types.AttributeValueMemberS{Value: "name"},
							},
						},
					},
				}, nil)

				return m
			},
			keys:                validKeys,
			remainingItemsCount: 1,
			hasError:            true,
		},
		{
			name:     "with remaining items",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
					},
					UnprocessedKeys: map[string]types.KeysAndAttributes{
						dbConfig.TableInfo.TableName: {
							Keys: []map[string]types.AttributeValue{
								{
									"id":      &types.AttributeValueMemberS{Value: "124"},
									"groupID": &types.AttributeValueMemberN{Value: "1245"},
								},
								{
									"id":      &types.AttributeValueMemberS{Value: "124"},
									"groupID": &types.AttributeValueMemberN{Value: "1246"},
								},
							},
						},
					},
				}, nil)

				return m
			},
			keys:                validKeys,
			itemsCount:          1,
			remainingItemsCount: 2,
		},
		{
			name:     "with remaining items but empty key (for coverage only)",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
					},
					UnprocessedKeys: map[string]types.KeysAndAttributes{
						dbConfig.TableInfo.TableName: {
							Keys: []map[string]types.AttributeValue{
								{},
							},
						},
					},
				}, nil)

				return m
			},
			keys:                validKeys,
			itemsCount:          1,
			remainingItemsCount: 0,
		},
		{
			name:     "with invalid remaining items",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
					},
					UnprocessedKeys: map[string]types.KeysAndAttributes{
						dbConfig.TableInfo.TableName: {
							Keys: []map[string]types.AttributeValue{
								{
									"username": &types.AttributeValueMemberS{Value: "124"},
								},
							},
						},
					},
				}, nil)

				return m
			},
			keys:                validKeys,
			itemsCount:          0,
			remainingItemsCount: 1,
			hasError:            true,
		},
		{
			name:     "with nil UnprocessedKeys",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
					},
					UnprocessedKeys: nil,
				}, nil)

				return m
			},
			keys:                validKeys,
			itemsCount:          1,
			remainingItemsCount: 0,
		},
		{
			name:     "with nil UnprocessedKeys for the selected table",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
					},
					UnprocessedKeys: nil,
				}, nil)

				return m
			},
			keys:                validKeys,
			itemsCount:          1,
			remainingItemsCount: 0,
		},
		{
			name:     "with empty UnprocessedKeys for the selected table",
			dbConfig: dbConfig,
			dbClient: func(t *testing.T) dy.DynamoClient {
				m := mocks.NewDynamoClient(t)
				m.On("BatchGetItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						dbConfig.TableInfo.TableName: getItemAttributeValuesTestData(),
					},
					UnprocessedKeys: map[string]types.KeysAndAttributes{
						dbConfig.TableInfo.TableName: {
							Keys: []map[string]types.AttributeValue{},
						},
					},
				}, nil)

				return m
			},
			keys:                validKeys,
			itemsCount:          1,
			remainingItemsCount: 0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := dy.NewClient[entity](tc.dbClient(t), tc.dbConfig)

			items, remaining, err := db.GetItems(context.Background(), tc.keys)
			assert.Equal(t, !tc.hasError, err == nil, err)
			assert.Equal(t, tc.itemsCount, len(items))
			assert.Equal(t, tc.remainingItemsCount, len(remaining))
		})
	}
}

func getItemAttributeValuesTestData() []map[string]types.AttributeValue {
	return []map[string]types.AttributeValue{
		{
			"id":        &types.AttributeValueMemberS{Value: "123"},
			"groupID":   &types.AttributeValueMemberN{Value: "1234"},
			"enabled":   &types.AttributeValueMemberBOOL{Value: true},
			"firstName": &types.AttributeValueMemberS{Value: "name"},
			"lastName":  &types.AttributeValueMemberS{Value: "l_name"},
		},
	}
}

func getLastEvaluatedKeysTestData() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id":      &types.AttributeValueMemberS{Value: "123"},
		"groupID": &types.AttributeValueMemberN{Value: "1234"},
	}
}
