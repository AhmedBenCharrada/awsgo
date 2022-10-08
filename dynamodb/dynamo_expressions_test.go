package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestNewDynamoUpdateBuildUpdateItemInput(t *testing.T) {
	t.Run("successfully", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "ID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		})

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		req, err := builder.BuildUpdateItemInput()

		assert.NoError(t, err)
		assert.NotNil(t, req.ExpressionAttributeNames["#0"])
		assert.True(t,
			*req.ExpressionAttributeNames["#0"] == "first-name" ||
				*req.ExpressionAttributeNames["#0"] == "family-name")

		assert.NotNil(t, req.ExpressionAttributeNames["#1"])
		assert.True(t,
			*req.ExpressionAttributeNames["#1"] == "first-name" ||
				*req.ExpressionAttributeNames["#1"] == "family-name")

	})

	t.Run("with empty partition key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "",
			KeyType: String,
			Value:   nil,
		})

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty sort key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("")},
		})

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty upload data", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "ID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		})

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

	t.Run("with nil partition-key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   nil,
		}).WithSortKey(&DynamoAttr{
			Name:    "ID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		})

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

	t.Run("with nil partition-key value", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: Boolean,
			Value:   &dynamodb.AttributeValue{},
		}).WithSortKey(&DynamoAttr{
			Name:    "ID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		})

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

}

func TestNewDynamoUpdateBuildDeleteItemInput(t *testing.T) {
	t.Run("successfully", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "ID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		})

		req, err := builder.BuildDeleteItemInput()
		assert.NoError(t, err)
		assert.NotNil(t, req)
	})

	t.Run("with empty partition key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "",
			KeyType: String,
			Value:   nil,
		})

		_, err := builder.BuildDeleteItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty sort key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("")},
		})

		_, err := builder.BuildDeleteItemInput()
		assert.Error(t, err)
	})
}

func TestNewDynamoUpdateBuildGetItemInput(t *testing.T) {
	t.Run("successfully", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "ID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		})

		req, err := builder.BuildGetItemInput()
		assert.NoError(t, err)
		assert.NotNil(t, req)
	})

	t.Run("with empty partition key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("")},
		})

		_, err := builder.BuildGetItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty sort key", func(t *testing.T) {
		builder := NewExpressionBuilder("table").WithPartitionKey(DynamoAttr{
			Name:    "GroupID",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("123")},
		}).WithSortKey(&DynamoAttr{
			Name:    "",
			KeyType: String,
			Value:   &dynamodb.AttributeValue{S: aws.String("")},
		})

		_, err := builder.BuildGetItemInput()
		assert.Error(t, err)
	})
}

func TestBuildBatchGetItemInput(t *testing.T) {
	cases := []struct {
		name     string
		input    []DynamoPrimaryKey
		hasError bool
	}{
		{
			name: "successfully",
			input: []DynamoPrimaryKey{
				{
					PartitionKey: DynamoAttribute{
						KeyName: "groupID",
						KeyType: String,
						Value:   "123",
					},
				},
			},
		},
		{
			name: "successfully (with sort-key)",
			input: []DynamoPrimaryKey{
				{
					PartitionKey: DynamoAttribute{
						KeyName: "groupID",
						KeyType: String,
						Value:   "123",
					},
					SortKey: &DynamoAttribute{
						KeyName: "ID",
						KeyType: String,
						Value:   "123",
					},
				},
			},
		},
		{
			name: "with invalid key",
			input: []DynamoPrimaryKey{
				{
					PartitionKey: DynamoAttribute{
						KeyName: "ID",
						KeyType: String,
						Value:   "123",
					},
				},
				{
					PartitionKey: DynamoAttribute{
						KeyName: "ID",
						KeyType: DBKeyType(99), // invalid key type
						Value:   "",
					},
				},
			},
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			query, err := NewExpressionBuilder("table").
				BuildBatchGetItemInput(tc.input...)

			assert.Equal(t, tc.hasError, err != nil)
			assert.Equal(t, !tc.hasError, query != nil)
			if query != nil {
				assert.Equal(t, !tc.hasError, len(query.RequestItems) == len(tc.input))
			}
		})
	}
}
