package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestNewDynamoUpdateBuildUpdateItemInput(t *testing.T) {
	t.Run("successfully", func(t *testing.T) {
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "GroupID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			}, &DynamoAttr{
				Name:  "ID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			},
		)

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
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "",
				Value: nil,
			}, nil,
		)

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty sort key", func(t *testing.T) {
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "GroupID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			},
			&DynamoAttr{
				Name:  "",
				Value: &dynamodb.AttributeValue{S: aws.String("")},
			},
		)

		builder.
			WithUpdateField("family-name", "fam-name").
			WithUpdateField("first-name", "fname")

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty upload data", func(t *testing.T) {
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "GroupID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			}, &DynamoAttr{
				Name:  "ID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			},
		)

		_, err := builder.BuildUpdateItemInput()
		assert.Error(t, err)
	})
}

func TestNewDynamoUpdateBuildDeleteItemInput(t *testing.T) {
	t.Run("successfully", func(t *testing.T) {
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "GroupID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			}, &DynamoAttr{
				Name:  "ID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			},
		)

		req, err := builder.BuildDeleteItemInput()
		assert.NoError(t, err)
		assert.NotNil(t, req)
	})

	t.Run("with empty partition key", func(t *testing.T) {
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "",
				Value: nil,
			}, nil,
		)

		_, err := builder.BuildDeleteItemInput()
		assert.Error(t, err)
	})

	t.Run("with empty sort key", func(t *testing.T) {
		builder := NewExpressionBuilder(
			"table",
			DynamoAttr{
				Name:  "GroupID",
				Value: &dynamodb.AttributeValue{S: aws.String("123")},
			},
			&DynamoAttr{
				Name:  "",
				Value: &dynamodb.AttributeValue{S: aws.String("")},
			},
		)

		_, err := builder.BuildDeleteItemInput()
		assert.Error(t, err)
	})
}
