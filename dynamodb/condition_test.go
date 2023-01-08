package dynamodb_test

import (
	"awsgo/dynamodb"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCriteria(t *testing.T) {
	cb := dynamodb.NewCriteria().
		And("attrib1", "some-value", dynamodb.EQUAL).
		And("attrib2", "val", dynamodb.GT).
		And("attrib3", "val", dynamodb.GE).
		And("attrib4", "val", dynamodb.LT).
		And("attrib4", "val", dynamodb.LE)

	assert.NotNil(t, cb)

	cb2 := dynamodb.NewCriteria().
		Or("attrib2", "val", dynamodb.GT).
		Or("attrib3", "val", dynamodb.GE).
		Or("attrib4", "val", dynamodb.LT).
		Or("attrib4", "val", dynamodb.LE).
		And("attrib1", "some-value", dynamodb.EQUAL)

	assert.NotNil(t, cb2)

	f := cb.Merge(*cb2)
	assert.NotNil(t, f)

	e := f.GetExpression()
	assert.NotEmpty(t, e)
}
