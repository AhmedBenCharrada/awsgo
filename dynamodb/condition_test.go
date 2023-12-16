package dy_test

import (
	"testing"

	dy "github.com/AhmedBenCharrada/awsgo/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestCreateCriteria(t *testing.T) {
	cb := dy.NewCriteria().
		And("attrib1", "some-value", dy.EQUAL).
		And("attrib2", "val", dy.GT).
		And("attrib3", "val", dy.GE).
		And("attrib4", "val", dy.LT).
		And("attrib4", "val", dy.LE)

	assert.NotNil(t, cb)

	cb2 := dy.NewCriteria().
		Or("attrib2", "val", dy.GT).
		Or("attrib3", "val", dy.GE).
		Or("attrib4", "val", dy.LT).
		Or("attrib4", "val", dy.LE).
		And("attrib1", "some-value", dy.EQUAL)

	assert.NotNil(t, cb2)

	f := cb.Merge(*cb2)
	assert.NotNil(t, f)

	e := f.GetExpression()
	assert.NotEmpty(t, e)
}
