package dynamodb

import "github.com/aws/aws-sdk-go/service/dynamodb/expression"

type Operator int

const (
	// EQUAL the equality operator
	EQUAL Operator = iota + 1
	// LT less-than operator
	LT
	// LE less or equal operator
	LE
	// GT greater-than operator
	GT
	// GE greater or equal operator
	GE
)

type ConditionBuilder struct {
	builder expression.ConditionBuilder
}

func NewConditionBuilder() *ConditionBuilder {
	return &ConditionBuilder{
		expression.ConditionBuilder{},
	}
}

// GetExpression returns dynamo Filter Expressions
func (cb *ConditionBuilder) GetExpression() expression.ConditionBuilder {
	return cb.builder
}

// Or applies the OR condition for the dynamo attribute
func (cb *ConditionBuilder) Or(attribName string, value interface{}, operator Operator) *ConditionBuilder {
	cond := cb.builder.Or(create(attribName, value, operator))
	cb.builder = cond
	return cb
}

// And applies the AND condition for the dynamo attribute
func (cb *ConditionBuilder) And(attribName string, value interface{}, operator Operator) *ConditionBuilder {
	cond := cb.builder.And(create(attribName, value, operator))
	cb.builder = cond
	return cb
}

// Merge applies the logical And clause for all conditions.
func (cb *ConditionBuilder) Merge(conditions ...ConditionBuilder) *ConditionBuilder {
	for _, cond := range conditions {
		cb.builder.And(cond.builder)
	}

	return cb
}

func create(attribName string, value interface{}, operator Operator) expression.ConditionBuilder {
	switch operator {
	case LT:
		return expression.Name(attribName).LessThan(expression.Value(value))
	case LE:
		return expression.Name(attribName).LessThanEqual(expression.Value(value))
	case GT:
		return expression.Name(attribName).GreaterThan(expression.Value(value))
	case GE:
		return expression.Name(attribName).GreaterThanEqual(expression.Value(value))
	default:
		return expression.Name(attribName).Equal(expression.Value(value))
	}
}
