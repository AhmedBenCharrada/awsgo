package dynamodb

import "github.com/aws/aws-sdk-go/service/dynamodb/expression"

// Operator ..
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

// Criteria ..
type Criteria struct {
	isEmpty bool
	builder expression.ConditionBuilder
}

// NewCriteria ..
func NewCriteria() *Criteria {
	return &Criteria{
		isEmpty: true,
		builder: expression.ConditionBuilder{},
	}
}

// GetExpression returns dynamo Filter Expressions
func (cb *Criteria) GetExpression() expression.ConditionBuilder {
	return cb.builder
}

// Or applies the OR condition for the dynamo attribute
func (cb *Criteria) Or(attribName string, value interface{}, operator Operator) *Criteria {
	if cb.isEmpty {
		cb.builder = create(attribName, value, operator)
		cb.isEmpty = false
		return cb
	}

	cond := cb.builder.Or(create(attribName, value, operator))
	cb.builder = cond
	return cb
}

// And applies the AND condition for the dynamo attribute
func (cb *Criteria) And(attribName string, value interface{}, operator Operator) *Criteria {
	if cb.isEmpty {
		cb.builder = create(attribName, value, operator)
		cb.isEmpty = false
		return cb
	}

	cond := cb.builder.And(create(attribName, value, operator))
	cb.builder = cond
	return cb
}

// Merge applies the logical And clause for all conditions.
func (cb *Criteria) Merge(conditions ...Criteria) *Criteria {
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
