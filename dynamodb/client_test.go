package dy_test

import (
	"testing"

	dy "github.com/AhmedBenCharrada/awsgo/dynamodb"
	"github.com/stretchr/testify/assert"
)

type entity struct {
	Id        string `json:"id"`
	GroupID   *int   `json:"groupID"`
	Enabled   *bool  `json:"enabled"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

func (e entity) IsEmpty() bool {
	return len(e.Id) == 0 && e.GroupID == nil && e.Enabled == nil && len(e.FirstName) == 0 && len(e.LastName) == 0
}

type wrongEntity string

func (w wrongEntity) IsEmpty() bool {
	return len(w) == 0
}

func TestNewDynamoStringAttrib(t *testing.T) {
	a := dy.NewDynamoStringAttrib("ID", "001")
	assert.NotEmpty(t, a)
}

func TestNewDynamoNumberAttrib(t *testing.T) {
	a := dy.NewDynamoNumberAttrib("GroupID", "55")
	assert.NotEmpty(t, a)
}

func TestNewDynamoBoolAttrib(t *testing.T) {
	a := dy.NewDynamoBoolAttrib("Enabled", true)
	assert.NotEmpty(t, a)
}
