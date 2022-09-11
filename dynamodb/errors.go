package dynamodb

import "fmt"

var (
	ErrKeyNotFound      = fmt.Errorf("key not found")
	ErrInvalidDBKeyType = fmt.Errorf("invalid key type")
)
