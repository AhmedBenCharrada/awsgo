package dynamodb

import "fmt"

// const errors
var (
	ErrKeyNotFound         = fmt.Errorf("key not found")
	ErrInvalidDBKeyType    = fmt.Errorf("invalid key type")
	ErrNotFound            = fmt.Errorf("not found")
	ErrInvalidPartitionKey = fmt.Errorf("invalid partition key")
	ErrInvalidSortKey      = fmt.Errorf("invalid sort key")
)
