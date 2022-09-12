package dynamodb

func preparePartSortKey(primaryKey DynamoPrimaryKey) (partKey DynamoAttr, sortKey *DynamoAttr, err error) {
	partKey, err = createDynamoAttribute(string(primaryKey.PartitionKey.KeyName), primaryKey.PartitionKey.Value,
		primaryKey.PartitionKey.KeyType,
	)
	if err != nil {
		return
	}

	if primaryKey.SortKey == nil {
		return
	}

	sKey, sKerErr := createDynamoAttribute(string(primaryKey.SortKey.KeyName), primaryKey.SortKey.Value,
		primaryKey.PartitionKey.KeyType,
	)

	err = sKerErr
	sortKey = &sKey
	return
}
