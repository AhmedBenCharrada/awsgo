package dy

// DBIndexName custom type for dynamo DB index name
type DBIndexName string

// DynamoKeyMetadata the dynamo attribute metadata.
type DynamoKeyMetadata struct {
	Name DBKey
	Type DBKeyType
}

// DBPrimaryKeyNames custom type for dynamo primary key
type DBPrimaryKeyNames struct {
	PartitionKey DynamoKeyMetadata
	SortKey      *DynamoKeyMetadata
}

// TableInfo dynamo DB table info
type TableInfo struct {
	TableName  string
	PrimaryKey DBPrimaryKeyNames
}

// DBConfig the dynamo DB table config
type DBConfig struct {
	TableInfo TableInfo
	Indexes   map[DBIndexName]DBPrimaryKeyNames
}
