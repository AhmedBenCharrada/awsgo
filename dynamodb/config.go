package dynamodb

// DBIndexName custom type for dynamo DB index name
type DBIndexName string

// DBPrimaryKeyNames custom type for dynamo primary key
type DBPrimaryKeyNames struct {
	PartitionKey DBKey
	SortKey      *DBKey
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
