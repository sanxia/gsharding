package gsharding

type (
	/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	 * sharding interface
	 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
	ISharding interface {
		IShardingDatabase
		IShardingTable
	}

	IShardingDatabase interface {
		GetDatabaseKey() string
		GetDatabaseShardingRoute() string //get db route
		GetDatabaseShardingField() string //get db sharding fiele name
		GetDatabaseShardingCount() int32  //get db sharding count
	}

	IShardingTable interface {
		GetTableKey() string
		GetTableShardingRoute() string //get table route
		GetTableShardingField() string //get table sharding fiele name
		GetTableShardingCount() int32  //get table sharding count
	}

	/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	 * sharding rule interface
	 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
	IShardingRule interface {
		GetDatabaseName() string
		GetTableName() string
	}
)
