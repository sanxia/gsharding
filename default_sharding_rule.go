package gsharding

import (
	"fmt"
	"strconv"
)

import (
	"github.com/sanxia/ging"
	"github.com/sanxia/glib"
)

type (
	DefaultShardingRule struct {
		Sharding ISharding
		Setting  *ging.Setting
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * initializer default sharding rule
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewDefaultShardingRule(sharding ISharding, setting *ging.Setting) IShardingRule {
	return &DefaultShardingRule{
		Sharding: sharding,
		Setting:  setting,
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * sharding rule interface impl
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultShardingRule) GetDatabaseName() string {
	databaseName := s.GetDatabaseConnection().Database

	//判断是否直接路由
	if len(s.Sharding.GetDatabaseShardingRoute()) > 0 {
		databaseName = fmt.Sprintf("%s-%s", databaseName, s.Sharding.GetDatabaseShardingRoute())
	} else {
		if s.Sharding.GetDatabaseShardingCount() > 0 && len(s.Sharding.GetDatabaseShardingField()) > 0 {
			shardingFieldValue := s.GetShardingFieldValue(true)

			shardingCount := uint64(s.Sharding.GetDatabaseShardingCount()) * uint64(s.Sharding.GetTableShardingCount())
			shardingIndex := int32(shardingFieldValue % shardingCount / uint64(s.Sharding.GetDatabaseShardingCount()))

			databaseName = fmt.Sprintf("%s-%d", databaseName, shardingIndex)
		}
	}

	return databaseName
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * sharding rule interface impl
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultShardingRule) GetTableName() string {
	tableName := s.Sharding.GetTableKey()

	//判断是否直接路由
	if len(s.Sharding.GetTableShardingRoute()) > 0 {
		tableName = fmt.Sprintf("%s-%s", tableName, s.Sharding.GetTableShardingRoute())
	} else {
		if s.Sharding.GetTableShardingCount() > 0 && len(s.Sharding.GetTableShardingField()) > 0 {
			shardingFieldValue := s.GetShardingFieldValue(false)

			shardingCount := uint64(s.Sharding.GetDatabaseShardingCount()) * uint64(s.Sharding.GetTableShardingCount())
			shardingIndex := int32(shardingFieldValue % shardingCount % uint64(s.Sharding.GetTableShardingCount()))

			tableName = fmt.Sprintf("%s-%d", tableName, shardingIndex)
		}
	}

	return tableName
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * get sharding field value
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultShardingRule) GetShardingFieldValue(isDatabase bool) uint64 {
	var shardingFieldName string
	var shardingFieldValue uint64

	shardingFields := map[bool]func() string{
		true:  s.Sharding.GetDatabaseShardingField,
		false: s.Sharding.GetTableShardingField,
	}

	if shardingFieldFunc, isOk := shardingFields[isDatabase]; isOk {
		shardingFieldName = shardingFieldFunc()
	}

	if _, fieldValue, err := glib.GetStructFieldValueByName(s.Sharding, shardingFieldName); err != nil {
		panic(err)
	} else {
		switch fieldValue.(type) {
		case uint:
			if uintValue, isOk := fieldValue.(uint); isOk {
				shardingFieldValue = uint64(uintValue)
			}
		case uint8:
			if uint8Value, isOk := fieldValue.(uint8); isOk {
				shardingFieldValue = uint64(uint8Value)
			}
		case uint16:
			if uint16Value, isOk := fieldValue.(uint16); isOk {
				shardingFieldValue = uint64(uint16Value)
			}
		case uint32:
			if uint32Value, isOk := fieldValue.(uint32); isOk {
				shardingFieldValue = uint64(uint32Value)
			}
		case uint64:
			if uint64Value, isOk := fieldValue.(uint64); isOk {
				shardingFieldValue = uint64(uint64Value)
			}
		case int:
			if intValue, isOk := fieldValue.(int); isOk {
				shardingFieldValue = uint64(intValue)
			}
		case int8:
			if int8Value, isOk := fieldValue.(int8); isOk {
				shardingFieldValue = uint64(int8Value)
			}
		case int16:
			if int16Value, isOk := fieldValue.(int16); isOk {
				shardingFieldValue = uint64(int16Value)
			}
		case int32:
			if int32Value, isOk := fieldValue.(int32); isOk {
				shardingFieldValue = uint64(int32Value)
			}
		case int64:
			if int64Value, isOk := fieldValue.(int64); isOk {
				shardingFieldValue = uint64(int64Value)
			}
		case string:
			if int64Value, err := strconv.ParseInt(fieldValue.(string), 10, 64); err == nil {
				shardingFieldValue = uint64(int64Value)
			} else {
				shardingFieldValue = glib.Hash(fieldValue.(string))
			}
		}
	}

	return shardingFieldValue
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * get db connection setting
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultShardingRule) GetDatabaseConnection() ging.DatabaseConnection {
	databaseKey := s.Sharding.GetDatabaseKey()

	var dbConnection ging.DatabaseConnection
	for _, databaseConnection := range s.Setting.Database.Connections {
		if databaseConnection.Key == databaseKey {
			dbConnection = databaseConnection
			break
		}
	}

	return dbConnection
}
