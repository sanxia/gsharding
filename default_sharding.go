package gsharding

import (
	"fmt"
	"log"
	"time"
)

import (
	"github.com/jinzhu/gorm"
)

import (
	"github.com/sanxia/ging"
	"github.com/sanxia/glib"
)

type (
	DefaultSharding struct {
		Sharding     ISharding
		ShardingRule IShardingRule
		Setting      *ging.Setting
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * initializer default sharding
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewDefaultSharding(sharding ISharding, shardingRule IShardingRule, setting *ging.Setting) *DefaultSharding {
	return &DefaultSharding{
		Sharding:     sharding,
		ShardingRule: shardingRule,
		Setting:      setting,
	}
}

func (s *DefaultSharding) GetDatabaseName() string {
	return s.ShardingRule.GetDatabaseName()
}

func (s *DefaultSharding) GetTableName() string {
	return s.ShardingRule.GetTableName()
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * get db map
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultSharding) GetDatabaseMap() *gorm.DB {
	databaseConnection := s.GetDatabaseConnection()

	isLog := !s.Setting.Log.IsDisabled && s.Setting.Database.IsLog

	dbMap, err := s.getDatabaseConnectionMap(databaseConnection, isLog)
	if err != nil {
		panic(fmt.Sprintf("gsharding connection fault: %s", err.Error()))
	}

	return dbMap
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * get db connection setting
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultSharding) GetDatabaseConnection() ging.DatabaseConnection {
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

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * get db map
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *DefaultSharding) getDatabaseConnectionMap(databaseConnection ging.DatabaseConnection, isLog bool) (*gorm.DB, error) {
	var dbIndex int32
	var dbServer ging.DatabaseServer

	dbName := s.GetDatabaseName()

	if names := glib.StringToStringSlice(dbName, "-"); len(names) > 1 {
		dbIndex = glib.StringToInt32(names[0])
	}

	for _, server := range databaseConnection.Servers {
		if server.Index == dbIndex {
			dbServer = server
		}
	}

	dsn := dbServer.Username + ":" + dbServer.Password + "@tcp(" + dbServer.Host + ")/" + dbName + "?charset=utf8mb4&parseTime=True&loc=Local"

	dbMap, err := gorm.Open(databaseConnection.Dialect, dsn)
	if err != nil {
		log.Printf("gsharding connecting error: %s", err.Error())
	}

	dbMap.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
	dbMap.DB().SetMaxIdleConns(32)
	dbMap.DB().SetMaxOpenConns(512)
	dbMap.DB().SetConnMaxLifetime(10 * time.Second)
	dbMap.LogMode(isLog)

	return dbMap, err
}
