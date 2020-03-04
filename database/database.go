package database

import (
	"database/sql"

	"go.knocknote.io/rapidash/database/mysql"
	"go.knocknote.io/rapidash/database/postgres"
)

type DBType int

const (
	MySQL DBType = iota
	Postgres
)

const (
	mysqlPlugin    = "mysql"
	postgresPlugin = "postgres"
)

type Adapter interface {
	TableDDL(*sql.DB, string) (string, error)
}

func NewDBAdapter() Adapter {
	drivers := sql.Drivers()
	if len(drivers) == 0 {
		return nil
	}
	return NewAdapterWithDBType(toDBType(drivers[0]))
}

func NewAdapterWithDBType(dbType DBType) Adapter {
	switch dbType {
	case MySQL:
		return &mysql.MySQL{}
	case Postgres:
		return &postgres.Postgres{}
	}
	return nil
}

func toDBType(pluginName string) DBType {
	switch pluginName {
	case mysqlPlugin:
		return MySQL
	case postgresPlugin:
		return Postgres
	}
	return 0
}
