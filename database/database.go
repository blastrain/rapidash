package database

import (
	"database/sql"

	"go.knocknote.io/rapidash/database/mysql"
	"go.knocknote.io/rapidash/database/postgres"
)

type DBType int

const (
	None DBType = iota
	MySQL
	Postgres
)

const (
	mysqlPlugin    = "mysql"
	postgresPlugin = "postgres"
)

type Database interface {
	database
	Placeholder(int) string
	Placeholders(int, int) string
}

type database interface {
	TableDDL(*sql.DB, string) (string, error)
	Quote(string) string
	SupportLastInsertID() bool
}

type QueryHelper struct {
	count    int
	database Database
}

func (qh *QueryHelper) Placeholder() string {
	qh.count++
	return qh.database.Placeholder(qh.count)
}

func (qh *QueryHelper) Placeholders(n int) string {
	start := qh.count + 1
	end := start + n - 1
	qh.count += n
	return qh.database.Placeholders(start, end)
}

func (qh *QueryHelper) Quote(str string) string {
	return qh.database.Quote(str)
}

func (qh *QueryHelper) SupportLastInsertID() bool {
	return qh.database.SupportLastInsertID()
}

func (qh *QueryHelper) ClearCount() {
	qh.count = 0
}

type Adapter interface {
	database
	QueryHelper() *QueryHelper
}

type adapter struct {
	Database
}

func (d *adapter) QueryHelper() *QueryHelper {
	return &QueryHelper{
		count:    0,
		database: d.Database,
	}
}

func NewAdapter() Adapter {
	return &adapter{
		Database: NewDatabase(),
	}
}

func NewAdapterWithDBType(dbType DBType) Adapter {
	return &adapter{
		Database: NewDatabaseWithDBType(dbType),
	}
}

func NewDatabase() Database {
	drivers := sql.Drivers()
	if len(drivers) == 0 {
		return nil
	}
	return NewDatabaseWithDBType(toDBType(drivers[0]))
}

func NewDatabaseWithDBType(dbType DBType) Database {
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
	return None
}
