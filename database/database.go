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
	count   int
	adapter *adapter
}

func (qh *QueryHelper) DBType() DBType {
	return qh.adapter.DBType
}

func (qh *QueryHelper) Placeholder() string {
	qh.count++
	return qh.adapter.Placeholder(qh.count)
}

func (qh *QueryHelper) Placeholders(n int) string {
	start := qh.count + 1
	end := start + n - 1
	qh.count += n
	return qh.adapter.Placeholders(start, end)
}

func (qh *QueryHelper) Quote(str string) string {
	return qh.adapter.Quote(str)
}

func (qh *QueryHelper) SupportLastInsertID() bool {
	return qh.adapter.SupportLastInsertID()
}

func (qh *QueryHelper) ClearCount() {
	qh.count = 0
}

type Adapter interface {
	database
	QueryHelper() *QueryHelper
}

type adapter struct {
	DBType DBType
	Database
}

func (d *adapter) QueryHelper() *QueryHelper {
	return &QueryHelper{
		count:   0,
		adapter: d,
	}
}

func NewAdapter() *adapter {
	drivers := sql.Drivers()
	if len(drivers) == 0 {
		return nil
	}
	dbType := toDBType(drivers[0])
	return &adapter{
		DBType:   dbType,
		Database: NewDatabase(dbType),
	}
}

func NewAdapterWithDBType(dbType DBType) *adapter {
	return &adapter{
		DBType:   dbType,
		Database: NewDatabase(dbType),
	}
}

func NewDatabase(dbType DBType) Database {
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

var _ Adapter = (*adapter)(nil)
