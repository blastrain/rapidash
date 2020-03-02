package mysql

import (
	"database/sql"
	"fmt"

	"golang.org/x/xerrors"
)

type MySQL struct{}

func (ms *MySQL) TableDDL(conn *sql.DB, tableName string) (string, error) {
	var (
		tbl string
		ddl string
	)
	if err := conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)).Scan(&tbl, &ddl); err != nil {
		return "", xerrors.Errorf("failed to execute 'SHOW CREATE TABLE `%s`': %w", tableName, err)
	}
	return ddl, nil
}
