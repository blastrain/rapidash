package mysql

import (
	"database/sql"
	"fmt"
	"strings"

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

func (ms *MySQL) Placeholder(_ int) string {
	return "?"
}

func (ms *MySQL) Placeholders(start, end int) string {
	sb := &strings.Builder{}
	sb.Grow((len(ms.Placeholder(0)) + 1) * (end - start + 1))
	for i := start; i <= end; i++ {
		sb.WriteString(ms.Placeholder(0))
		if i < end {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func (ms *MySQL) Quote(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func (ms *MySQL) SupportLastInsertID() bool {
	return true
}
