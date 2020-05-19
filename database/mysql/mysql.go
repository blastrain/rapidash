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

func (ms *MySQL) Placeholders(length int) string {
	sb := &strings.Builder{}
	sb.Grow((len(ms.Placeholder(0)) + 1) * length)
	for i := 0; i < length; i++ {
		sb.WriteString(ms.Placeholder(0))
		if i < length-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func (p *MySQL) Quote(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func (p *MySQL) SupportLastInsertID() bool {
	return true
}
