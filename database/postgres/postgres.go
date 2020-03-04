package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"golang.org/x/xerrors"
)

type Postgres struct{}

func (d *Postgres) TableDDL(conn *sql.DB, table string) (string, error) {
	cols, err := d.getColumns(conn, table)
	if err != nil {
		return "", xerrors.Errorf("failed to get columns: %w", err)
	}
	primaryKeyDef, err := d.getPrimaryKeyDef(conn, table)
	if err != nil {
		return "", xerrors.Errorf("failed to get primary key def: %w", err)
	}
	indexDefs, err := d.getIndexDefs(conn, table)
	if err != nil {
		return "", xerrors.Errorf("failed to get index defs: %w", err)
	}
	return d.buildDDL(table, cols, primaryKeyDef, indexDefs), nil
}

func (d *Postgres) buildDDL(table string, columns []*column, primaryKeyDef string, indexDefs []string) string {
	builder := &strings.Builder{}
	builder.WriteString(fmt.Sprintf("CREATE TABLE public.%s (\n", table))
	for i, col := range columns {
		builder.WriteString(indent)
		builder.WriteString(fmt.Sprintf("%s %s", col.Name, col.DataType()))
		if !col.Nullable {
			builder.WriteString(" NOT NULL")
		}
		if i < len(columns)-1 || primaryKeyDef != "" || len(indexDefs) > 0 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}
	if primaryKeyDef != "" {
		builder.WriteString(primaryKeyDef)
		if len(indexDefs) > 0 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}
	for idx, def := range indexDefs {
		defTxt := strings.Split(def, " ")
		for _, v := range defTxt {
			if v == "UNIQUE" {
				builder.WriteString(fmt.Sprintf("%s%s ", indent, v))
				continue
			}
			if v == "INDEX" {
				builder.WriteString(fmt.Sprintf("%s ", "KEY"))
				continue
			}
			if strings.Contains(v, "(") {
				builder.WriteString(v)
				continue
			}
			if strings.Contains(v, ")") {
				builder.WriteString(v)
				continue
			}
		}
		if idx < len(indexDefs)-1 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}
	builder.WriteString(");")
	return builder.String()
}

func (d *Postgres) getColumns(conn *sql.DB, table string) ([]*column, error) {
	query := "SELECT column_name, data_type, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name=$1;"
	rows, err := conn.Query(query, table)
	if err != nil {
		return nil, xerrors.Errorf("failed to exec query %s: %w", query, err)
	}
	defer rows.Close()

	var cols []*column
	for rows.Next() {
		var colName, nullable, dataType string
		if err := rows.Scan(&colName, &dataType, &nullable); err != nil {
			return nil, xerrors.Errorf("failed to scan index key def: %w", err)
		}
		cols = append(cols, &column{
			Name:     strings.Trim(colName, `" `),
			dataType: dataType,
			Nullable: nullable == "YES",
		})
	}
	return cols, nil
}

func (d *Postgres) getIndexDefs(conn *sql.DB, table string) ([]string, error) {
	query := "SELECT indexName, indexdef FROM pg_indexes WHERE tablename=$1"
	rows, err := conn.Query(query, table)
	if err != nil {
		if err == sql.ErrNoRows {
			return []string{}, nil
		}
		return nil, xerrors.Errorf("failed to exec query %s: %w", query, err)
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var indexName, indexdef string
		if err := rows.Scan(&indexName, &indexdef); err != nil {
			return nil, xerrors.Errorf("failed to scan index key def: %w", err)
		}
		indexName = strings.Trim(indexName, `" `)
		if strings.HasSuffix(indexName, "_pkey") {
			continue
		}
		indexes = append(indexes, indexdef)
	}
	return indexes, nil
}

func (d *Postgres) getPrimaryKeyDef(conn *sql.DB, table string) (string, error) {
	query := `SELECT kcu.column_name FROM information_schema.table_constraints AS tc
	JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
	WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name=$1`
	rows, err := conn.Query(query, table)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", xerrors.Errorf("failed to exec query %s: %w", query, err)
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return "", xerrors.Errorf("failed to scan primary key def: %w", err)
		}
		columnNames = append(columnNames, columnName)
	}
	if len(columnNames) == 0 {
		return "", nil
	}
	return fmt.Sprintf("%sPRIMARY KEY (%s)", indent, strings.Join(columnNames, ",")), nil
}

type column struct {
	Name     string
	dataType string
	Nullable bool
}

const (
	smallint                 = "smallint"
	smallserial              = "smallserial"
	integer                  = "integer"
	serial                   = "serial"
	bigint                   = "bigint"
	bigserial                = "bigserial"
	timestampWithoutTimeZone = "timestamp without time zone"
	timestamp                = "timestamp"
	timeWithoutTimeZone      = "time without time zone"
	time                     = "time"
	UserDefined              = "USER-DEFINED"
	char                     = "char"

	indent = "    "
)

func (c *column) DataType() string {
	switch c.dataType {
	case smallint, integer, bigint, smallserial, serial, bigserial:
		return c.dataType
	case timestampWithoutTimeZone:
		return timestamp
	case timeWithoutTimeZone:
		return time
	case UserDefined:
		return char
	default:
		return c.dataType
	}
}
