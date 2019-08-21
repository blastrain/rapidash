package rapidash

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/knocknote/vitess-sqlparser/sqlparser"
	"golang.org/x/xerrors"
)

type FirstLevelCacheMap struct {
	*sync.Map
}

func (c *FirstLevelCacheMap) set(tableName string, cache *FirstLevelCache) {
	c.Store(tableName, cache)
}

func (c *FirstLevelCacheMap) get(tableName string) (*FirstLevelCache, bool) {
	cache, exists := c.Load(tableName)
	if !exists {
		return nil, false
	}
	return cache.(*FirstLevelCache), exists
}

func NewFirstLevelCacheMap() *FirstLevelCacheMap {
	return &FirstLevelCacheMap{&sync.Map{}}
}

type FirstLevelCache struct {
	typ          *Struct
	indexTrees   map[string]*BTree
	findAllValue *StructSliceValue
	primaryKey   string
	valueFactory *ValueFactory
}

func NewFirstLevelCache(s *Struct) *FirstLevelCache {
	return &FirstLevelCache{
		typ:          s,
		indexTrees:   map[string]*BTree{},
		valueFactory: NewValueFactory(),
	}
}

func (c *FirstLevelCache) WarmUp(conn *sql.DB) (e error) {
	ddl, err := c.showCreateTable(conn)
	if err != nil {
		return xerrors.Errorf("failed to 'show create table': %w", err)
	}
	stmt, err := sqlparser.Parse(ddl)
	if err != nil {
		return xerrors.Errorf("cannot parse ddl %s: %w", ddl, err)
	}
	rows, err := c.loadAll(conn)
	if err != nil {
		return xerrors.Errorf("failed to load all records: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			e = xerrors.Errorf("failed to close rows: %w", err)
		}
	}()
	allLeaf, err := c.setupAllLeaf(rows)
	if err != nil {
		return xerrors.Errorf("cannot setup all leaf: %w", err)
	}
	for _, constraint := range (stmt.(*sqlparser.CreateTable)).Constraints {
		switch constraint.Type {
		case sqlparser.ConstraintPrimaryKey:
			c.setupPrimaryKey(constraint, allLeaf)
		case sqlparser.ConstraintUniq, sqlparser.ConstraintUniqKey, sqlparser.ConstraintUniqIndex:
			c.setupUniqKey(constraint, allLeaf)
		case sqlparser.ConstraintKey, sqlparser.ConstraintIndex:
			c.setupKey(constraint, allLeaf)
		}
	}
	tree := c.indexTrees[c.primaryKey]
	if tree != nil {
		c.findAllValue = c.flatten(tree.all())
	}
	return nil
}

func (c *FirstLevelCache) showCreateTable(conn *sql.DB) (string, error) {
	var (
		tbl string
		ddl string
	)
	if err := conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", c.typ.tableName)).Scan(&tbl, &ddl); err != nil {
		return "", xerrors.Errorf("failed to execute 'SHOW CREATE TABLE `%s`': %w", c.typ.tableName, err)
	}
	return ddl, nil
}

func (c *FirstLevelCache) loadAll(conn *sql.DB) (*sql.Rows, error) {
	columns := c.typ.Columns()
	escapedColumns := make([]string, len(columns))
	for idx, column := range columns {
		escapedColumns[idx] = fmt.Sprintf("`%s`", column)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(escapedColumns, ","), c.typ.tableName)
	rows, err := conn.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("failed to query %s: %w", query, err)
	}
	return rows, nil
}

func (c *FirstLevelCache) setupAllLeaf(rows *sql.Rows) (*StructSliceValue, error) {
	values := NewStructSliceValue()
	for rows.Next() {
		scanValues := c.typ.ScanValues(c.valueFactory)
		if err := rows.Scan(scanValues...); err != nil {
			return nil, xerrors.Errorf("cannot scan from rows: %w", err)
		}
		values.Append(c.typ.StructValue(scanValues))
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("rows has error while scanning: %w", err)
	}
	return values, nil
}

func (c *FirstLevelCache) setupPrimaryKey(constraint *sqlparser.Constraint, allLeaf *StructSliceValue) {
	indexColumn := constraint.Keys[0].String()
	c.indexTrees[indexColumn] = c.makeBTree(allLeaf, indexColumn)
	c.primaryKey = indexColumn
}

func (c *FirstLevelCache) setupUniqKey(constraint *sqlparser.Constraint, allLeaf *StructSliceValue) {
	for idx := range constraint.Keys {
		subKeys := constraint.Keys[:idx+1]
		if len(subKeys) == 0 {
			continue
		}
		indexColumns := []string{}
		for _, key := range subKeys {
			indexColumns = append(indexColumns, key.String())
		}
		tree := c.makeBTree(allLeaf, indexColumns...)
		indexKey := strings.Join(indexColumns, ":")
		c.indexTrees[indexKey] = tree
	}
}

func (c *FirstLevelCache) setupKey(constraint *sqlparser.Constraint, allLeaf *StructSliceValue) {
	for idx := range constraint.Keys {
		subKeys := constraint.Keys[:idx+1]
		if len(subKeys) == 0 {
			continue
		}
		indexColumns := []string{}
		for _, key := range subKeys {
			indexColumns = append(indexColumns, key.String())
		}
		tree := c.makeBTree(allLeaf, indexColumns...)
		indexKey := strings.Join(indexColumns, ":")
		c.indexTrees[indexKey] = tree
	}
}

func (c *FirstLevelCache) indexesFromMap(leafMap map[interface{}]*StructSliceValue) []*Value {
	indexes := []*Value{}
	for index := range leafMap {
		indexes = append(indexes, c.valueFactory.CreateValue(index))
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].LT(indexes[j])
	})
	return indexes
}

func (c *FirstLevelCache) makeBTree(allLeaf *StructSliceValue, indexColumns ...string) *BTree {
	indexColumn := indexColumns[0]
	leafMap := map[interface{}]*StructSliceValue{}
	for _, v := range allLeaf.values {
		index := v.ValueByColumn(indexColumn)
		if leafMap[index.RawValue()] == nil {
			leafMap[index.RawValue()] = NewStructSliceValue()
		}
		leafMap[index.RawValue()].Append(v)
	}
	indexes := c.indexesFromMap(leafMap)
	tree := NewBTree()
	for _, index := range indexes {
		if len(indexColumns) > 1 {
			leafs := leafMap[index.RawValue()]
			subtree := c.makeBTree(leafs, indexColumns[1:]...)
			tree.insert(index, subtree)
		} else {
			value := leafMap[index.RawValue()]
			tree.insert(index, value)
		}
	}
	return tree
}

func (c *FirstLevelCache) FindByPrimaryKey(key *Value, unmarshaler Unmarshaler) error {
	tree := c.indexTrees[c.primaryKey]
	if tree.root.isWithoutBranchAndLeaf() {
		return nil
	}
	leaf := tree.searchEq(key)
	if leaf != nil {
		values, ok := leaf.(*StructSliceValue)
		if !ok || values.Len() == 0 {
			return ErrRecordNotFoundByPrimaryKey
		}
		if values.Len() != 1 {
			return xerrors.Errorf("found duplicate entries ( %d entries ) by primary key", values.Len())
		}
		if err := unmarshaler.DecodeRapidash(values); err != nil {
			return xerrors.Errorf("failed to decode values: %w", err)
		}
	}
	return nil
}

func (c *FirstLevelCache) findIndexTreeByQueryBuilder(builder *QueryBuilder) *BTree {
	indexes := builder.indexes()
	for _, index := range indexes {
		for k, tree := range c.indexTrees {
			if k == index {
				return tree
			}
		}
	}
	return nil
}

func (c *FirstLevelCache) searchByTree(tree *BTree, conditions *Conditions) (*StructSliceValue, error) {
	totalValues := NewStructSliceValue()
	leafsOrTrees := conditions.Current().Search(tree)
	for _, leafsOrTree := range leafsOrTrees {
		values, ok := leafsOrTree.(*StructSliceValue)
		if ok {
			if values == nil {
				return nil, nil
			}
			subConditions := conditions.Next()
			for ; subConditions != nil; subConditions = subConditions.Next() {
				values = values.Filter(subConditions.Current())
			}
			totalValues.AppendSlice(values)
			continue
		}
		tree, ok := leafsOrTree.(*BTree)
		if ok {
			values, err := c.searchByTree(tree, conditions)
			if err != nil {
				return nil, xerrors.Errorf("failed to search btree: %w", err)
			}
			totalValues.AppendSlice(values)
			continue
		}
		return nil, ErrInvalidLeafs
	}
	return totalValues, nil
}

func (c *FirstLevelCache) findByQueryBuilder(builder *QueryBuilder) (*StructSliceValue, error) {
	if builder.conditions.Len() == 0 {
		return c.findAll(), nil
	}
	conditions := builder.conditions
	defer conditions.Reset()
	var indexTree *BTree
	if builder.AvailableIndex() {
		indexTree = c.findIndexTreeByQueryBuilder(builder)
	}
	if indexTree == nil {
		log.Warn(fmt.Sprintf("not found index for [select * from %s where %s]. exec full scan", c.typ.tableName, builder.Query()))
		values := c.findAll()
		if values == nil {
			return nil, nil
		}
		subConditions := conditions.Next()
		for ; subConditions != nil; subConditions = subConditions.Next() {
			values = values.Filter(subConditions.Current())
		}
		values.Sort(builder.orderConditions)
		return values, nil
	}
	if indexTree.root.isWithoutBranchAndLeaf() {
		return NewStructSliceValue(), nil
	}
	totalValues, err := c.searchByTree(indexTree, conditions)
	if err != nil {
		return nil, xerrors.Errorf("failed to search btree: %w", err)
	}
	totalValues.Sort(builder.orderConditions)
	return totalValues, nil
}

func (c *FirstLevelCache) FindByQueryBuilder(builder *QueryBuilder, unmarshaler Unmarshaler) error {
	builder.Build(c.valueFactory)
	defer builder.Release()
	values, err := c.findByQueryBuilder(builder)
	if err != nil {
		return xerrors.Errorf("failed to findByQueryBuilder: %w", err)
	}
	if values != nil && values.Len() > 0 {
		if err := unmarshaler.DecodeRapidash(values); err != nil {
			return xerrors.Errorf("failed to decode values: %w", err)
		}
	}
	return nil
}

func (c *FirstLevelCache) CountByQueryBuilder(builder *QueryBuilder) (uint64, error) {
	builder.Build(c.valueFactory)
	defer builder.Release()
	values, err := c.findByQueryBuilder(builder)
	if err != nil {
		return 0, xerrors.Errorf("failed to findByQueryBuilder: %w", err)
	}
	if values == nil {
		return 0, nil
	}
	return uint64(values.Len()), nil
}

func (c *FirstLevelCache) findAll() *StructSliceValue {
	return c.findAllValue
}

func (c *FirstLevelCache) FindAll(unmarshaler Unmarshaler) error {
	values := c.findAll()
	if values != nil && values.Len() > 0 {
		if err := unmarshaler.DecodeRapidash(values); err != nil {
			return xerrors.Errorf("failed to decode values: %w", err)
		}
	}
	return nil
}

func (c *FirstLevelCache) flatten(leafs []Leaf) *StructSliceValue {
	values := NewStructSliceValue()
	for _, leaf := range leafs {
		sliceValue, ok := leaf.(*StructSliceValue)
		if ok {
			values.AppendSlice(sliceValue)
		}
	}
	return values
}
