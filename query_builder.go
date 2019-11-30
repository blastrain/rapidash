package rapidash

import (
	"fmt"
	"strings"

	"github.com/knocknote/vitess-sqlparser/sqlparser"
	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

type Query struct {
	columns  []string
	value    *StructValue
	index    *Index
	cacheKey server.CacheKey
}

func NewQuery(columnNum int) *Query {
	return &Query{
		columns: make([]string, 0, columnNum),
		value: &StructValue{
			fields: make(map[string]*Value, columnNum),
		},
	}
}

func (q *Query) SetIndex(index *Index) error {
	q.index = index
	key, err := index.CacheKey(q.value)
	if err != nil {
		return xerrors.Errorf("failed to get cache key: %w", err)
	}
	q.cacheKey = key
	return nil
}

func (q *Query) Add(condition Condition) {
	column := condition.Column()
	q.columns = append(q.columns, column)
	q.value.fields[column] = condition.Value()
}

func (q *Query) Index() *Index {
	return q.index
}

func (q *Query) Field(column string) *Value {
	return q.value.fields[column]
}

type QueryResult struct {
	query       *Query
	primaryKeys []server.CacheKey
	err         error
}

type QueryIterator struct {
	currentIndex         int
	keys                 []server.CacheKey
	keyToIndexMap        map[server.CacheKey]int
	primaryKeyToQueryMap map[server.CacheKey]*Query
	results              []*QueryResult
}

func (i *QueryIterator) Next() bool {
	if i.currentIndex < len(i.keys)-1 {
		i.currentIndex++
		return true
	}
	return false
}

func (i *QueryIterator) QueryByPrimaryKey(primaryKey server.CacheKey) *Query {
	return i.primaryKeyToQueryMap[primaryKey]
}

func (i *QueryIterator) Query() *Query {
	return i.results[i.currentIndex].query
}

func (i *QueryIterator) PrimaryKeys() []server.CacheKey {
	return i.results[i.currentIndex].primaryKeys
}

func (i *QueryIterator) Key() server.CacheKey {
	return i.keys[i.currentIndex]
}

func (i *QueryIterator) Error() error {
	return i.results[i.currentIndex].err
}

func (i *QueryIterator) SetPrimaryKey(primaryKey server.CacheKey) {
	result := i.results[i.currentIndex]
	if primaryKey != nil && primaryKey.String() != "" {
		result.primaryKeys = []server.CacheKey{primaryKey}
	}
	i.primaryKeyToQueryMap[primaryKey] = result.query
}

func (i *QueryIterator) SetPrimaryKeys(primaryKeys []server.CacheKey) {
	result := i.results[i.currentIndex]
	result.primaryKeys = primaryKeys
	for _, primaryKey := range primaryKeys {
		i.primaryKeyToQueryMap[primaryKey] = result.query
	}
}

func (i *QueryIterator) SetPrimaryKeyWithKey(key, primaryKey server.CacheKey) {
	result := i.results[i.keyToIndexMap[key]]
	if primaryKey != nil && primaryKey.String() != "" {
		result.primaryKeys = []server.CacheKey{primaryKey}
	}
	i.primaryKeyToQueryMap[primaryKey] = result.query
}

func (i *QueryIterator) SetPrimaryKeysWithKey(key server.CacheKey, primaryKeys []server.CacheKey) {
	result := i.results[i.keyToIndexMap[key]]
	result.primaryKeys = primaryKeys
	for _, primaryKey := range primaryKeys {
		i.primaryKeyToQueryMap[primaryKey] = result.query
	}
}

func (i *QueryIterator) SetError(err error) {
	i.results[i.currentIndex].err = err
}

func (i *QueryIterator) SetErrorWithKey(key server.CacheKey, err error) {
	i.results[i.keyToIndexMap[key]].err = err
}

func (i *QueryIterator) Reset() {
	i.currentIndex = -1
}

func NewQueryIterator(queries []*Query) *QueryIterator {
	keys := make([]server.CacheKey, len(queries))
	for idx, query := range queries {
		keys[idx] = query.cacheKey
	}
	keyToIndexMap := map[server.CacheKey]int{}
	for idx, key := range keys {
		keyToIndexMap[key] = idx
	}
	results := make([]*QueryResult, len(keys))
	for idx := range results {
		results[idx] = &QueryResult{query: queries[idx]}
	}
	return &QueryIterator{
		currentIndex:         -1,
		keys:                 keys,
		keyToIndexMap:        keyToIndexMap,
		primaryKeyToQueryMap: map[server.CacheKey]*Query{},
		results:              results,
	}
}

type ValueIterator struct {
	currentIndex  int
	keys          []server.CacheKey
	values        []*StructValue
	errs          []error
	keyToIndexMap map[server.CacheKey]int
}

func (i *ValueIterator) Next() bool {
	if i.currentIndex < len(i.keys)-1 {
		i.currentIndex++
		return true
	}
	return false
}

func (i *ValueIterator) QueryByPrimaryKey(factory *ValueFactory, primaryIndex *Index) (*Query, error) {
	cacheKey := i.keys[i.currentIndex]
	keyValueMap, err := cacheKeyToKeyValueMap(cacheKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to create cache key to key/value map: %w", err)
	}
	query := NewQuery(len(keyValueMap))
	for k, v := range keyValueMap {
		typeID := primaryIndex.ColumnTypeMap[k]
		value, err := factory.CreateValueFromString(v, typeID)
		if err != nil {
			return nil, xerrors.Errorf("failed to create value from string: %w", err)
		}
		condition := &EQCondition{
			column: k,
			value:  value,
		}
		query.Add(condition)
	}
	if err := query.SetIndex(primaryIndex); err != nil {
		return nil, xerrors.Errorf("failed to set index by primary index: %w", err)
	}
	return query, nil
}

func (i *ValueIterator) PrimaryKey() server.CacheKey {
	return i.keys[i.currentIndex]
}

func (i *ValueIterator) Value() *StructValue {
	return i.values[i.currentIndex]
}

func (i *ValueIterator) Error() error {
	return i.errs[i.currentIndex]
}

func (i *ValueIterator) SetValue(value *StructValue) {
	i.values[i.currentIndex] = value
}

func (i *ValueIterator) SetValueWithKey(key server.CacheKey, value *StructValue) {
	i.values[i.keyToIndexMap[key]] = value
}

func (i *ValueIterator) SetError(err error) {
	i.errs[i.currentIndex] = err
}

func (i *ValueIterator) SetErrorWithKey(key server.CacheKey, err error) {
	i.errs[i.keyToIndexMap[key]] = err
}

func (i *ValueIterator) Reset() {
	i.currentIndex = -1
}

func NewValueIterator(keys []server.CacheKey) *ValueIterator {
	keyToIndexMap := map[server.CacheKey]int{}
	for idx, key := range keys {
		keyToIndexMap[key] = idx
	}
	return &ValueIterator{
		currentIndex:  -1,
		keys:          keys,
		values:        make([]*StructValue, len(keys)),
		errs:          make([]error, len(keys)),
		keyToIndexMap: keyToIndexMap,
	}
}

type Queries struct {
	tableName        string
	primaryIndex     *Index
	queries          []*Query
	cacheMissQueries []*Query
	rawSQL           string
	rawSQLValues     []interface{}
	lockOpt          *LockingReadOption
	isAllSQL         bool
}

func NewQueries(tableName string, primaryIndex *Index, queryNum int) *Queries {
	return &Queries{
		tableName:        tableName,
		primaryIndex:     primaryIndex,
		queries:          make([]*Query, 0, queryNum),
		cacheMissQueries: []*Query{},
	}
}

func (q *Queries) Add(query *Query) {
	q.queries = append(q.queries, query)
}

func (q *Queries) At(idx int) *Query {
	return q.queries[idx]
}

func (q *Queries) Len() int {
	return len(q.queries)
}

func (q *Queries) Each(iter func(*Query) error) error {
	for _, query := range q.queries {
		if err := iter(query); err != nil {
			if IsCacheMiss(err) {
				q.cacheMissQueries = append(q.cacheMissQueries, query)
				continue
			}
			return xerrors.Errorf("failed to cache: %w", err)
		}
	}
	return nil
}

func (q *Queries) LoadValues(factory *ValueFactory, primaryKeyLoader func(IndexType, *QueryIterator) error, valueLoader func(*ValueIterator) error) (*StructSliceValue, error) {
	queryIter := NewQueryIterator(q.queries)
	if err := primaryKeyLoader(q.queries[0].index.Type, queryIter); err != nil {
		return nil, xerrors.Errorf("failed to load primary key: %w", err)
	}
	queryIter.Reset()

	foundValues := NewStructSliceValue()
	findPrimaryKeys := []server.CacheKey{}
	for queryIter.Next() {
		if err := queryIter.Error(); err != nil {
			if IsCacheMiss(err) {
				q.cacheMissQueries = append(q.cacheMissQueries, queryIter.Query())
				continue
			}
			return nil, xerrors.Errorf("failed to cache: %w", err)
		}
		findPrimaryKeys = append(findPrimaryKeys, queryIter.PrimaryKeys()...)
	}
	valueIter := NewValueIterator(findPrimaryKeys)
	if err := valueLoader(valueIter); err != nil {
		return nil, xerrors.Errorf("failed to load value: %w", err)
	}
	valueIter.Reset()

	existsFirstPhaseCacheMissQuery := len(q.cacheMissQueries) != 0
	alreadyAddedCacheMissQueryMap := map[*Query]struct{}{}

	for valueIter.Next() {
		if err := valueIter.Error(); err != nil {
			if IsCacheMiss(err) {
				if existsFirstPhaseCacheMissQuery {
					query := queryIter.QueryByPrimaryKey(valueIter.PrimaryKey())
					if _, exists := alreadyAddedCacheMissQueryMap[query]; !exists {
						q.cacheMissQueries = append(q.cacheMissQueries, query)
					}
					continue
				}
				query, err := valueIter.QueryByPrimaryKey(factory, q.primaryIndex)
				if err != nil {
					return nil, xerrors.Errorf("failed to get query by primary key: %w", err)
				}
				q.cacheMissQueries = append(q.cacheMissQueries, query)
				continue
			} else {
				return nil, xerrors.Errorf("failed to cache: %w", err)
			}
		}
		foundValues.Append(valueIter.Value())
	}

	return foundValues, nil
}

func (q *Queries) CacheMissQueries() []*Query {
	return q.cacheMissQueries
}

func (q *Queries) FindCacheMissQueryByStructValue(value *StructValue) *Query {
	for _, query := range q.cacheMissQueries {
		if query == nil {
			continue
		}
		allEqualColumn := true
		for _, column := range query.columns {
			if !query.value.fields[column].EQ(value.fields[column]) {
				allEqualColumn = false
				break
			}
		}
		if allEqualColumn {
			return query
		}
	}
	return nil
}

func (q *Queries) CacheMissQueriesToSQL(typ *Struct) (string, []interface{}) {
	escapedColumns := []string{}
	for _, column := range typ.Columns() {
		escapedColumns = append(escapedColumns, fmt.Sprintf("`%s`", column))
	}
	if q.rawSQL != "" {
		return fmt.Sprintf("SELECT %s FROM `%s` %s",
			strings.Join(escapedColumns, ","),
			q.tableName,
			q.rawSQL,
		), q.rawSQLValues
	} else if q.isAllSQL {
		return fmt.Sprintf("SELECT %s FROM `%s`",
			strings.Join(escapedColumns, ","),
			q.tableName,
		), nil
	}
	if len(q.cacheMissQueries) == 0 {
		return "", nil
	}
	columnMap := map[string][]*Value{}
	for _, query := range q.cacheMissQueries {
		for _, column := range query.columns {
			columnMap[column] = append(columnMap[column], query.Field(column))
		}
	}
	query := q.cacheMissQueries[0]
	conditions := []string{}
	queryArgs := []interface{}{}
	for _, column := range query.columns {
		values := columnMap[column]
		value := values[0]
		isINQuery := false
		for _, v := range values {
			if !value.EQ(v) {
				isINQuery = true
				break
			}
			value = v
		}
		var condition string
		if isINQuery {
			placeholders := []string{}
			for _, v := range values {
				if v.IsNil {
					queryArgs = append(queryArgs, nil)
				} else {
					queryArgs = append(queryArgs, v.RawValue())
				}
				placeholders = append(placeholders, "?")
			}
			condition = fmt.Sprintf("`%s` IN (%s)", column, strings.Join(placeholders, ","))
		} else {
			if !value.IsNil {
				queryArgs = append(queryArgs, value.RawValue())
				condition = fmt.Sprintf("`%s` = ?", column)
			} else {
				condition = fmt.Sprintf("`%s` IS NULL", column)
			}

		}
		conditions = append(conditions, condition)
	}
	lockOpt := q.lockOpt.String()
	if lockOpt != "" {
		lockOpt = " " + lockOpt
	}
	return fmt.Sprintf("SELECT %s FROM `%s` WHERE %s%s",
		strings.Join(escapedColumns, ","),
		q.tableName,
		strings.Join(conditions, " AND "),
		lockOpt,
	), queryArgs
}

type Condition interface {
	Value() *Value
	Column() string
	Compare(v *Value) bool
	Search(*BTree) []Leaf
	Query() string
	QueryArgs() []interface{}
	Build(*ValueFactory)
	Release()
}

type Conditions struct {
	index      int
	conditions []Condition
}

func (c *Conditions) Build(factory *ValueFactory) {
	for _, condition := range c.conditions {
		condition.Build(factory)
	}
}

func (c *Conditions) Release() {
	for _, condition := range c.conditions {
		condition.Release()
	}
}

func (c *Conditions) Len() int {
	return len(c.conditions)
}

func (c *Conditions) Append(condition Condition) {
	c.conditions = append(c.conditions, condition)
}

func (c *Conditions) Current() Condition {
	condition := c.conditions[c.index]
	c.index++
	return condition
}

func (c *Conditions) currentWithoutProgress() Condition {
	return c.conditions[c.index]
}

func (c *Conditions) Next() *Conditions {
	if c.index < len(c.conditions) {
		return &Conditions{
			index:      c.index,
			conditions: c.conditions,
		}
	}
	return nil
}

func (c *Conditions) Reset() {
	c.index = 0
}

func (c *Conditions) Columns() []string {
	columns := []string{}
	for _, condition := range c.conditions {
		columns = append(columns, condition.Column())
	}
	return columns
}

func (c *Conditions) Queries() []string {
	queries := []string{}
	for _, condition := range c.conditions {
		queries = append(queries, condition.Query())
	}
	return queries
}

func (b *QueryBuilder) AvailableIndex() bool {
	condition := b.conditions.currentWithoutProgress()
	if _, ok := condition.(*NEQCondition); ok {
		return false
	}
	return true
}

type QueryBuilder struct {
	tableName       string
	conditions      *Conditions
	inCondition     *INCondition
	sqlCondition    *SQLCondition
	orderConditions []*OrderCondition
	lockOpt         *LockingReadOption
	err             error
	isIgnoreCache   bool
	cachedQueries   *Queries
}

func NewQueryBuilder(tableName string) *QueryBuilder {
	return &QueryBuilder{
		tableName: tableName,
		conditions: &Conditions{
			conditions: []Condition{},
		},
		orderConditions: []*OrderCondition{},
	}
}

func (b *QueryBuilder) Conditions() *Conditions {
	return b.conditions
}

func (b *QueryBuilder) AvailableCache() bool {
	if b.isIgnoreCache {
		return false
	}
	for _, condition := range b.conditions.conditions {
		_, isEQCondition := condition.(*EQCondition)
		_, isINCondition := condition.(*INCondition)
		if !isEQCondition && !isINCondition {
			return false
		}
	}
	return true
}

func (b *QueryBuilder) Fields() map[string]*Value {
	fields := map[string]*Value{}
	for _, condition := range b.conditions.conditions {
		fields[condition.Column()] = condition.Value()
	}
	return fields
}

func (b *QueryBuilder) Index() string {
	return strings.Join(b.conditions.Columns(), ":")
}

func (b *QueryBuilder) indexes() []string {
	columns := b.conditions.Columns()
	indexes := []string{}
	if len(columns) < 2 {
		indexes = append(indexes, columns[0])
	}
	for idx := range columns {
		index := strings.Join(columns[:idx], ":")
		if index == "" {
			continue
		}
		indexes = append(indexes, index)
	}
	sortedIndexes := make([]string, len(indexes))
	for i := 0; i < len(indexes); i++ {
		sortedIndexes[i] = indexes[len(indexes)-1-i]
	}
	return sortedIndexes
}

func (b *QueryBuilder) SelectSQL(typ *Struct) (string, []interface{}) {
	where := []string{}
	args := []interface{}{}
	for _, condition := range b.conditions.conditions {
		where = append(where, condition.Query())
		args = append(args, condition.QueryArgs()...)
	}
	escapedColumns := []string{}
	for _, column := range typ.Columns() {
		escapedColumns = append(escapedColumns, fmt.Sprintf("`%s`", column))
	}
	lockOpt := b.lockOpt.String()
	if lockOpt != "" {
		lockOpt = " " + lockOpt
	}
	return fmt.Sprintf("SELECT %s FROM `%s` WHERE %s%s",
		strings.Join(escapedColumns, ","),
		b.tableName,
		strings.Join(where, " AND "),
		lockOpt,
	), args
}

func (b *QueryBuilder) UpdateSQL(updateMap map[string]interface{}) (string, []interface{}) {
	where := []string{}
	args := []interface{}{}
	for _, condition := range b.conditions.conditions {
		where = append(where, condition.Query())
		args = append(args, condition.QueryArgs()...)
	}
	setList := []string{}
	values := []interface{}{}
	for k, v := range updateMap {
		setList = append(setList, fmt.Sprintf("`%s` = ?", k))
		values = append(values, v)
	}
	values = append(values, args...)
	return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", b.tableName, strings.Join(setList, ","), strings.Join(where, " AND ")), values
}

func (b *QueryBuilder) DeleteSQL() (string, []interface{}) {
	where := []string{}
	args := []interface{}{}
	for _, condition := range b.conditions.conditions {
		where = append(where, condition.Query())
		args = append(args, condition.QueryArgs()...)
	}
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s", b.tableName, strings.Join(where, " AND ")), args
}

func (b *QueryBuilder) Release() {
	b.conditions.Release()
}

func (b *QueryBuilder) Build(factory *ValueFactory) {
	b.conditions.Build(factory)
}

func (b *QueryBuilder) buildINQueryWithIndex(indexes map[string]*Index) (*Queries, error) {
	queryNum := len(b.inCondition.values)
	columnNum := len(b.conditions.conditions)
	queries := NewQueries(b.tableName, b.primaryIndexFromIndexes(indexes), queryNum)
	for i := 0; i < queryNum; i++ {
		queries.Add(NewQuery(columnNum))
	}
	for _, condition := range b.conditions.conditions {
		if condition != b.inCondition {
			if _, ok := condition.(*EQCondition); !ok {
				return nil, ErrInvalidQuery
			}
			len := queries.Len()
			for i := 0; i < len; i++ {
				queries.At(i).Add(condition)
			}
		} else {
			for i, value := range b.inCondition.values {
				queries.At(i).Add(&EQCondition{
					column: b.inCondition.column,
					value:  value,
				})
			}
		}
	}
	index, exists := indexes[strings.Join(queries.At(0).columns, ":")]
	if !exists {
		return nil, ErrLookUpIndexFromQuery
	}
	for _, query := range queries.queries {
		if err := query.SetIndex(index); err != nil {
			return nil, xerrors.Errorf("failed to set index: %w", err)
		}
	}
	b.cachedQueries = queries
	return queries, nil
}

func (b *QueryBuilder) buildAllQuery() *Queries {
	b.isIgnoreCache = true
	return &Queries{
		tableName: b.tableName,
		isAllSQL:  true,
		queries:   make([]*Query, 1),
	}
}

func (b *QueryBuilder) buildRawQuery() (*Queries, error) {
	prefix := fmt.Sprintf("SELECT * FROM `%s` ", b.tableName)
	stmt, err := sqlparser.Parse(prefix + b.sqlCondition.stmt)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse %s: %w", prefix+b.sqlCondition.stmt, err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	if selectStmt.GroupBy != nil ||
		selectStmt.Having != nil ||
		selectStmt.OrderBy != nil {
		b.isIgnoreCache = true
	}
	return &Queries{
		tableName:    b.tableName,
		rawSQL:       b.sqlCondition.stmt,
		rawSQLValues: b.sqlCondition.rawValues,
		queries:      make([]*Query, 1),
	}, nil
}

func (b *QueryBuilder) primaryIndexFromIndexes(indexes map[string]*Index) *Index {
	for _, index := range indexes {
		if index.Type == IndexTypePrimaryKey {
			return index
		}
	}
	return nil
}

func (b *QueryBuilder) validateCondition(typ *Struct) error {
	for _, condition := range b.conditions.conditions {
		column := condition.Column()
		field, exists := typ.fields[column]
		if !exists {
			return xerrors.Errorf("%s.%s is not found: %w", b.tableName, column, ErrUnknownColumnName)
		}
		value := condition.Value()
		if value == nil {
			return xerrors.Errorf("%s.%s type is invalid: %w", b.tableName, column, ErrInvalidColumnType)
		}
		if value.IsNil {
			continue
		}
		if value.kind != field.kind {
			return xerrors.Errorf("%s.%s kind is %s but required %s: %w",
				b.tableName, column, field.kind, value.kind, ErrInvalidColumnType)
		}
	}
	return nil
}

func (b *QueryBuilder) BuildWithIndex(factory *ValueFactory, indexes map[string]*Index, typ *Struct) (*Queries, error) {
	if b.err != nil {
		return nil, xerrors.Errorf("failed to build query: %w", b.err)
	}
	b.conditions.Build(factory)
	if err := b.validateCondition(typ); err != nil {
		return nil, xerrors.Errorf("invalid query: %w", err)
	}
	if b.cachedQueries != nil {
		b.cachedQueries.cacheMissQueries = []*Query{}
		return b.cachedQueries, nil
	}
	if b.sqlCondition != nil {
		queries, err := b.buildRawQuery()
		if err != nil {
			return nil, xerrors.Errorf("failed to build raw query: %w", err)
		}
		return queries, nil
	} else if b.conditions.Len() == 0 {
		return b.buildAllQuery(), nil
	} else if b.inCondition != nil {
		queries, err := b.buildINQueryWithIndex(indexes)
		if err != nil {
			return nil, xerrors.Errorf("failed to build IN query with index: %w", err)
		}
		return queries, nil
	}
	columnNum := len(b.conditions.conditions)
	queries := NewQueries(b.tableName, b.primaryIndexFromIndexes(indexes), 1)
	queries.lockOpt = b.lockOpt
	query := NewQuery(columnNum)
	for _, condition := range b.conditions.conditions {
		query.Add(condition)
	}
	queries.Add(query)
	if !b.AvailableCache() {
		b.cachedQueries = queries
		return queries, nil
	}
	index, exists := indexes[strings.Join(query.columns, ":")]
	if !exists {
		return nil, ErrLookUpIndexFromQuery
	}
	if err := query.SetIndex(index); err != nil {
		return nil, xerrors.Errorf("failed to set index: %w", err)
	}
	b.cachedQueries = queries
	return queries, nil
}

func (b *QueryBuilder) Query() string {
	queries := b.conditions.Queries()
	return strings.Join(queries, " AND ")
}

func (b *QueryBuilder) Eq(column string, value interface{}) *QueryBuilder {
	b.conditions.Append(&EQCondition{column: column, rawValue: value})
	return b
}

func (b *QueryBuilder) Neq(column string, value interface{}) *QueryBuilder {
	b.conditions.Append(&NEQCondition{column: column, rawValue: value})
	return b
}

func (b *QueryBuilder) Gt(column string, value interface{}) *QueryBuilder {
	b.conditions.Append(&GTCondition{column: column, rawValue: value})
	return b
}

func (b *QueryBuilder) Lt(column string, value interface{}) *QueryBuilder {
	b.conditions.Append(&LTCondition{column: column, rawValue: value})
	return b
}

func (b *QueryBuilder) Gte(column string, value interface{}) *QueryBuilder {
	b.conditions.Append(&GTECondition{column: column, rawValue: value})
	return b
}

func (b *QueryBuilder) Lte(column string, value interface{}) *QueryBuilder {
	b.conditions.Append(&LTECondition{column: column, rawValue: value})
	return b
}

func (b *QueryBuilder) In(column string, values interface{}) *QueryBuilder {
	if b.inCondition != nil {
		b.err = ErrMultipleINQueries
		return b
	}
	condition := &INCondition{column: column, rawValues: values}
	b.inCondition = condition
	b.conditions.Append(condition)
	return b
}

type SQLCondition struct {
	stmt      string
	rawValues []interface{}
	values    []*Value
}

func (c *SQLCondition) Build(factory *ValueFactory) {
	if c.values != nil {
		return
	}
	c.values = make([]*Value, len(c.rawValues))
	for idx, rawValue := range c.rawValues {
		c.values[idx] = factory.CreateValue(rawValue)
	}
}

func (c *SQLCondition) Release() {
	if c.values == nil {
		return
	}
	for _, value := range c.values {
		value.Release()
	}
	c.values = nil
}

func (b *QueryBuilder) SQL(stmt string, values ...interface{}) *QueryBuilder {
	b.sqlCondition = &SQLCondition{stmt: stmt, rawValues: values}
	return b
}

type OrderCondition struct {
	column string
	isAsc  bool
}

func (b *QueryBuilder) OrderBy(column string) *QueryBuilder {
	b.orderConditions = append(b.orderConditions, &OrderCondition{column: column, isAsc: true})
	return b
}

func (b *QueryBuilder) OrderAsc(column string) *QueryBuilder {
	b.orderConditions = append(b.orderConditions, &OrderCondition{column: column, isAsc: true})
	return b
}

func (b *QueryBuilder) OrderDesc(column string) *QueryBuilder {
	b.orderConditions = append(b.orderConditions, &OrderCondition{column: column, isAsc: false})
	return b
}

type LockingReadOption struct {
	isSharedLock    bool // LOCK IN SHARE MODE
	isExclusiveLock bool // FOR UPDATE
}

func (o *LockingReadOption) String() string {
	if o == nil {
		return ""
	}
	if o.isSharedLock {
		return "LOCK IN SHARE MODE"
	}
	if o.isExclusiveLock {
		return "FOR UPDATE"
	}
	return ""
}

func (b *QueryBuilder) LockInShareMode() *QueryBuilder {
	b.lockOpt = &LockingReadOption{isSharedLock: true}
	return b
}

func (b *QueryBuilder) ForUpdate() *QueryBuilder {
	b.lockOpt = &LockingReadOption{isExclusiveLock: true}
	return b
}

func (b *QueryBuilder) IsUnsupportedCacheQuery() bool {
	// if used SQL() or All() in QueryBuilder, this API return false and process by CacheMissQueriesToSQL
	return b.isIgnoreCache && b.sqlCondition == nil && len(b.conditions.conditions) != 0
}

type EQCondition struct {
	column   string
	rawValue interface{}
	value    *Value
}

func (c *EQCondition) Column() string {
	return c.column
}

func (c *EQCondition) Value() *Value {
	return c.value
}

func (c *EQCondition) Compare(value *Value) bool {
	return value.EQ(c.value)
}

func (c *EQCondition) Search(tree *BTree) []Leaf {
	result := tree.searchEq(c.value)
	if result == nil {
		return []Leaf{}
	}
	return []Leaf{result}
}

func (c *EQCondition) Query() string {
	if c.rawValue == nil {
		return fmt.Sprintf("`%s` IS NULL", c.column)
	}
	return fmt.Sprintf("`%s` = ?", c.column)
}

func (c *EQCondition) QueryArgs() []interface{} {
	if c.rawValue == nil {
		return []interface{}{}
	}
	return []interface{}{c.rawValue}
}

func (c *EQCondition) Build(factory *ValueFactory) {
	if c.value != nil {
		return
	}
	c.value = factory.CreateValue(c.rawValue)
}

func (c *EQCondition) Release() {
	if c.value == nil {
		return
	}
	c.value.Release()
	c.value = nil
}

type NEQCondition struct {
	column   string
	rawValue interface{}
	value    *Value
}

func (c *NEQCondition) Column() string {
	return c.column
}

func (c *NEQCondition) Value() *Value {
	return c.value
}

func (c *NEQCondition) Query() string {
	if c.rawValue == nil {
		return fmt.Sprintf("`%s` IS NOT NULL", c.column)
	}
	return fmt.Sprintf("`%s` != ?", c.column)
}

func (c *NEQCondition) QueryArgs() []interface{} {
	if c.rawValue == nil {
		return []interface{}{}
	}
	return []interface{}{c.rawValue}
}

func (c *NEQCondition) Compare(value *Value) bool {
	return value.NEQ(c.value)
}

func (c *NEQCondition) Search(tree *BTree) []Leaf {
	log.Warn("not support not equal search")
	return nil
}

func (c *NEQCondition) Build(factory *ValueFactory) {
	if c.value != nil {
		return
	}
	c.value = factory.CreateValue(c.rawValue)
}

func (c *NEQCondition) Release() {
	if c.value == nil {
		return
	}
	c.value.Release()
	c.value = nil
}

type GTCondition struct {
	column   string
	rawValue interface{}
	value    *Value
}

func (c *GTCondition) Column() string {
	return c.column
}

func (c *GTCondition) Value() *Value {
	return c.value
}

func (c *GTCondition) Query() string {
	return fmt.Sprintf("`%s` > ?", c.column)
}

func (c *GTCondition) QueryArgs() []interface{} {
	return []interface{}{c.rawValue}
}

func (c *GTCondition) Compare(value *Value) bool {
	return value.GT(c.value)
}

func (c *GTCondition) Search(tree *BTree) []Leaf {
	return tree.searchGt(c.value)
}

func (c *GTCondition) Build(factory *ValueFactory) {
	if c.value != nil {
		return
	}
	c.value = factory.CreateValue(c.rawValue)
}

func (c *GTCondition) Release() {
	if c.value == nil {
		return
	}
	c.value.Release()
	c.value = nil
}

type GTECondition struct {
	column   string
	rawValue interface{}
	value    *Value
}

func (c *GTECondition) Column() string {
	return c.column
}

func (c *GTECondition) Value() *Value {
	return c.value
}

func (c *GTECondition) Query() string {
	return fmt.Sprintf("`%s` >= ?", c.column)
}

func (c *GTECondition) QueryArgs() []interface{} {
	return []interface{}{c.rawValue}
}

func (c *GTECondition) Compare(value *Value) bool {
	return value.GTE(c.value)
}

func (c *GTECondition) Search(tree *BTree) []Leaf {
	return tree.searchGte(c.value)
}

func (c *GTECondition) Build(factory *ValueFactory) {
	if c.value != nil {
		return
	}
	c.value = factory.CreateValue(c.rawValue)
}

func (c *GTECondition) Release() {
	if c.value == nil {
		return
	}
	c.value.Release()
	c.value = nil
}

type LTCondition struct {
	column   string
	rawValue interface{}
	value    *Value
}

func (c *LTCondition) Column() string {
	return c.column
}

func (c *LTCondition) Value() *Value {
	return c.value
}

func (c *LTCondition) Query() string {
	return fmt.Sprintf("`%s` < ?", c.column)
}

func (c *LTCondition) QueryArgs() []interface{} {
	return []interface{}{c.rawValue}
}

func (c *LTCondition) Compare(value *Value) bool {
	return value.LT(c.value)
}

func (c *LTCondition) Search(tree *BTree) []Leaf {
	return tree.searchLt(c.value)
}

func (c *LTCondition) Build(factory *ValueFactory) {
	if c.value != nil {
		return
	}
	c.value = factory.CreateValue(c.rawValue)
}

func (c *LTCondition) Release() {
	if c.value == nil {
		return
	}
	c.value.Release()
	c.value = nil
}

type LTECondition struct {
	column   string
	rawValue interface{}
	value    *Value
}

func (c *LTECondition) Column() string {
	return c.column
}

func (c *LTECondition) Value() *Value {
	return c.value
}

func (c *LTECondition) Query() string {
	return fmt.Sprintf("`%s` <= ?", c.column)
}

func (c *LTECondition) QueryArgs() []interface{} {
	return []interface{}{c.rawValue}
}

func (c *LTECondition) Compare(value *Value) bool {
	return value.LTE(c.value)
}

func (c *LTECondition) Search(tree *BTree) []Leaf {
	return tree.searchLte(c.value)
}

func (c *LTECondition) Build(factory *ValueFactory) {
	if c.value != nil {
		return
	}
	c.value = factory.CreateValue(c.rawValue)
}

func (c *LTECondition) Release() {
	if c.value == nil {
		return
	}
	c.value.Release()
	c.value = nil
}

type INCondition struct {
	column    string
	rawValues interface{}
	values    []*Value
}

func (c *INCondition) Column() string {
	return c.column
}

func (c *INCondition) Value() *Value {
	return c.values[0]
}

func (c *INCondition) Query() string {
	placeholders := make([]string, len(c.values))
	for i := 0; i < len(c.values); i++ {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("`%s` IN (%s)", c.column, strings.Join(placeholders, ","))
}

func (c *INCondition) QueryArgs() []interface{} {
	args := make([]interface{}, len(c.values))
	for i := 0; i < len(c.values); i++ {
		args[i] = c.values[i].RawValue()
	}
	return args
}

func (c *INCondition) Compare(value *Value) bool {
	for _, v := range c.values {
		if value.EQ(v) {
			return true
		}
	}
	return false
}

func (c *INCondition) Search(tree *BTree) []Leaf {
	leafs := []Leaf{}
	for _, v := range c.values {
		value := tree.searchEq(v)
		if value == nil {
			continue
		}
		leafs = append(leafs, value)
	}
	return leafs
}

func (c *INCondition) Build(factory *ValueFactory) {
	if c.values != nil {
		return
	}
	c.values = factory.CreateUniqueValues(c.rawValues)
}

func (c *INCondition) Release() {
	if c.values == nil {
		return
	}
	for _, value := range c.values {
		value.Release()
	}
	c.values = nil
}
