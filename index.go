package rapidash

import (
	"fmt"
	"net"
	"strings"

	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

type IndexType int

const (
	IndexTypeUniqueKey IndexType = iota
	IndexTypeKey
	IndexTypePrimaryKey
)

const (
	CacheKeyQueryDelimiter         = "&"
	CacheKeyQueryKeyValueDelimiter = "#"
)

type CacheKey struct {
	key  string
	hash uint32
	typ  server.CacheKeyType
	addr net.Addr
}

func (c CacheKey) String() string {
	return c.key
}

func (c CacheKey) Hash() uint32 {
	return c.hash
}

func (c CacheKey) Addr() net.Addr {
	return c.addr
}

func (c CacheKey) Type() server.CacheKeyType {
	if c.typ == server.CacheKeyTypeNone {
		return server.CacheKeyTypeSLC
	}
	return c.typ
}

func (c CacheKey) LockKey() server.CacheKey {
	return &CacheKey{
		key:  fmt.Sprintf("%s/lock", c.key),
		hash: c.hash,
		typ:  c.typ,
		addr: c.addr,
	}
}

type Index struct {
	Type             IndexType
	Table            string
	Option           *TableOption
	Columns          []string
	ColumnTypeMap    map[string]TypeID
	cacheKeyTemplate string
}

func (i *Index) HasColumn(col string) bool {
	for _, column := range i.Columns {
		if column == col {
			return true
		}
	}
	return false
}

func cacheKeyToKeyValueMap(cacheKey server.CacheKey) (map[string]string, error) {
	subCacheKey, err := getSubCacheKey(cacheKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to get sub cache key from %s: %w", cacheKey.String(), err)
	}
	cacheQueries, err := getCacheQueriesBySubCacheKey(subCacheKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to get cache queries from %s: %w", subCacheKey, err)
	}
	keyValueMap := map[string]string{}
	for _, cacheQuery := range cacheQueries {
		key, value, err := getKeyValueByCacheQuery(cacheQuery)
		if err != nil {
			return nil, xerrors.Errorf("failed to get key value pair from %s: %w", cacheQuery, err)
		}
		keyValueMap[key] = value
	}
	return keyValueMap, nil
}

func getSubCacheKey(cacheKey server.CacheKey) (string, error) {
	splitted := strings.Split(cacheKey.String(), "/")
	if len(splitted) <= 1 {
		return "", ErrInvalidCacheKey
	}
	return splitted[len(splitted)-1], nil
}

func getCacheQueriesBySubCacheKey(subCacheKey string) ([]string, error) {
	queries := strings.Split(subCacheKey, CacheKeyQueryDelimiter)
	if len(queries) == 0 {
		return nil, ErrInvalidCacheKey
	}
	return queries, nil
}

func getKeyValueByCacheQuery(query string) (string, string, error) {
	keyValuePair := strings.Split(query, CacheKeyQueryKeyValueDelimiter)
	if len(keyValuePair) != 2 {
		return "", "", ErrInvalidCacheKey
	}
	return keyValuePair[0], keyValuePair[1], nil
}

func (i *Index) createCacheQuery(key, value string) string {
	return fmt.Sprintf("%s%s%s", key, CacheKeyQueryKeyValueDelimiter, value)
}

func (i *Index) subCacheKey(value *StructValue) (string, error) {
	subKeys := []string{}
	for _, column := range i.Columns {
		indexValue := value.fields[column]
		if indexValue == nil {
			return "", xerrors.Errorf("failed to get value for %s.%s", i.Table, column)
		}
		subKeys = append(subKeys, i.createCacheQuery(column, indexValue.String()))
	}
	return strings.Join(subKeys, CacheKeyQueryDelimiter), nil
}

func (i *Index) CacheKey(value *StructValue) (*CacheKey, error) {
	subKey, err := i.subCacheKey(value)
	if err != nil {
		return nil, xerrors.Errorf("cannot get sub cache key: %w", err)
	}
	key := fmt.Sprintf(i.cacheKeyTemplate, i.Table, subKey)
	opt := i.Option
	hash := uint32(0)
	if opt.shardKey != nil {
		v, exists := value.fields[opt.ShardKey()]
		if !exists {
			return nil, xerrors.Errorf("cannot find column %s.%s for shard_key", i.Table, opt.ShardKey())
		}
		hash = v.Hash()
	} else {
		hash = NewStringValue(key).Hash()
	}
	return &CacheKey{key: key, hash: hash}, nil
}

func (i *Index) CacheKeys(slice *StructSliceValue) ([]server.CacheKey, error) {
	keys := []server.CacheKey{}
	for _, value := range slice.values {
		key, err := i.CacheKey(value)
		if err != nil {
			return nil, xerrors.Errorf("cannot get cache key: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func NewPrimaryKey(opt *TableOption, tableName string, columns []string, typ *Struct) *Index {
	columnTypeMap := map[string]TypeID{}
	for _, column := range columns {
		columnTypeMap[column] = typ.fields[column].typ
	}
	return &Index{
		Type:             IndexTypePrimaryKey,
		Table:            tableName,
		Option:           opt,
		Columns:          columns,
		ColumnTypeMap:    columnTypeMap,
		cacheKeyTemplate: "r/slc/%s/%s",
	}
}

func NewUniqueKey(opt *TableOption, tableName string, columns []string, typ *Struct) *Index {
	columnTypeMap := map[string]TypeID{}
	for _, column := range columns {
		columnTypeMap[column] = typ.fields[column].typ
	}
	return &Index{
		Type:             IndexTypeUniqueKey,
		Table:            tableName,
		Option:           opt,
		Columns:          columns,
		ColumnTypeMap:    columnTypeMap,
		cacheKeyTemplate: "r/slc/%s/uq/%s",
	}
}

func NewKey(opt *TableOption, tableName string, columns []string, typ *Struct) *Index {
	columnTypeMap := map[string]TypeID{}
	for _, column := range columns {
		columnTypeMap[column] = typ.fields[column].typ
	}
	return &Index{
		Type:             IndexTypeKey,
		Table:            tableName,
		Option:           opt,
		Columns:          columns,
		ColumnTypeMap:    columnTypeMap,
		cacheKeyTemplate: "r/slc/%s/idx/%s",
	}
}
