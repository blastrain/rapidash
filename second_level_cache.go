package rapidash

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/knocknote/msgpack"
	"github.com/knocknote/vitess-sqlparser/sqlparser"
	"go.knocknote.io/rapidash/database"
	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

type SecondLevelCacheMap struct {
	*sync.Map
}

func (c *SecondLevelCacheMap) set(tableName string, cache *SecondLevelCache) {
	c.Store(tableName, cache)
}

func (c *SecondLevelCacheMap) get(tableName string) (*SecondLevelCache, bool) {
	cache, exists := c.Load(tableName)
	if !exists {
		return nil, false
	}
	return cache.(*SecondLevelCache), exists
}

func NewSecondLevelCacheMap() *SecondLevelCacheMap {
	return &SecondLevelCacheMap{&sync.Map{}}
}

type SecondLevelCache struct {
	typ                   *Struct
	opt                   *TableOption
	indexes               map[string]*Index
	primaryKey            *Index
	indexColumns          map[string]struct{}
	cacheServer           server.CacheServer
	valueDecoderPool      sync.Pool
	primaryKeyDecoderPool sync.Pool
	valueFactory          *ValueFactory
	adapter               database.Adapter
}

type TxValue struct {
	id   string
	key  string
	time time.Time
}

func (v *TxValue) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeString(v.id); err != nil {
		return nil, xerrors.Errorf("failed to encode tx.id: %w", err)
	}
	if err := enc.EncodeString(v.key); err != nil {
		return nil, xerrors.Errorf("failed to encode tx.key: %w", err)
	}
	if err := enc.EncodeTime(v.time); err != nil {
		return nil, xerrors.Errorf("failed to encode tx.time: %w", err)
	}
	return buf.Bytes(), nil
}

func (v *TxValue) Unmarshal(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeString(&v.id); err != nil {
		return xerrors.Errorf("failed to decode tx.id: %w", err)
	}
	if err := dec.DecodeString(&v.key); err != nil {
		return xerrors.Errorf("failed to decode tx.key: %w", err)
	}
	if err := dec.DecodeTime(&v.time); err != nil {
		return xerrors.Errorf("failed to decode tx.time: %w", err)
	}
	return nil
}

func (v *TxValue) String() string {
	return fmt.Sprintf(`{ "id": %s, "key": %s, "time": %s }`, v.id, v.key, v.time)
}

func (v *TxValue) EncodeLog() string {
	return v.String()
}

func NewSecondLevelCache(s *Struct, server server.CacheServer, opt TableOption, adapter database.Adapter) *SecondLevelCache {
	valueFactory := NewValueFactory()
	return &SecondLevelCache{
		typ:          s,
		opt:          &opt,
		cacheServer:  server,
		indexes:      map[string]*Index{},
		indexColumns: map[string]struct{}{},
		valueDecoderPool: sync.Pool{
			New: func() interface{} {
				return NewDecoder(s, &bytes.Buffer{}, valueFactory)
			},
		},
		primaryKeyDecoderPool: sync.Pool{
			New: func() interface{} {
				return NewPrimaryKeyDecoder(&bytes.Buffer{})
			},
		},
		valueFactory: valueFactory,
		adapter:      adapter,
	}
}

func (c *SecondLevelCache) valueDecoder() *ValueDecoder {
	return c.valueDecoderPool.Get().(*ValueDecoder)
}

func (c *SecondLevelCache) releaseValueDecoder(decoder *ValueDecoder) {
	c.valueDecoderPool.Put(decoder)
}

func (c *SecondLevelCache) primaryKeyDecoder() *PrimaryKeyDecoder {
	return c.primaryKeyDecoderPool.Get().(*PrimaryKeyDecoder)
}

func (c *SecondLevelCache) releasePrimaryKeyDecoder(decoder *PrimaryKeyDecoder) {
	c.primaryKeyDecoderPool.Put(decoder)
}

func (c *SecondLevelCache) WarmUp(conn *sql.DB) error {
	ddl, err := c.showCreateTable(conn)
	if err != nil {
		return xerrors.Errorf("failed show create table %s: %w", ddl, err)
	}
	stmt, err := sqlparser.Parse(ddl)
	if err != nil {
		return xerrors.Errorf("cannot parse ddl %s: %w", ddl, err)
	}
	for _, constraint := range (stmt.(*sqlparser.CreateTable)).Constraints {
		switch constraint.Type {
		case sqlparser.ConstraintPrimaryKey:
			c.setupPrimaryKey(constraint)
		case sqlparser.ConstraintUniq, sqlparser.ConstraintUniqKey, sqlparser.ConstraintUniqIndex:
			c.setupUniqKey(constraint)
		case sqlparser.ConstraintKey, sqlparser.ConstraintIndex:
			c.setupKey(constraint)
		}
	}
	return nil
}

func (c *SecondLevelCache) showCreateTable(conn *sql.DB) (string, error) {
	ddl, err := c.adapter.TableDDL(conn, c.typ.tableName)
	if err != nil {
		return "", xerrors.Errorf("failed to get ddl for %s: %w", c.typ.tableName)
	}
	return ddl, nil
}

func (c *SecondLevelCache) setupPrimaryKey(constraint *sqlparser.Constraint) {
	columns := []string{}
	isNotFoundShardKey := true
	shardKey := c.opt.ShardKey()
	for _, key := range constraint.Keys {
		column := key.String()
		if column == shardKey {
			isNotFoundShardKey = false
		}
		c.indexColumns[column] = struct{}{}
		columns = append(columns, column)
	}
	primaryKey := strings.Join(columns, ":")
	for idx := range columns {
		subColumns := columns[: idx+1 : idx+1]
		if len(subColumns) == 0 {
			continue
		}
		index := strings.Join(subColumns, ":")
		if shardKey != "" && isNotFoundShardKey {
			subColumns = append(subColumns, shardKey)
		}
		if index == primaryKey {
			c.primaryKey = NewPrimaryKey(c.opt, c.typ.tableName, subColumns, c.typ)
			c.indexes[strings.Join(subColumns, ":")] = c.primaryKey
		} else {
			c.indexes[strings.Join(subColumns, ":")] = NewKey(c.opt, c.typ.tableName, subColumns, c.typ)
		}
	}
}

func (c *SecondLevelCache) setupUniqKey(constraint *sqlparser.Constraint) {
	uniqKeys := []string{}
	for _, key := range constraint.Keys {
		c.indexColumns[key.String()] = struct{}{}
		uniqKeys = append(uniqKeys, key.String())
	}
	uniqKey := strings.Join(uniqKeys, ":")
	for idx := range constraint.Keys {
		subKeys := constraint.Keys[:idx+1]
		if len(subKeys) == 0 {
			continue
		}
		columns := []string{}
		for _, key := range subKeys {
			columns = append(columns, key.String())
		}
		index := strings.Join(columns, ":")
		if index == uniqKey {
			c.indexes[index] = NewUniqueKey(c.opt, c.typ.tableName, columns, c.typ)
		} else {
			c.indexes[index] = NewKey(c.opt, c.typ.tableName, columns, c.typ)
		}
	}
}

func (c *SecondLevelCache) setupKey(constraint *sqlparser.Constraint) {
	for idx := range constraint.Keys {
		subKeys := constraint.Keys[:idx+1]
		if len(subKeys) == 0 {
			continue
		}
		columns := []string{}
		for _, key := range subKeys {
			c.indexColumns[key.String()] = struct{}{}
			columns = append(columns, key.String())
		}
		index := strings.Join(columns, ":")
		c.indexes[index] = NewKey(c.opt, c.typ.tableName, columns, c.typ)
	}
}

func (c *SecondLevelCache) lockKey(tx *Tx, key server.CacheKey) error {
	value := &TxValue{
		id:   tx.id,
		key:  key.String(),
		time: time.Now(),
	}
	bytes, err := value.Marshal()
	if err != nil {
		return xerrors.Errorf("failed to marshal tx: %w", err)
	}
	lockKey := key.LockKey()
	log.Add(tx.id, lockKey, value)
	if err := c.cacheServer.Add(lockKey, bytes, c.opt.LockExpiration()); err != nil {
		content, getErr := c.cacheServer.Get(lockKey)
		if IsCacheMiss(getErr) {
			return xerrors.Errorf("fatal error. cannot add transaction key. but transaction key doesn't exist: %w", err)
		}
		if getErr != nil {
			return xerrors.Errorf("fatal error. cannot add transaction key (reason %s) and cannot get value by transaction key (reason %s)",
				err.Error(), getErr.Error(),
			)
		}
		value := &TxValue{}
		if err := value.Unmarshal(content.Value); err != nil {
			return xerrors.Errorf("lock key (%s) is already added: %w", lockKey, err)
		}
		return xerrors.Errorf("lock key (%s) is already added. value is %s: %w", lockKey, value, err)
	}
	tx.lockKeys = append(tx.lockKeys, lockKey)
	return nil
}

func (c *SecondLevelCache) set(tx *Tx, key server.CacheKey, value []byte, logenc LogEncoder) error {
	keyStr := key.String()
	if c.opt.PessimisticLock() {
		if _, exists := tx.pendingQueries[keyStr]; !exists {
			if err := c.lockKey(tx, key); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	tx.pendingQueries[keyStr] = &PendingQuery{
		QueryLog: &QueryLog{
			Command: string(SLCCommandSet),
			Key:     keyStr,
			Hash:    key.Hash(),
			Type:    server.CacheKeyTypeSLC,
		},
		fn: func() error {
			log.Set(tx.id, SLCServer, key, logenc)
			casID := uint64(0)
			if c.opt.OptimisticLock() {
				casID = tx.stash.casIDs[key.String()]
			}
			if err := c.cacheServer.Set(&server.CacheStoreRequest{
				Key:        key,
				Value:      value,
				Expiration: c.opt.Expiration(),
				CasID:      casID,
			}); err != nil {
				return xerrors.Errorf("failed to set cache: %w", err)
			}
			return nil
		},
	}
	return nil
}

func (c *SecondLevelCache) setPrimaryKey(tx *Tx, key server.CacheKey, value *StructValue) error {
	if value == nil {
		log.Set(tx.id, SLCStash, key, value)
		if err := c.set(tx, key, nil, value); err != nil {
			return xerrors.Errorf("failed to set primary key: %w", err)
		}
		return nil
	}
	content, err := value.encodeValue()
	if err != nil {
		return xerrors.Errorf("failed to encode value: %w", err)
	}
	log.Set(tx.id, SLCStash, key, value)
	tx.stash.primaryKeyToValue[key.String()] = value
	if err := c.set(tx, key, content, value); err != nil {
		return xerrors.Errorf("failed to set value: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) setUniqueKey(tx *Tx, uniqueKey, primaryKey server.CacheKey) error {
	var writer bytes.Buffer
	enc := msgpack.NewEncoder(&writer)
	var primaryKeyText string
	if primaryKey == nil {
		primaryKeyText = ""
	} else {
		primaryKeyText = primaryKey.String()
	}
	if err := enc.EncodeString(primaryKeyText); err != nil {
		return xerrors.Errorf("failed to encode primary key: %w", err)
	}
	log.Set(tx.id, SLCStash, uniqueKey, LogString(primaryKeyText))
	tx.stash.uniqueKeyToPrimaryKey[uniqueKey.String()] = primaryKey
	if err := c.set(tx, uniqueKey, writer.Bytes(), LogString(primaryKeyText)); err != nil {
		return xerrors.Errorf("failed to set cache by unique key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) setKey(tx *Tx, key server.CacheKey, primaryKeys []server.CacheKey) error {
	var writer bytes.Buffer
	enc := msgpack.NewEncoder(&writer)
	if err := enc.EncodeArrayHeader(len(primaryKeys)); err != nil {
		return xerrors.Errorf("failed to encode array header: %w", err)
	}
	for _, primaryKey := range primaryKeys {
		if err := enc.EncodeString(primaryKey.String()); err != nil {
			return xerrors.Errorf("failed to encode primary key: %w", err)
		}
	}
	log.Set(tx.id, SLCStash, key, LogStrings(primaryKeys))
	tx.stash.keyToPrimaryKeys[key.String()] = primaryKeys
	if err := c.set(tx, key, writer.Bytes(), LogStrings(primaryKeys)); err != nil {
		return xerrors.Errorf("failed to set cache by key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) update(tx *Tx, key server.CacheKey, value []byte, logenc LogEncoder) error {
	keyStr := key.String()
	if c.opt.PessimisticLock() {
		if _, exists := tx.pendingQueries[keyStr]; !exists {
			if err := c.lockKey(tx, key); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	tx.pendingQueries[keyStr] = &PendingQuery{
		QueryLog: &QueryLog{
			Command: string(SLCCommandUpdate),
			Key:     keyStr,
			Hash:    key.Hash(),
			Type:    server.CacheKeyTypeSLC,
		},
		fn: func() error {
			log.Update(tx.id, SLCServer, key, logenc)
			casID := uint64(0)
			if c.opt.OptimisticLock() {
				casID = tx.stash.casIDs[key.String()]
			}
			if err := c.cacheServer.Set(&server.CacheStoreRequest{
				Key:        key,
				Value:      value,
				Expiration: c.opt.Expiration(),
				CasID:      casID,
			}); err != nil {
				return xerrors.Errorf("failed to update cache: %w", err)
			}
			return nil
		},
	}
	return nil
}

func (c *SecondLevelCache) updatePrimaryKey(tx *Tx, key server.CacheKey, value *StructValue) error {
	log.Update(tx.id, SLCStash, key, value)
	tx.stash.primaryKeyToValue[key.String()] = value
	content, err := value.encodeValue()
	if err != nil {
		return xerrors.Errorf("failed to encode value: %w", err)
	}
	if err := c.update(tx, key, content, value); err != nil {
		return xerrors.Errorf("failed to update value: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) delete(tx *Tx, key server.CacheKey) error {
	keyStr := key.String()
	if c.opt.PessimisticLock() {
		if _, exists := tx.pendingQueries[keyStr]; !exists {
			if err := c.lockKey(tx, key); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	tx.pendingQueries[keyStr] = &PendingQuery{
		QueryLog: &QueryLog{
			Command: string(SLCCommandDelete),
			Key:     keyStr,
			Hash:    key.Hash(),
			Type:    server.CacheKeyTypeSLC,
		},
		fn: func() error {
			log.Delete(tx.id, SLCServer, key)
			if err := c.cacheServer.Delete(key); err != nil {
				return xerrors.Errorf("failed to delete cache: %w", err)
			}
			return nil
		},
	}
	return nil
}

func (c *SecondLevelCache) deletePrimaryKey(tx *Tx, key server.CacheKey) error {
	log.Delete(tx.id, SLCStash, key)
	tx.stash.primaryKeyToValue[key.String()] = nil
	if err := c.delete(tx, key); err != nil {
		return xerrors.Errorf("failed to delete primary key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) deleteUniqueKeyOrOldKey(tx *Tx, key server.CacheKey) error {
	log.Delete(tx.id, SLCStash, key)
	tx.stash.uniqueKeyToPrimaryKey[key.String()] = nil
	tx.stash.oldKey[key.String()] = struct{}{}
	if err := c.delete(tx, key); err != nil {
		return xerrors.Errorf("failed to delete unique key or old key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) deleteOldKey(tx *Tx, key server.CacheKey) error {
	log.Delete(tx.id, SLCStash, key)
	tx.stash.oldKey[key.String()] = struct{}{}
	if err := c.delete(tx, key); err != nil {
		return xerrors.Errorf("failed to delete old key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) encode(marshaler Marshaler) ([]byte, *StructValue, error) {
	enc := NewStructEncoder(c.typ, c.valueFactory)
	if err := marshaler.EncodeRapidash(enc); err != nil {
		return nil, nil, xerrors.Errorf("failed to encode: %w", err)
	}
	content, err := enc.Encode()
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to encode: %w", err)
	}
	return content, enc.value, nil
}

func (c *SecondLevelCache) cacheKeyByPrimaryKeyValue(v *Value) (server.CacheKey, error) {
	if len(c.primaryKey.Columns) > 1 {
		// TODO: if shard_key set by TableOption and it difference primary key column, always failure.
		return nil, ErrCreateCacheKeyAtMultiplePrimaryKeys
	}
	primaryKeyColumn := c.primaryKey.Columns[0]
	structValue := &StructValue{
		typ:    c.typ,
		fields: map[string]*Value{},
	}
	structValue.fields[primaryKeyColumn] = v
	key, err := c.primaryKey.CacheKey(structValue)
	if err != nil {
		return nil, xerrors.Errorf("failed to get cache key: %w", err)
	}
	return key, nil
}

func (c *SecondLevelCache) UpdateByPrimaryKey(tx *Tx, marshaler Marshaler) error {
	_, value, err := c.encode(marshaler)
	if err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	defer value.Release()
	key, err := c.primaryKey.CacheKey(value)
	if err != nil {
		return xerrors.Errorf("failed to get cache key: %w", err)
	}
	if err := c.updatePrimaryKey(tx, key, value); err != nil {
		return xerrors.Errorf("failed to update primary key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) DeleteByPrimaryKey(tx *Tx, v *Value) error {
	key, err := c.cacheKeyByPrimaryKeyValue(v)
	if err != nil {
		return xerrors.Errorf("failed to get cache key: %w", err)
	}
	if err := c.deletePrimaryKey(tx, key); err != nil {
		return xerrors.Errorf("failed to delete primary key: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) decodePrimaryKey(content []byte, flags uint32) (server.CacheKey, error) {
	decoder := c.primaryKeyDecoder()
	defer func() {
		c.releasePrimaryKeyDecoder(decoder)
	}()
	decoder.SetBuffer(content)
	primaryKey, err := decoder.Decode()
	if err != nil {
		return nil, xerrors.Errorf("failed to decode primary key: %w", err)
	}
	hash := flags
	if c.opt.shardKey == nil {
		hash = NewStringValue(primaryKey).Hash()
	}
	return &CacheKey{key: primaryKey, hash: hash}, nil
}

func (c *SecondLevelCache) decodeMultiplePrimaryKeys(content []byte, flags uint32) ([]server.CacheKey, error) {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return nil, xerrors.Errorf("failed to decode array length: %w", err)
	}
	primaryKeys := make([]server.CacheKey, len)
	for i := 0; i < len; i++ {
		var v string
		if err := dec.DecodeString(&v); err != nil {
			return nil, xerrors.Errorf("failed to decode string: %w", err)
		}
		hash := flags
		if c.opt.shardKey == nil {
			hash = NewStringValue(v).Hash()
		}
		primaryKeys[i] = &CacheKey{key: v, hash: hash}
	}
	return primaryKeys, nil
}

func (c *SecondLevelCache) findByPrimaryKeys(tx *Tx, valueIter *ValueIterator) error {
	requestKeys := []server.CacheKey{}
	for valueIter.Next() {
		if _, exists := tx.stash.oldKey[valueIter.PrimaryKey().String()]; exists {
			// need lookup db
			valueIter.SetErrorWithKey(valueIter.PrimaryKey(), server.ErrCacheMiss)
			continue
		}
		value, exists := tx.stash.primaryKeyToValue[valueIter.PrimaryKey().String()]
		if exists {
			log.Get(tx.id, SLCStash, valueIter.PrimaryKey(), value)
			valueIter.SetValue(value)
		} else {
			requestKeys = append(requestKeys, valueIter.PrimaryKey())
		}
	}
	if len(requestKeys) == 0 {
		return nil
	}
	iter, err := c.cacheServer.GetMulti(requestKeys)
	if err != nil {
		return xerrors.Errorf("failed to get primary keys from server: %w", err)
	}
	var values *StructSliceValue
	if !isNopLogger {
		values = NewStructSliceValue()
	}
	decoder := c.valueDecoder()
	defer c.releaseValueDecoder(decoder)
	for iter.Next() {
		if err := iter.Error(); err != nil {
			valueIter.SetErrorWithKey(iter.Key(), xerrors.Errorf("set error: %w", err))
			continue
		}
		content := iter.Content()
		var value *StructValue
		if len(content.Value) > 0 {
			decoder.SetBuffer(content.Value)
			var err error
			value, err = decoder.Decode()
			if err != nil {
				valueIter.SetErrorWithKey(iter.Key(), xerrors.Errorf("%s: %w", err.Error(), server.ErrCacheMiss))
				continue
			}
		}
		key := iter.Key().String()
		tx.stash.primaryKeyToValue[key] = value
		tx.stash.casIDs[key] = content.CasID
		valueIter.SetValueWithKey(iter.Key(), value)
		if !isNopLogger {
			values.Append(value)
		}
	}
	log.GetMulti(tx.id, SLCServer, requestKeys, values)
	return nil
}

func (c *SecondLevelCache) setPrimaryKeysByUniqueKeys(tx *Tx, queryIter *QueryIterator) error {
	requestKeys := []server.CacheKey{}
	defer queryIter.Reset()
	for queryIter.Next() {
		uniqueKey := queryIter.Key()
		if _, exists := tx.stash.oldKey[uniqueKey.String()]; exists {
			// need lookup db
			queryIter.SetErrorWithKey(uniqueKey, server.ErrCacheMiss)
			continue
		}
		primaryKey, exists := tx.stash.uniqueKeyToPrimaryKey[uniqueKey.String()]
		if exists {
			queryIter.SetPrimaryKey(primaryKey)
		} else {
			requestKeys = append(requestKeys, uniqueKey)
		}
	}
	if len(requestKeys) == 0 {
		return nil
	}
	iter, err := c.cacheServer.GetMulti(requestKeys)
	if err != nil {
		return xerrors.Errorf("failed to get primary keys from server: %w", err)
	}
	var values []server.CacheKey
	if !isNopLogger {
		values = []server.CacheKey{}
	}
	for iter.Next() {
		if iter.Error() != nil {
			queryIter.SetErrorWithKey(iter.Key(), iter.Error())
			continue
		}
		content := iter.Content()
		primaryKey, err := c.decodePrimaryKey(content.Value, content.Flags)
		if err != nil {
			queryIter.SetErrorWithKey(iter.Key(), xerrors.Errorf("set error: %w", err))
		} else {
			if !isNopLogger {
				values = append(values, primaryKey)
			}
			key := iter.Key().String()
			tx.stash.uniqueKeyToPrimaryKey[key] = primaryKey
			tx.stash.casIDs[key] = content.CasID
			queryIter.SetPrimaryKeyWithKey(iter.Key(), primaryKey)
		}
	}
	log.GetMulti(tx.id, SLCServer, requestKeys, LogStrings(values))
	return nil
}

func (c *SecondLevelCache) setPrimaryKeysByKeys(tx *Tx, queryIter *QueryIterator) error {
	requestKeys := []server.CacheKey{}
	defer queryIter.Reset()
	for queryIter.Next() {
		key := queryIter.Key()
		if _, exists := tx.stash.oldKey[key.String()]; exists {
			// need lookup db
			queryIter.SetErrorWithKey(key, server.ErrCacheMiss)
			continue
		}
		primaryKeys, exists := tx.stash.keyToPrimaryKeys[key.String()]
		if exists {
			queryIter.SetPrimaryKeys(primaryKeys)
		} else {
			requestKeys = append(requestKeys, key)
		}
	}
	if len(requestKeys) == 0 {
		return nil
	}

	iter, err := c.cacheServer.GetMulti(requestKeys)
	if err != nil {
		return xerrors.Errorf("failed to get primary keys from server: %w", err)
	}
	values := []server.CacheKey{}
	for iter.Next() {
		if iter.Error() != nil {
			queryIter.SetErrorWithKey(iter.Key(), iter.Error())
			continue
		}
		content := iter.Content()
		primaryKeys, err := c.decodeMultiplePrimaryKeys(content.Value, content.Flags)
		if err != nil {
			queryIter.SetErrorWithKey(iter.Key(), xerrors.Errorf("set error: %w", err))
		} else {
			values = append(values, primaryKeys...)
			queryIter.SetPrimaryKeysWithKey(iter.Key(), primaryKeys)
			key := iter.Key().String()
			tx.stash.keyToPrimaryKeys[key] = primaryKeys
			tx.stash.casIDs[key] = content.CasID
		}
	}
	log.GetMulti(tx.id, SLCServer, requestKeys, LogStrings(values))
	return nil
}

func (c *SecondLevelCache) findValuesByCache(tx *Tx, builder *QueryBuilder, queries *Queries) (*StructSliceValue, error) {
	if builder.isIgnoreCache || builder.lockOpt != nil {
		queries.cacheMissQueries = queries.queries
		return NewStructSliceValue(), nil
	}
	values, err := queries.LoadValues(c.valueFactory, func(indexType IndexType, iter *QueryIterator) error {
		switch indexType {
		case IndexTypePrimaryKey:
			for iter.Next() {
				iter.SetPrimaryKey(iter.Key())
			}
		case IndexTypeUniqueKey:
			if err := c.setPrimaryKeysByUniqueKeys(tx, iter); err != nil {
				return xerrors.Errorf("failed to set primary keys by unique keys: %w", err)
			}
		case IndexTypeKey:
			if err := c.setPrimaryKeysByKeys(tx, iter); err != nil {
				return xerrors.Errorf("failed to set primary keys by keys: %w", err)
			}
		}
		return nil
	}, func(valueIter *ValueIterator) error {
		if err := c.findByPrimaryKeys(tx, valueIter); err != nil {
			return xerrors.Errorf("failed to find by primary keys: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to load values: %w", err)
	}
	return values, nil
}

func (c *SecondLevelCache) createCacheByCacheMissQueryMap(tx *Tx, cacheMissQueryMap map[*Query][]*StructValue) error {
	for cacheMissQuery, values := range cacheMissQueryMap {
		if len(values) == 0 {
			if err := c.createNegativeCacheByQuery(tx, cacheMissQuery); err != nil {
				return xerrors.Errorf("failed to create negative cache by query: %w", err)
			}
		} else if len(values) == 1 {
			if err := c.createByQueryWithValue(tx, cacheMissQuery, values[0]); err != nil {
				return xerrors.Errorf("failed to create cache by single value: %w", err)
			}
		} else {
			if err := c.createByQueryWithValues(tx, cacheMissQuery, values); err != nil {
				return xerrors.Errorf("failed to create cache by multiple values: %w", err)
			}
		}
	}
	return nil
}

func (c *SecondLevelCache) primaryKeyStringByStructValue(value *StructValue) string {
	primaryKeys := make([]string, len(c.primaryKey.Columns))
	for idx, column := range c.primaryKey.Columns {
		primaryKeys[idx] = value.ValueByColumn(column).String()
	}
	return strings.Join(primaryKeys, ":")
}

func (c *SecondLevelCache) findValuesByQueryBuilder(ctx context.Context, tx *Tx, builder *QueryBuilder) (ssv *StructSliceValue, e error) {
	if builder.IsUnsupportedCacheQuery() {
		foundValues, err := c.findValuesByQueryBuilderWithoutCache(ctx, tx, builder)
		if err != nil {
			return nil, xerrors.Errorf("failed to find values by query builder without cache: %w", err)
		}
		return foundValues, nil
	}

	queries, err := builder.BuildWithIndex(c.valueFactory, c.indexes, c.typ)
	if err != nil {
		return nil, xerrors.Errorf("failed to build query: %w", err)
	}
	if queries.Len() == 0 {
		return nil, nil
	}

	foundValues, err := c.findValuesByCache(tx, builder, queries)
	if err != nil {
		return nil, xerrors.Errorf("failed to find values by cache: %w", err)
	}
	query, values := queries.CacheMissQueriesToSQL(c.typ)
	if query == "" {
		return foundValues, nil
	}

	rows, err := tx.conn.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, xerrors.Errorf("failed sql %s %v: %w", query, values, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			e = xerrors.Errorf("failed to close rows: %w", err)
		}
	}()
	cacheMissQueryMap := map[*Query][]*StructValue{}
	for _, cacheMissQuery := range queries.CacheMissQueries() {
		cacheMissQueryMap[cacheMissQuery] = []*StructValue{}
	}
	var dbValues *StructSliceValue
	if !isNopLogger {
		dbValues = NewStructSliceValue()
	}
	alreadyFoundValues := map[string]struct{}{}
	for _, value := range foundValues.values {
		alreadyFoundValues[c.primaryKeyStringByStructValue(value)] = struct{}{}
	}
	for rows.Next() {
		scanValues := c.typ.ScanValues(c.valueFactory)
		if err := rows.Scan(scanValues...); err != nil {
			return nil, xerrors.Errorf("failed to scan: %w", err)
		}
		value := c.typ.StructValue(scanValues)

		pkStr := c.primaryKeyStringByStructValue(value)
		if _, exists := alreadyFoundValues[pkStr]; !exists {
			alreadyFoundValues[pkStr] = struct{}{}
			foundValues.Append(value)
			if !isNopLogger {
				dbValues.Append(value)
			}
		}
		cacheMissQuery := queries.FindCacheMissQueryByStructValue(value)
		if cacheMissQuery == nil {
			continue
		}
		cacheMissQueryMap[cacheMissQuery] = append(cacheMissQueryMap[cacheMissQuery], value)
	}

	log.GetFromDB(tx.id, query, values, dbValues)
	if builder.isIgnoreCache {
		return foundValues, nil
	}
	if err := c.createCacheByCacheMissQueryMap(tx, cacheMissQueryMap); err != nil {
		return nil, xerrors.Errorf("failed to create cache by cache miss query map: %w", err)
	}
	return foundValues, nil
}

func (c *SecondLevelCache) FindByQueryBuilder(ctx context.Context, tx *Tx, builder *QueryBuilder, unmarshaler Unmarshaler) error {
	defer builder.Release()
	foundValues, err := c.findValuesByQueryBuilder(ctx, tx, builder)
	if err != nil {
		return xerrors.Errorf("failed to find values by query builder: %w", err)
	}
	if foundValues != nil && foundValues.Len() > 0 {
		if err := unmarshaler.DecodeRapidash(foundValues); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) deleteCacheKeyByOldValue(tx *Tx, column string, value *StructValue) error {
	for _, index := range c.indexes {
		if !index.HasColumn(column) {
			continue
		}

		cacheKey, err := index.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		if err := c.deleteUniqueKeyOrOldKey(tx, cacheKey); err != nil {
			return xerrors.Errorf("failed to delete unique key or old key: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) updateOrDeleteCacheKeyByNewValue(tx *Tx, column string, value *StructValue) error {
	for _, index := range c.indexes {
		if index.Type == IndexTypePrimaryKey {
			continue
		}
		if !index.HasColumn(column) {
			continue
		}

		switch index.Type {
		case IndexTypeUniqueKey:
			primaryKey, err := c.primaryKey.CacheKey(value)
			if err != nil {
				return xerrors.Errorf("failed to get cache key: %w", err)
			}
			cacheKey, err := index.CacheKey(value)
			if err != nil {
				return xerrors.Errorf("failed to get cache key: %w", err)
			}
			if err := c.setUniqueKey(tx, cacheKey, primaryKey); err != nil {
				return xerrors.Errorf("failed to set unique key: %w", err)
			}
		case IndexTypeKey:
			cacheKey, err := index.CacheKey(value)
			if err != nil {
				return xerrors.Errorf("failed to get cache key: %w", err)
			}
			if err := c.deleteOldKey(tx, cacheKey); err != nil {
				return xerrors.Errorf("failed to delete old key: %w", err)
			}
		}
	}
	return nil
}

func (c *SecondLevelCache) updateValue(tx *Tx, target *StructValue, updateMap map[string]interface{}) error {
	for k, v := range updateMap {
		field, exists := target.fields[k]
		if !exists {
			return xerrors.Errorf("%s.%s is not found: %w", c.typ.tableName, k, ErrUnknownColumnName)
		}
		value := c.valueFactory.CreateValue(v)
		if value == nil {
			return xerrors.Errorf("%s.%s type is invalid: %w", c.typ.tableName, k, ErrInvalidColumnType)
		}
		if !field.IsNil && !value.IsNil && field.kind != value.kind {
			return xerrors.Errorf("%s.%s kind is %s but required %s: %w",
				c.typ.tableName, k, field.kind, value.kind, ErrInvalidColumnType)
		}
		if field.EQ(value) {
			continue
		}
		if _, exists := c.indexColumns[k]; !exists {
			target.fields[k] = value
			continue
		}

		// remove cache key by old unique key or old key
		if err := c.deleteCacheKeyByOldValue(tx, k, target); err != nil {
			return xerrors.Errorf("failed to delete cache key by value before updating")
		}

		target.fields[k] = value // update indexed value

		// remove cache key by new key
		if err := c.updateOrDeleteCacheKeyByNewValue(tx, k, target); err != nil {
			return xerrors.Errorf("failed to delete cache key by value after updating")
		}
	}
	return nil
}

func (c *SecondLevelCache) UpdateByQueryBuilder(ctx context.Context, tx *Tx, builder *QueryBuilder, updateMap map[string]interface{}) (e error) {
	defer builder.Release()
	var foundValues *StructSliceValue
	if builder.AvailableCache() {
		values, err := c.findValuesByQueryBuilder(ctx, tx, builder)
		if err != nil {
			return xerrors.Errorf("failed to find values by query builder: %w", err)
		}
		foundValues = values
	} else {
		sql, args := builder.SelectSQL(c.typ)
		rows, err := tx.conn.QueryContext(ctx, sql, args...)
		if err != nil {
			return xerrors.Errorf("failed sql %s %v: %w", sql, args, err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				e = xerrors.Errorf("failed to close rows: %w", err)
			}
		}()
		foundValues = NewStructSliceValue()
		for rows.Next() {
			scanValues := c.typ.ScanValues(c.valueFactory)
			if err := rows.Scan(scanValues...); err != nil {
				return xerrors.Errorf("failed to scan: %w", err)
			}
			value := c.typ.StructValue(scanValues)
			foundValues.Append(value)
			log.GetFromDB(tx.id, sql, "", value)
		}
	}
	sql, values := builder.UpdateSQL(updateMap)
	if _, err := tx.conn.ExecContext(ctx, sql, values...); err != nil {
		return xerrors.Errorf("failed update sql %s %v: %w", sql, values, err)
	}
	log.UpdateForDB(tx.id, sql, values, LogMap(updateMap))
	if builder.isIgnoreCache {
		return nil
	}
	queries, err := builder.BuildWithIndex(c.valueFactory, c.indexes, c.typ)
	if err != nil {
		return xerrors.Errorf("failed to build query: %w", err)
	}
	for idx, value := range foundValues.values {
		if err := c.updateValue(tx, value, updateMap); err != nil {
			return xerrors.Errorf("faield to update value: %w", err)
		}
		if builder.AvailableCache() {
			if err := c.updateByQueryWithValue(tx, queries.At(idx), value); err != nil {
				return xerrors.Errorf("failed to update by query with value: %w", err)
			}
		} else {
			if err := c.updateByValue(tx, value, updateMap); err != nil {
				return xerrors.Errorf("failed to update by value: %w", err)
			}
		}
	}
	return nil
}

func (c *SecondLevelCache) updateByValue(tx *Tx, value *StructValue, updateMap map[string]interface{}) error {
	for _, index := range c.indexes {
		builder := c.updateBuilderByValue(value, index, updateMap)
		if builder == nil {
			continue
		}
		queries, err := builder.BuildWithIndex(c.valueFactory, c.indexes, c.typ)
		if err != nil {
			return xerrors.Errorf("failed to build query: %w", err)
		}
		for i := 0; i < queries.Len(); i++ {
			if err := c.updateByQueryWithValue(tx, queries.At(i), value); err != nil {
				return xerrors.Errorf("failed to update by query with value: %w", err)
			}
		}
	}
	return nil
}

func (c *SecondLevelCache) updateByQueryWithValue(tx *Tx, query *Query, value *StructValue) error {
	cacheKey := query.cacheKey
	index := query.Index()
	switch index.Type {
	case IndexTypePrimaryKey:
		if err := c.updatePrimaryKey(tx, cacheKey, value); err != nil {
			return xerrors.Errorf("failed to update primary key", err)
		}
	case IndexTypeUniqueKey:
		primaryKey, err := c.primaryKey.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		newCacheKey, err := index.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		if cacheKey != newCacheKey {
			if err := c.setUniqueKey(tx, newCacheKey, primaryKey); err != nil {
				return xerrors.Errorf("failed to set unique key: %w", err)
			}
		}
		if err := c.updatePrimaryKey(tx, primaryKey, value); err != nil {
			return xerrors.Errorf("failed to update primary key: %w", err)
		}
	case IndexTypeKey:
		primaryKey, err := c.primaryKey.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		newCacheKey, err := index.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		if cacheKey != newCacheKey {
			if err := c.deleteOldKey(tx, newCacheKey); err != nil {
				return xerrors.Errorf("failed to delete old key: %w", err)
			}
		}
		if err := c.updatePrimaryKey(tx, primaryKey, value); err != nil {
			return xerrors.Errorf("failed to update primary key: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) createNegativeCacheByQuery(tx *Tx, query *Query) error {
	cacheKey := query.cacheKey
	switch query.Index().Type {
	case IndexTypePrimaryKey:
		if err := c.setPrimaryKey(tx, cacheKey, nil); err != nil {
			return xerrors.Errorf("failed to set primary key: %w", err)
		}
	case IndexTypeUniqueKey:
		if err := c.setUniqueKey(tx, cacheKey, nil); err != nil {
			return xerrors.Errorf("failed to set unique key: %w", err)
		}
	case IndexTypeKey:
		if err := c.setKey(tx, cacheKey, []server.CacheKey{}); err != nil {
			return xerrors.Errorf("failed to set key: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) createByQueryWithValue(tx *Tx, query *Query, value *StructValue) error {
	cacheKey := query.cacheKey
	index := query.Index()
	switch index.Type {
	case IndexTypePrimaryKey:
		if err := c.setPrimaryKey(tx, cacheKey, value); err != nil {
			return xerrors.Errorf("failed to set primary key: %w", err)
		}
	case IndexTypeUniqueKey:
		primaryKey, err := c.primaryKey.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		if err := c.setUniqueKey(tx, cacheKey, primaryKey); err != nil {
			return xerrors.Errorf("failed to set unique key: %w", err)
		}
		if err := c.setPrimaryKey(tx, primaryKey, value); err != nil {
			return xerrors.Errorf("failed to set primary key: %w", err)
		}
	case IndexTypeKey:
		primaryKey, err := c.primaryKey.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		if err := c.setKey(tx, cacheKey, []server.CacheKey{primaryKey}); err != nil {
			return xerrors.Errorf("failed to set key: %w", err)
		}
		if err := c.setPrimaryKey(tx, primaryKey, value); err != nil {
			return xerrors.Errorf("failed to set primary key: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) createByQueryWithValues(tx *Tx, query *Query, values []*StructValue) error {
	cacheKey := query.cacheKey
	index := query.Index()
	switch index.Type {
	case IndexTypePrimaryKey:
		return ErrCreatePrimaryKeyCacheBySlice
	case IndexTypeUniqueKey:
		return ErrCreateUniqueKeyCacheBySlice
	case IndexTypeKey:
		primaryKeys := []server.CacheKey{}
		for _, value := range values {
			primaryKey, err := c.primaryKey.CacheKey(value)
			if err != nil {
				return xerrors.Errorf("failed to get cache key: %w", err)
			}
			primaryKeys = append(primaryKeys, primaryKey)
		}
		if err := c.setKey(tx, cacheKey, primaryKeys); err != nil {
			return xerrors.Errorf("failed to set key: %w", err)
		}
		for idx, primaryKey := range primaryKeys {
			if err := c.setPrimaryKey(tx, primaryKey, values[idx]); err != nil {
				return xerrors.Errorf("failed to set primary key: %w", err)
			}
		}
	}
	return nil
}

func (c *SecondLevelCache) insertSQL(value *StructValue) (string, []interface{}) {
	escapedColumns := []string{}
	placeholders := []string{}
	values := []interface{}{}
	for idx, column := range value.typ.Columns() {
		escapedColumns = append(escapedColumns, c.adapter.Quote(column))
		placeholders = append(placeholders, c.adapter.Placeholder(idx+1))
		if value.fields[column] == nil {
			values = append(values, nil)
		} else {
			values = append(values, value.fields[column].RawValue())
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		c.adapter.Quote(c.typ.tableName),
		strings.Join(escapedColumns, ","),
		strings.Join(placeholders, ","),
	), values
}

func (c *SecondLevelCache) Create(ctx context.Context, tx *Tx, marshaler Marshaler) (id int64, e error) {
	_, value, err := c.encode(marshaler)
	if err != nil {
		e = xerrors.Errorf("failed to encode: %w", err)
		return
	}
	defer value.Release()
	sql, values := c.insertSQL(value)
	result, err := tx.conn.ExecContext(ctx, sql, values...)
	if err != nil {
		e = xerrors.Errorf("failed sql %s %v: %w", sql, values, err)
		return
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		e = xerrors.Errorf("failed to get last_insert_id(): %w", err)
		return
	}
	id = lastInsertID
	for _, column := range c.primaryKey.Columns {
		if value.fields[column] == nil {
			// if value for primary key is not defined,
			// rapidash assume that result.LastInsertId() can use alternatively.
			value.fields[column] = c.valueFactory.CreateInt64Value(lastInsertID)
		}
	}
	log.InsertIntoDB(tx.id, sql, values, value)
	if err := c.deleteKeyByValue(tx, value); err != nil {
		e = xerrors.Errorf("failed to delete key by value: %w", err)
		return
	}
	return id, nil
}

func (c *SecondLevelCache) CreateWithoutCache(ctx context.Context, tx *Tx, marshaler Marshaler) (id int64, e error) {
	_, value, err := c.encode(marshaler)
	if err != nil {
		e = xerrors.Errorf("failed to encode: %w", err)
		return
	}
	defer value.Release()
	sql, values := c.insertSQL(value)
	result, err := tx.conn.ExecContext(ctx, sql, values...)
	if err != nil {
		e = xerrors.Errorf("failed sql %s %v: %w", sql, values, err)
		return
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		e = xerrors.Errorf("failed to get last_insert_id(): %w", err)
		return
	}
	id = lastInsertID
	for _, column := range c.primaryKey.Columns {
		if value.fields[column] == nil {
			// if value for primary key is not defined,
			// rapidash assume that result.LastInsertId() can use alternatively.
			value.fields[column] = c.valueFactory.CreateInt64Value(lastInsertID)
		}
	}
	log.InsertIntoDB(tx.id, sql, values, value)
	return id, nil
}

func (c *SecondLevelCache) deleteKeyByQueryBuilder(tx *Tx, builder *QueryBuilder) error {
	queries, err := builder.BuildWithIndex(c.valueFactory, c.indexes, c.typ)
	if err != nil {
		return xerrors.Errorf("failed to build query: %w", err)
	}
	if _, err := queries.LoadValues(c.valueFactory, func(indexType IndexType, iter *QueryIterator) error {
		switch indexType {
		case IndexTypePrimaryKey:
			for iter.Next() {
				if err := c.deleteOldKey(tx, iter.Key()); err != nil {
					return xerrors.Errorf("failed to delete old key: %w", err)
				}
			}
		case IndexTypeUniqueKey:
			for iter.Next() {
				if err := c.deleteOldKey(tx, iter.Key()); err != nil {
					return xerrors.Errorf("failed to delete old key: %w", err)
				}
			}
		case IndexTypeKey:
			for iter.Next() {
				if err := c.deleteOldKey(tx, iter.Key()); err != nil {
					return xerrors.Errorf("failed to delete old key: %w", err)
				}
			}
		}
		return nil
	}, func(valueIter *ValueIterator) error {
		return nil
	}); err != nil {
		return xerrors.Errorf("failed to delete values: %w", err)
	}
	return nil
}

func (c *SecondLevelCache) isUsedPrimaryKeyBuilder(queries *Queries) bool {
	primaryKeyColumns := c.primaryKey.Columns
	for i := 0; i < queries.Len(); i++ {
		for j, column := range queries.At(i).columns {
			if column != primaryKeyColumns[j] {
				return false
			}
		}
	}
	return true
}

func (c *SecondLevelCache) deleteCacheFromSQL(ctx context.Context, tx *Tx, builder *QueryBuilder) (e error) {
	sql, args := builder.SelectSQL(c.typ)
	rows, err := tx.conn.QueryContext(ctx, sql, args...)
	if err != nil {
		return xerrors.Errorf("failed sql %s %v: %w", sql, args, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			e = xerrors.Errorf("failed to close rows: %w", err)
		}
	}()
	for rows.Next() {
		scanValues := c.typ.ScanValues(c.valueFactory)
		if err := rows.Scan(scanValues...); err != nil {
			return xerrors.Errorf("failed to scan: %w", err)
		}
		value := c.typ.StructValue(scanValues)
		primaryKey, err := c.primaryKey.CacheKey(value)
		if err != nil {
			return xerrors.Errorf("failed to get cache key: %w", err)
		}
		if err := c.deletePrimaryKey(tx, primaryKey); err != nil {
			return xerrors.Errorf("failed to delete primary key: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) DeleteByQueryBuilder(ctx context.Context, tx *Tx, builder *QueryBuilder) error {
	defer builder.Release()
	if !builder.AvailableCache() {
		if !builder.isIgnoreCache {
			if err := c.deleteCacheFromSQL(ctx, tx, builder); err != nil {
				return xerrors.Errorf("failed to delete cache by SQL: %w", err)
			}
		}
		sql, args := builder.DeleteSQL()
		if _, err := tx.conn.ExecContext(ctx, sql, args...); err != nil {
			return xerrors.Errorf("failed sql %s %v: %w", sql, args, err)
		}
		log.DeleteFromDB(tx.id, sql)
		return nil
	}
	queries, err := builder.BuildWithIndex(c.valueFactory, c.indexes, c.typ)
	if err != nil {
		return xerrors.Errorf("failed to build query: %w", err)
	}
	if !c.isUsedPrimaryKeyBuilder(queries) {
		if err := c.deleteCacheFromSQL(ctx, tx, builder); err != nil {
			return xerrors.Errorf("failed to delete cache by SQL: %w", err)
		}
	} else {
		for i := 0; i < queries.Len(); i++ {
			cacheKey := queries.At(i).cacheKey
			if err := c.deletePrimaryKey(tx, cacheKey); err != nil {
				return xerrors.Errorf("failed to delete primary key: %w", err)
			}
		}
	}
	sql, args := builder.DeleteSQL()
	if _, err := tx.conn.ExecContext(ctx, sql, args...); err != nil {
		return xerrors.Errorf("failed sql %s %v: %w", sql, args, err)
	}
	log.DeleteFromDB(tx.id, sql)
	return nil
}

func (c *SecondLevelCache) builderByValue(value *StructValue, index *Index) *QueryBuilder {
	builder := NewQueryBuilder(c.typ.tableName, c.adapter)
	for _, column := range index.Columns {
		if value.fields[column] == nil {
			return nil
		}
		builder.Eq(column, value.fields[column].RawValue())
	}
	return builder
}

func (c *SecondLevelCache) updateBuilderByValue(value *StructValue, index *Index, updateMap map[string]interface{}) *QueryBuilder {
	switch index.Type {
	case IndexTypePrimaryKey:
		builder := NewQueryBuilder(c.typ.tableName, c.adapter)
		for _, column := range index.Columns {
			builder.Eq(column, value.fields[column].RawValue())
		}
		return builder
	case IndexTypeKey, IndexTypeUniqueKey:
		builder := NewQueryBuilder(c.typ.tableName, c.adapter)
		for _, column := range index.Columns {
			if _, exists := updateMap[column]; !exists {
				return nil
			}
			builder.Eq(column, value.fields[column].RawValue())
		}
		return builder
	}
	return nil
}

func (c *SecondLevelCache) deleteKeyByValue(tx *Tx, value *StructValue) error {
	for _, index := range c.indexes {
		builder := c.builderByValue(value, index)
		if builder == nil {
			continue
		}
		if err := c.deleteKeyByQueryBuilder(tx, builder); err != nil {
			return xerrors.Errorf("failed to delete key by query builder: %w", err)
		}
	}
	return nil
}

func (c *SecondLevelCache) findValuesByQueryBuilderWithoutCache(ctx context.Context, tx *Tx, builder *QueryBuilder) (ssv *StructSliceValue, e error) {
	sql, args := builder.SelectSQL(c.typ)
	rows, err := tx.conn.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, xerrors.Errorf("failed sql %s %v: %w", sql, args, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			e = xerrors.Errorf("failed to close rows: %w", err)
		}
	}()
	foundValues := NewStructSliceValue()
	for rows.Next() {
		scanValues := c.typ.ScanValues(c.valueFactory)
		if err := rows.Scan(scanValues...); err != nil {
			return nil, xerrors.Errorf("failed to scan: %w", err)
		}
		value := c.typ.StructValue(scanValues)
		foundValues.Append(value)
		log.GetFromDB(tx.id, sql, "", value)
	}
	return foundValues, nil
}

func (c *SecondLevelCache) CountByQueryBuilder(ctx context.Context, tx *Tx, builder *QueryBuilder) (uint64, error) {
	defer builder.Release()
	values, err := c.findValuesByQueryBuilder(ctx, tx, builder)
	if err != nil {
		return 0, xerrors.Errorf("failed to count by query builder: %w", err)
	}
	if values == nil {
		return 0, nil
	}
	return uint64(values.Len()), nil
}
