package rapidash

import (
	"context"
	"database/sql"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/rs/xid"
	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

type Coder interface {
	Marshaler
	Unmarshaler
}

type Marshaler interface {
	EncodeRapidash(Encoder) error
}

type Unmarshaler interface {
	DecodeRapidash(Decoder) error
}

type Rapidash struct {
	cacheServer       server.CacheServer
	ignoreCaches      map[string]struct{}
	firstLevelCaches  *FirstLevelCacheMap
	secondLevelCaches *SecondLevelCacheMap
	lastLevelCache    *LastLevelCache
	opt               Option
}

type Selectors struct {
	slcSelector *server.Selector
	llcSelector *server.Selector
}

type CacheServerType int

const (
	CacheServerTypeMemcached CacheServerType = iota
	CacheServerTypeRedis
	CacheServerTypeOnMemory

	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond
	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

type LogModeType int

const (
	LogModeConsole LogModeType = iota
	LogModeJSON
	LogModeServerDebug
)

type TableOption struct {
	shardKey        *string
	server          *string
	expiration      *time.Duration
	lockExpiration  *time.Duration
	optimisticLock  *bool
	pessimisticLock *bool
}

func (o *TableOption) ShardKey() string {
	if o.shardKey == nil {
		return ""
	}
	return *o.shardKey
}

func (o *TableOption) Server() string {
	if o.server == nil {
		return ""
	}
	return *o.server
}

func (o *TableOption) Expiration() time.Duration {
	if o.expiration == nil {
		return 0
	}
	return *o.expiration
}

func (o *TableOption) LockExpiration() time.Duration {
	if o.lockExpiration == nil {
		return 0
	}
	return *o.lockExpiration
}

func (o *TableOption) OptimisticLock() bool {
	if o.optimisticLock == nil {
		return false
	}
	return *o.optimisticLock
}

func (o *TableOption) PessimisticLock() bool {
	if o.pessimisticLock == nil {
		return false
	}
	return *o.pessimisticLock
}

type LastLevelCacheOption struct {
	lockExpiration  time.Duration
	expiration      time.Duration
	optimisticLock  bool
	pessimisticLock bool
	tagOpt          map[string]TagOption
}

type TagOption struct {
	server          string
	expiration      time.Duration
	lockExpiration  time.Duration
	ignoreStash     bool
	optimisticLock  bool
	pessimisticLock bool
}

type QueryLog struct {
	Command string              `json:"command"`
	Key     string              `json:"key"`
	Hash    uint32              `json:"hash"`
	Type    server.CacheKeyType `json:"type"`
	Addr    string              `json:"addr"`
}

type Option struct {
	serverType                 CacheServerType
	serverAddrs                []string
	timeout                    time.Duration
	maxIdleConnections         int
	maxRetryCount              int
	retryInterval              time.Duration
	logMode                    LogModeType
	logEnabled                 bool
	logServerAddr              string
	slcServerAddrs             []string
	slcLockExpiration          time.Duration
	slcExpiration              time.Duration
	slcOptimisticLock          bool
	slcPessimisticLock         bool
	slcIgnoreNewerCache        bool
	slcTableOpt                map[string]TableOption
	llcOpt                     *LastLevelCacheOption
	llcServerAddrs             []string
	beforeCommitCallback       func(*Tx, []*QueryLog) error
	afterCommitSuccessCallback func(*Tx) error
	afterCommitFailureCallback func(*Tx, []*QueryLog) error
}

func defaultOption() Option {
	return Option{
		serverType:          CacheServerTypeMemcached,
		timeout:             DefaultTimeout,
		maxIdleConnections:  DefaultMaxIdleConns,
		maxRetryCount:       3,
		retryInterval:       30 * time.Millisecond,
		logMode:             LogModeConsole,
		logEnabled:          false,
		slcLockExpiration:   0,
		slcExpiration:       0,
		slcOptimisticLock:   true,
		slcPessimisticLock:  true,
		slcIgnoreNewerCache: true,
		slcTableOpt:         map[string]TableOption{},
		llcOpt: &LastLevelCacheOption{
			tagOpt:          map[string]TagOption{},
			optimisticLock:  true,
			pessimisticLock: true,
		},
	}
}

type Connection interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type TxConnection interface {
	Connection
	Commit() error
	Rollback() error
}

type PendingQuery struct {
	*QueryLog
	fn func() error
}

type Tx struct {
	r                          *Rapidash
	conn                       Connection
	stash                      *Stash
	id                         string
	pendingQueries             map[string]*PendingQuery
	lockKeys                   []server.CacheKey
	isDBCommitted              bool
	isCacheCommitted           bool
	beforeCommitCallback       func([]*QueryLog) error
	afterCommitSuccessCallback func() error
	afterCommitFailureCallback func([]*QueryLog) error
}

type Stash struct {
	oldKey                   map[string]struct{}
	uniqueKeyToPrimaryKey    map[string]server.CacheKey
	keyToPrimaryKeys         map[string][]server.CacheKey
	primaryKeyToValue        map[string]*StructValue
	lastLevelCacheKeyToBytes map[string][]byte
	casIDs                   map[string]uint64
}

func NewStash() *Stash {
	return &Stash{
		oldKey:                   map[string]struct{}{},
		uniqueKeyToPrimaryKey:    map[string]server.CacheKey{},
		keyToPrimaryKeys:         map[string][]server.CacheKey{},
		primaryKeyToValue:        map[string]*StructValue{},
		lastLevelCacheKeyToBytes: map[string][]byte{},
		casIDs:                   map[string]uint64{},
	}
}

func (r *Rapidash) Begin(conns ...Connection) (*Tx, error) {
	if len(conns) > 1 {
		return nil, ErrBeginTransaction
	}
	var conn Connection
	if len(conns) == 1 {
		conn = conns[0]
	}
	return &Tx{
		r:              r,
		conn:           conn,
		stash:          NewStash(),
		id:             xid.New().String(),
		pendingQueries: map[string]*PendingQuery{},
		lockKeys:       []server.CacheKey{},
	}, nil
}

func (tx *Tx) ID() string {
	return tx.id
}

func (tx *Tx) BeforeCommitCallback(callback func([]*QueryLog) error) {
	tx.beforeCommitCallback = callback
}

func (tx *Tx) AfterCommitCallback(
	successCallback func() error,
	failureCallback func([]*QueryLog) error) {
	tx.afterCommitSuccessCallback = successCallback
	tx.afterCommitFailureCallback = failureCallback
}

func (tx *Tx) Create(key string, value Type) error {
	if err := tx.CreateWithExpiration(key, value, 0); err != nil {
		return xerrors.Errorf("failed to CreateWithExpiration: %w", err)
	}
	return nil
}

func (tx *Tx) CreateWithTag(tag, key string, value Type) error {
	if err := tx.CreateWithTagAndExpiration(tag, key, value, 0); err != nil {
		return xerrors.Errorf("failed to CreateWithTagAndExpiration: %w", err)
	}
	return nil
}

func (tx *Tx) CreateWithExpiration(key string, value Type, expiration time.Duration) error {
	if err := tx.CreateWithTagAndExpiration("", key, value, expiration); err != nil {
		return xerrors.Errorf("failed to CreateWithTagAndExpiration: %w", err)
	}
	return nil
}

func (tx *Tx) CreateWithTagAndExpiration(tag, key string, value Type, expiration time.Duration) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	if err := tx.r.lastLevelCache.Create(tx, tag, key, value, expiration); err != nil {
		return xerrors.Errorf("failed to Create: %w", err)
	}
	return nil
}

func (tx *Tx) Find(key string, value Type) error {
	if err := tx.FindWithTag("", key, value); err != nil {
		return xerrors.Errorf("failed to FindWithTag: %w", err)
	}
	return nil
}

func (tx *Tx) FindWithTag(tag, key string, value Type) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	if err := tx.r.lastLevelCache.Find(tx, tag, key, value); err != nil {
		return xerrors.Errorf("failed to Find: %w", err)
	}
	return nil
}

func (tx *Tx) Update(key string, value Type) error {
	if err := tx.UpdateWithExpiration(key, value, 0); err != nil {
		return xerrors.Errorf("failed to UpdateWithExpiration: %w", err)
	}
	return nil
}

func (tx *Tx) UpdateWithTag(tag, key string, value Type) error {
	if err := tx.UpdateWithTagAndExpiration(tag, key, value, 0); err != nil {
		return xerrors.Errorf("failed to UpdateWithTag: %w", err)
	}
	return nil
}

func (tx *Tx) UpdateWithExpiration(key string, value Type, expiration time.Duration) error {
	if err := tx.UpdateWithTagAndExpiration("", key, value, expiration); err != nil {
		return xerrors.Errorf("failed to UpdateWithTagAndExpiration: %w", err)
	}
	return nil
}

func (tx *Tx) UpdateWithTagAndExpiration(tag, key string, value Type, expiration time.Duration) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	if err := tx.r.lastLevelCache.Update(tx, tag, key, value, expiration); err != nil {
		return xerrors.Errorf("failed to Update: %w", err)
	}
	return nil
}

func (tx *Tx) Delete(key string) error {
	if err := tx.DeleteWithTag("", key); err != nil {
		return xerrors.Errorf("failed to DeleteWithTag: %w", err)
	}
	return nil
}

func (tx *Tx) DeleteWithTag(tag, key string) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	if err := tx.r.lastLevelCache.Delete(tx, tag, key); err != nil {
		return xerrors.Errorf("failed to Delete: %w", err)
	}
	return nil
}

func (tx *Tx) enabledIgnoreCacheIfExistsTable(builder *QueryBuilder) {
	if _, exists := tx.r.ignoreCaches[builder.tableName]; exists {
		builder.isIgnoreCache = true
	}
}

func (tx *Tx) CreateByTable(tableName string, marshaler Marshaler) (int64, error) {
	id, err := tx.CreateByTableContext(context.Background(), tableName, marshaler)
	if err != nil {
		return id, xerrors.Errorf("failed to CreateByTableContext: %w", err)
	}
	return id, nil
}

func (tx *Tx) CreateByTableContext(ctx context.Context, tableName string, marshaler Marshaler) (id int64, e error) {
	if tx.IsCommitted() {
		e = ErrAlreadyCommittedTransaction
		return
	}
	if _, exists := tx.r.firstLevelCaches.get(tableName); exists {
		e = xerrors.Errorf("%s is read only table. it doesn't support write query", tableName)
		return
	}
	if c, exists := tx.r.secondLevelCaches.get(tableName); exists {
		if _, exists := tx.r.ignoreCaches[tableName]; exists {
			lastInsertID, err := c.CreateWithoutCache(ctx, tx, marshaler)
			if err != nil {
				e = xerrors.Errorf("failed to CreateWithoutCache: %w", err)
			}
			id = lastInsertID
			return
		}
		lastInsertID, err := c.Create(ctx, tx, marshaler)
		if err != nil {
			e = xerrors.Errorf("failed to Create: %w", err)
			return
		}
		id = lastInsertID
		return
	}
	e = xerrors.Errorf("unknown table name %s", tableName)
	return
}

func (tx *Tx) FindByQueryBuilder(builder *QueryBuilder, unmarshaler Unmarshaler) error {
	if err := tx.FindByQueryBuilderContext(context.Background(), builder, unmarshaler); err != nil {
		return xerrors.Errorf("failed to FindByQueryBuilderContext: %w", err)
	}
	return nil
}

func (tx *Tx) FindByQueryBuilderContext(ctx context.Context, builder *QueryBuilder, unmarshaler Unmarshaler) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	tx.enabledIgnoreCacheIfExistsTable(builder)
	if c, exists := tx.r.firstLevelCaches.get(builder.tableName); exists {
		if err := c.FindByQueryBuilder(builder, unmarshaler); err != nil {
			return xerrors.Errorf("failed to FindByQueryBuilder of FirstLevelCache: %w", err)
		}
		return nil
	}
	if c, exists := tx.r.secondLevelCaches.get(builder.tableName); exists {
		if tx.conn == nil {
			return ErrConnectionOfTransaction
		}
		if err := c.FindByQueryBuilder(ctx, tx, builder, unmarshaler); err != nil {
			return xerrors.Errorf("failed to FindByQueryBuilder of SecondLevelCache: %w", err)
		}
		return nil
	}
	return xerrors.Errorf("unknown table name %s", builder.tableName)
}

func (tx *Tx) CountByQueryBuilder(builder *QueryBuilder) (uint64, error) {
	count, err := tx.CountByQueryBuilderContext(context.Background(), builder)
	if err != nil {
		return 0, xerrors.Errorf("failed to CountByQueryBuilderContext: %w", err)
	}
	return count, nil
}

func (tx *Tx) CountByQueryBuilderContext(ctx context.Context, builder *QueryBuilder) (uint64, error) {
	if c, exists := tx.r.firstLevelCaches.get(builder.tableName); exists {
		count, err := c.CountByQueryBuilder(builder)
		if err != nil {
			return 0, xerrors.Errorf("failed to CountByQueryBuilder of FirstLevelCache: %w", err)
		}
		return count, nil
	}
	if c, exists := tx.r.secondLevelCaches.get(builder.tableName); exists {
		count, err := c.CountByQueryBuilder(ctx, tx, builder)
		if err != nil {
			return 0, xerrors.Errorf("failed to CountByQueryBuilder of SecondLevelCache: %w", err)
		}
		return count, nil
	}
	return 0, xerrors.Errorf("unknown table name %s", builder.tableName)
}

func (tx *Tx) FindAllByTable(tableName string, unmarshaler Unmarshaler) error {
	if c, exists := tx.r.firstLevelCaches.get(tableName); exists {
		if err := c.FindAll(unmarshaler); err != nil {
			return xerrors.Errorf("failed to FindAll of FirstLevelCache: %w", err)
		}
		return nil
	}
	return xerrors.Errorf("unknown table name %s", tableName)
}

func (tx *Tx) UpdateByQueryBuilder(builder *QueryBuilder, updateMap map[string]interface{}) error {
	if err := tx.UpdateByQueryBuilderContext(context.Background(), builder, updateMap); err != nil {
		return xerrors.Errorf("failed to UpdateByQueryBuilderContext: %w", err)
	}
	return nil
}

func (tx *Tx) UpdateByQueryBuilderContext(ctx context.Context, builder *QueryBuilder, updateMap map[string]interface{}) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	tx.enabledIgnoreCacheIfExistsTable(builder)
	if _, exists := tx.r.firstLevelCaches.get(builder.tableName); exists {
		return xerrors.Errorf("%s is read only table. it doesn't support write query", builder.tableName)
	}
	if c, exists := tx.r.secondLevelCaches.get(builder.tableName); exists {
		if tx.conn == nil {
			return ErrConnectionOfTransaction
		}
		if err := c.UpdateByQueryBuilder(ctx, tx, builder, updateMap); err != nil {
			return xerrors.Errorf("failed to UpdateByQueryBuilder: %w", err)
		}
		return nil
	}
	return xerrors.Errorf("unknown table name %s", builder.tableName)
}

func (tx *Tx) DeleteByQueryBuilder(builder *QueryBuilder) error {
	if err := tx.DeleteByQueryBuilderContext(context.Background(), builder); err != nil {
		return xerrors.Errorf("failed to DeleteByQueryBuilderContext: %w", err)
	}
	return nil
}

func (tx *Tx) DeleteByQueryBuilderContext(ctx context.Context, builder *QueryBuilder) error {
	if tx.IsCommitted() {
		return ErrAlreadyCommittedTransaction
	}
	tx.enabledIgnoreCacheIfExistsTable(builder)
	if _, exists := tx.r.firstLevelCaches.get(builder.tableName); exists {
		return xerrors.Errorf("%s is read only table. it doesn't support write query", builder.tableName)
	}
	if c, exists := tx.r.secondLevelCaches.get(builder.tableName); exists {
		if tx.conn == nil {
			return ErrConnectionOfTransaction
		}
		if err := c.DeleteByQueryBuilder(ctx, tx, builder); err != nil {
			return xerrors.Errorf("failed to DeleteByQueryBuilder: %w", err)
		}
		return nil
	}
	return xerrors.Errorf("unknown table name %s", builder.tableName)
}

func (tx *Tx) IsCommitted() bool {
	return tx.isDBCommitted || tx.isCacheCommitted
}

func (tx *Tx) execQuery(queries []*PendingQuery) []*PendingQuery {
	failedQueries := []*PendingQuery{}
	for _, query := range queries {
		if err := query.fn(); err != nil {
			failedQueries = append(failedQueries, query)
		}
	}
	return failedQueries
}

func (tx *Tx) sortedPendingQueryKeys() []string {
	keys := make([]string, len(tx.pendingQueries))
	idx := 0
	for k := range tx.pendingQueries {
		keys[idx] = k
		idx++
	}
	sort.Strings(keys)
	return keys
}

func (tx *Tx) unlockAllKeys() error {
	mergedErr := []string{}
	for _, key := range tx.lockKeys {
		log.Delete(tx.id, SLCServer, key)
		if err := tx.r.cacheServer.Delete(key); err != nil {
			mergedErr = append(mergedErr, err.Error())
		}
	}
	if len(mergedErr) > 0 {
		return xerrors.Errorf("%s: %w", strings.Join(mergedErr, ","), ErrUnlockCacheKeys)
	}
	return nil
}

func (tx *Tx) releaseValues() {
	for _, value := range tx.stash.primaryKeyToValue {
		value.Release()
	}
	tx.stash.primaryKeyToValue = make(map[string]*StructValue)
}

func (tx *Tx) commitBeforeProcess(queries []*PendingQuery) error {
	if tx.beforeCommitCallback != nil {
		totalQueries := make([]*QueryLog, len(queries))
		for idx, query := range queries {
			totalQueries[idx] = query.QueryLog
		}
		if err := tx.beforeCommitCallback(totalQueries); err != nil {
			return xerrors.Errorf("failed to callback for BeforeCommit: %w", err)
		}
	} else if tx.r.opt.beforeCommitCallback != nil {
		totalQueries := make([]*QueryLog, len(queries))
		for idx, query := range queries {
			totalQueries[idx] = query.QueryLog
		}
		if err := tx.r.opt.beforeCommitCallback(tx, totalQueries); err != nil {
			return xerrors.Errorf("failed to callback for BeforeCommit: %w", err)
		}
	}
	return nil
}

func (tx *Tx) commitAfterProcess(queries []*PendingQuery) error {
	tx.isCacheCommitted = true
	errs := []string{}
	if err := tx.unlockAllKeys(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(queries) == 0 {
		if tx.afterCommitSuccessCallback != nil {
			if err := tx.afterCommitSuccessCallback(); err != nil {
				errs = append(errs, err.Error())
			}
		} else if tx.r.opt.afterCommitSuccessCallback != nil {
			if err := tx.r.opt.afterCommitSuccessCallback(tx); err != nil {
				errs = append(errs, err.Error())
			}
		}
	} else {
		if tx.afterCommitFailureCallback != nil {
			failureQueries := []*QueryLog{}
			for _, query := range queries {
				failureQueries = append(failureQueries, query.QueryLog)
			}
			if err := tx.afterCommitFailureCallback(failureQueries); err != nil {
				errs = append(errs, err.Error())
			}
		} else if tx.r.opt.afterCommitFailureCallback != nil {
			failureQueries := []*QueryLog{}
			for _, query := range queries {
				failureQueries = append(failureQueries, query.QueryLog)
			}
			if err := tx.r.opt.afterCommitFailureCallback(tx, failureQueries); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("%s: %w", strings.Join(errs, ","), ErrCleanUpCache)
	}
	return nil
}

func (tx *Tx) commitCache() (e error) {
	queries := []*PendingQuery{}
	tx.releaseValues()
	defer func() {
		if err := tx.commitAfterProcess(queries); err != nil {
			e = xerrors.Errorf("failed to run commit after process: %w", err)
		}
	}()
	keys := tx.sortedPendingQueryKeys()
	for _, key := range keys {
		queries = append(queries, tx.pendingQueries[key])
	}
	if err := tx.commitBeforeProcess(queries); err != nil {
		return xerrors.Errorf("failed to run commit before process: %w", err)
	}
	for i := 0; i < tx.r.opt.maxRetryCount-1; i++ {
		queries = tx.execQuery(queries)
		if len(queries) == 0 {
			return nil
		}
		time.Sleep(tx.r.opt.retryInterval)
	}
	errs := []string{}
	for _, query := range queries {
		if err := query.fn(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("%s: %w", strings.Join(errs, ","), ErrCacheCommit)
	}
	return nil
}

func (tx *Tx) commitDB() error {
	if tx.conn == nil {
		return nil
	}
	txConn, ok := tx.conn.(TxConnection)
	if !ok {
		return nil
	}
	if err := txConn.Commit(); err != nil {
		return xerrors.Errorf("failed to Commit for database: %w", err)
	}
	tx.isDBCommitted = true
	return nil
}

func (tx *Tx) CommitCacheOnly() error {
	if err := tx.commitCache(); err != nil {
		return xerrors.Errorf("failed to Commit for cache: %w", err)
	}
	return nil
}

func (tx *Tx) CommitDBOnly() error {
	if err := tx.commitDB(); err != nil {
		return xerrors.Errorf("failed to Commit for database: %w", err)
	}
	return nil
}

func (tx *Tx) Commit() error {
	if err := tx.commitDB(); err != nil {
		return xerrors.Errorf("failed to Commit for database: %w", err)
	}
	if err := tx.commitCache(); err != nil {
		return xerrors.Errorf("failed to Commit for cache: %w", err)
	}
	return nil
}

func (tx *Tx) rollbackCache() error {
	tx.releaseValues()
	if err := tx.unlockAllKeys(); err != nil {
		return xerrors.Errorf("failed to unlock for all keys: %w", err)
	}
	return nil
}

func (tx *Tx) rollbackDB() error {
	if tx.conn == nil {
		return nil
	}
	txConn, ok := tx.conn.(TxConnection)
	if !ok {
		return nil
	}
	if err := txConn.Rollback(); err != nil {
		return xerrors.Errorf("failed to Rollback for database: %w", err)
	}
	return nil
}

func (tx *Tx) RollbackCacheOnly() error {
	if err := tx.rollbackCache(); err != nil {
		return xerrors.Errorf("failed to Rollback for cache: %w", err)
	}
	return nil
}

func (tx *Tx) RollbackDBOnly() error {
	if err := tx.rollbackDB(); err != nil {
		return xerrors.Errorf("failed to Rollback for database: %w", err)
	}
	return nil
}

func (tx *Tx) Rollback() error {
	if err := tx.rollbackDB(); err != nil {
		return xerrors.Errorf("failed to Rollback for database: %w", err)
	}
	if err := tx.rollbackCache(); err != nil {
		return xerrors.Errorf("failed to Rollback for cache: %w", err)
	}
	return nil
}

func (tx *Tx) RollbackCacheOnlyUnlessCommitted() error {
	if !tx.isCacheCommitted {
		if err := tx.rollbackCache(); err != nil {
			return xerrors.Errorf("failed to rollback: %w", err)
		}
		return nil
	}
	return nil
}

func (tx *Tx) RollbackDBOnlyUnlessCommitted() error {
	if !tx.isDBCommitted {
		if err := tx.rollbackDB(); err != nil {
			return xerrors.Errorf("failed to rollback: %w", err)
		}
		return nil
	}
	return nil
}

func (tx *Tx) RollbackUnlessCommitted() error {
	if err := tx.RollbackDBOnlyUnlessCommitted(); err != nil {
		return xerrors.Errorf("failed to rollback: %w", err)
	}
	if err := tx.RollbackCacheOnlyUnlessCommitted(); err != nil {
		return xerrors.Errorf("failed to rollback: %w", err)
	}
	return nil
}

func (r *Rapidash) Recover(queries []*QueryLog) error {
	mergedErr := []string{}
	for _, query := range queries {
		var serverAddr net.Addr
		if query.Addr != "" {
			addr, err := getAddr(query.Addr)
			if err != nil {
				return xerrors.Errorf("cannot get addr for recovery: %w", err)
			}
			serverAddr = addr
		}
		cacheKey := &CacheKey{
			key:  query.Key,
			hash: query.Hash,
			typ:  query.Type,
			addr: serverAddr,
		}
		if err := r.cacheServer.Delete(cacheKey); err != nil {
			mergedErr = append(mergedErr, err.Error())
		}
	}
	if len(mergedErr) > 0 {
		return xerrors.Errorf("%s: %w", strings.Join(mergedErr, ","), ErrRecoverCache)
	}
	return nil
}

func (r *Rapidash) BeforeCommitCallback(callback func(*Tx, []*QueryLog) error) {
	r.opt.beforeCommitCallback = callback
}

func (r *Rapidash) AfterCommitCallback(
	successCallback func(*Tx) error,
	failureCallback func(*Tx, []*QueryLog) error) {
	r.opt.afterCommitSuccessCallback = successCallback
	r.opt.afterCommitFailureCallback = failureCallback
}

// Ignore read/write to database without cache access
func (r *Rapidash) Ignore(conn *sql.DB, typ *Struct) error {
	r.ignoreCaches[typ.tableName] = struct{}{}
	if err := r.WarmUpSecondLevelCache(conn, typ); err != nil {
		return xerrors.Errorf("cannot warm up SecondLevelCache. table is %s: %w", typ.tableName, err)
	}
	return nil
}

func (r *Rapidash) WarmUp(conn *sql.DB, typ *Struct, isReadOnly bool) error {
	if isReadOnly {
		if err := r.WarmUpFirstLevelCache(conn, typ); err != nil {
			return xerrors.Errorf("cannot warm up FirstLevelCache: %w", err)
		}
		return nil
	}
	if err := r.WarmUpSecondLevelCache(conn, typ); err != nil {
		return xerrors.Errorf("cannot warm up SecondLevelCache: %w", err)
	}
	return nil
}

func (r *Rapidash) WarmUpFirstLevelCache(conn *sql.DB, typ *Struct) error {
	flc := NewFirstLevelCache(typ)
	if err := flc.WarmUp(conn); err != nil {
		return xerrors.Errorf("cannot warm up FirstLevelCache. table is %s: %w", typ.tableName, err)
	}
	r.firstLevelCaches.set(typ.tableName, flc)
	return nil
}

func (r *Rapidash) tableOption(tableName string) TableOption {
	opt := r.opt.slcTableOpt[tableName]
	if opt.expiration == nil {
		opt.expiration = &r.opt.slcExpiration
	}
	if opt.lockExpiration == nil {
		opt.lockExpiration = &r.opt.slcLockExpiration
	}
	if opt.optimisticLock == nil {
		opt.optimisticLock = &r.opt.slcOptimisticLock
	}
	if opt.pessimisticLock == nil {
		opt.pessimisticLock = &r.opt.slcPessimisticLock
	}
	return opt
}

func (r *Rapidash) WarmUpSecondLevelCache(conn *sql.DB, typ *Struct) error {
	slc := NewSecondLevelCache(typ, r.cacheServer, r.tableOption(typ.tableName))
	if err := slc.WarmUp(conn); err != nil {
		return xerrors.Errorf("cannot warm up SecondLevelCache. table is %s: %w", typ.tableName, err)
	}
	r.secondLevelCaches.set(typ.tableName, slc)
	return nil
}

func (r *Rapidash) RemoveServers(servers ...string) error {
	client := r.cacheServer.GetClient()
	if err := client.RemoveSecondLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to remove second level cache servers: %w", err)
	}
	if err := client.RemoveLastLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to remove last level cache servers: %w", err)
	}
	return nil
}

func (r *Rapidash) RemoveSecondLevelCacheServers(servers ...string) error {
	client := r.cacheServer.GetClient()
	if err := client.RemoveSecondLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to remove second level cache servers: %w", err)
	}
	return nil
}

func (r *Rapidash) RemoveLastLevelCacheServers(servers ...string) error {
	client := r.cacheServer.GetClient()
	if err := client.RemoveLastLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to remove last level cache servers: %w", err)
	}
	return nil
}

func (r *Rapidash) AddServers(servers ...string) error {
	client := r.cacheServer.GetClient()
	if err := client.AddSecondLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to add second level cache servers: %w", err)
	}
	if err := client.AddLastLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to add last level cache servers: %w", err)
	}
	return nil
}

func (r *Rapidash) AddSecondLevelCacheServer(servers ...string) error {
	client := r.cacheServer.GetClient()
	if err := client.AddSecondLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to add second level cache servers: %w", err)
	}
	return nil
}

func (r *Rapidash) AddLastLevelCacheServer(servers ...string) error {
	client := r.cacheServer.GetClient()
	if err := client.AddLastLevelCacheServers(servers...); err != nil {
		return xerrors.Errorf("failed to add last level cache servers: %w", err)
	}
	return nil
}

func (r *Rapidash) Flush() error {
	if err := r.cacheServer.Flush(); err != nil {
		return xerrors.Errorf("failed to flush cache server: %w", err)
	}
	return nil
}

func (r *Rapidash) setServer() error {
	switch r.opt.serverType {
	case CacheServerTypeMemcached:
		s := &Selectors{}
		if err := s.setSelector(r.opt.serverAddrs, r.opt.slcServerAddrs, r.opt.llcServerAddrs); err != nil {
			return xerrors.Errorf("failed to set cache server selector: %w", err)
		}
		memcached := server.NewMemcachedBySelectors(s.slcSelector, s.llcSelector)
		r.cacheServer = memcached
		r.lastLevelCache = NewLastLevelCache(r.cacheServer, r.opt.llcOpt)
	case CacheServerTypeRedis:
		s := &Selectors{}
		if err := s.setSelector(r.opt.serverAddrs, r.opt.slcServerAddrs, r.opt.llcServerAddrs); err != nil {
			return xerrors.Errorf("failed to set cache server selector: %w", err)
		}
		redis := server.NewRedisBySelectors(s.slcSelector, s.llcSelector)
		r.cacheServer = redis
		r.lastLevelCache = NewLastLevelCache(r.cacheServer, r.opt.llcOpt)
	case CacheServerTypeOnMemory:
	}
	if err := r.cacheServer.SetTimeout(r.opt.timeout); err != nil {
		return xerrors.Errorf("failed to set timeout for cache server: %w", err)
	}
	if err := r.cacheServer.SetMaxIdleConnections(r.opt.maxIdleConnections); err != nil {
		return xerrors.Errorf("failed to set max idle connections for cache server: %w", err)
	}
	return nil
}

func (r *Rapidash) setLogger() {
	if !r.opt.logEnabled {
		setNopLogger()
		return
	}
	switch r.opt.logMode {
	case LogModeConsole:
		setConsoleLogger()
	case LogModeJSON:
		setJSONLogger()
	case LogModeServerDebug:
	}
}

func (s *Selectors) setSelector(serverAddrs, slcServerAddrs, llcServerAddrs []string) error {
	if len(serverAddrs) > 0 {
		slcSelector, err := server.NewSelector(serverAddrs...)
		if err != nil {
			return err
		}
		s.slcSelector = slcSelector
		llcSelector, err := server.NewSelector(serverAddrs...)
		if err != nil {
			return err
		}
		s.llcSelector = llcSelector
	}
	if len(slcServerAddrs) > 0 {
		selector, err := server.NewSelector(slcServerAddrs...)
		if err != nil {
			return err
		}
		s.slcSelector = selector
	}
	if len(llcServerAddrs) > 0 {
		selector, err := server.NewSelector(llcServerAddrs...)
		if err != nil {
			return err
		}
		s.llcSelector = selector
	}
	return nil
}

func New(opts ...OptionFunc) (*Rapidash, error) {
	r := &Rapidash{
		ignoreCaches:      map[string]struct{}{},
		firstLevelCaches:  NewFirstLevelCacheMap(),
		secondLevelCaches: NewSecondLevelCacheMap(),
		opt:               defaultOption(),
	}
	for _, opt := range opts {
		opt(r)
	}
	if err := r.setServer(); err != nil {
		return nil, xerrors.Errorf("failed to set server: %w", err)
	}
	r.setLogger()
	return r, nil
}
