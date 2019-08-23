package rapidash

import (
	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

var (
	ErrBeginTransaction            = xerrors.New("failed begin cache transaction. required single connection instance or nothing")
	ErrConnectionOfTransaction     = xerrors.New("connection instance ( like sql.DB or sql.Tx ) is required for (*Rapidash).Begin()")
	ErrAlreadyCommittedTransaction = xerrors.New("transaction is already committed")
	ErrUnlockCacheKeys             = xerrors.New("failed unlock cache keys")
	ErrCacheCommit                 = xerrors.New("failed cache commit")
	ErrCleanUpCache                = xerrors.New("failed clean up cache")
	ErrRecoverCache                = xerrors.New("failed recover cache")
)

var (
	ErrInvalidQuery         = xerrors.New("query builder includes not equal query")
	ErrLookUpIndexFromQuery = xerrors.New("cannot lookup index from query")
	ErrMultipleINQueries    = xerrors.New("multiple IN queries are not supported")
	ErrInvalidColumnType    = xerrors.New("invalid column type")
)

var (
	ErrRecordNotFoundByPrimaryKey = xerrors.New("cannot find record by primary key")
	ErrInvalidLeafs               = xerrors.New("failed to find values. ( invalid leafs )")
)

var (
	ErrCacheMiss                           = xerrors.New("cache miss hit")
	ErrCreatePrimaryKeyCacheBySlice        = xerrors.New("cannot create cache for primary key with slice value")
	ErrCreateUniqueKeyCacheBySlice         = xerrors.New("cannot create cache for unique key with slice value")
	ErrCreateCacheKeyAtMultiplePrimaryKeys = xerrors.New("cannot find by primary key because table is set multiple primary keys")
)

var (
	ErrScanToNilValue    = xerrors.New("cannot scan to nil value")
	ErrUnknownColumnType = xerrors.New("unknown column type")
	ErrUnknownColumnName = xerrors.New("unknown column name")
	ErrInvalidDecodeType = xerrors.New("invalid decode type")
	ErrInvalidEncodeType = xerrors.New("invalid encode type")
)

var (
	ErrInvalidCacheKey = xerrors.New("invalid cache key")
)

func IsCacheMiss(err error) bool {
	if xerrors.Is(err, ErrCacheMiss) {
		return true
	}
	if xerrors.Is(err, server.ErrCacheMiss) {
		return true
	}
	return false
}

func IsTimeout(err error) bool {
	return xerrors.Is(err, server.ErrSetTimeout)
}

func IsMaxIdleConnections(err error) bool {
	return xerrors.Is(err, server.ErrSetMaxIdleConnections)
}
