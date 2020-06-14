package rapidash

import (
	"time"

	"go.knocknote.io/rapidash/database"
)

type OptionFunc func(*Rapidash)

func ServerType(typ CacheServerType) OptionFunc {
	return func(r *Rapidash) {
		r.opt.serverType = typ
	}
}

func ServerAddrs(addrs []string) OptionFunc {
	return func(r *Rapidash) {
		r.opt.serverAddrs = addrs
	}
}

func SecondLevelCacheServerAddrs(addrs []string) OptionFunc {
	return func(r *Rapidash) {
		r.opt.slcServerAddrs = addrs
	}
}

func LastLevelCacheServerAddrs(addrs []string) OptionFunc {
	return func(r *Rapidash) {
		r.opt.llcServerAddrs = addrs
	}
}

func Timeout(timeout time.Duration) OptionFunc {
	return func(r *Rapidash) {
		r.opt.timeout = timeout
	}
}

func MaxIdleConnections(cons int) OptionFunc {
	return func(r *Rapidash) {
		r.opt.maxIdleConnections = cons
	}
}

func MaxRetryCount(cnt int) OptionFunc {
	return func(r *Rapidash) {
		r.opt.maxRetryCount = cnt
	}
}

func RetryInterval(interval time.Duration) OptionFunc {
	return func(r *Rapidash) {
		r.opt.retryInterval = interval
	}
}

func LogMode(mode LogModeType) OptionFunc {
	return func(r *Rapidash) {
		r.opt.logMode = mode
	}
}

func LogEnabled(enabled bool) OptionFunc {
	return func(r *Rapidash) {
		r.opt.logEnabled = enabled
	}
}

func LogServerAddr(addr string) OptionFunc {
	return func(r *Rapidash) {
		r.opt.logServerAddr = addr
	}
}

func SecondLevelCacheLockExpiration(expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		r.opt.slcLockExpiration = expiration
	}
}

func SecondLevelCacheExpiration(expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		r.opt.slcExpiration = expiration
	}
}

func SecondLevelCacheOptimisticLock(enabled bool) OptionFunc {
	return func(r *Rapidash) {
		r.opt.slcOptimisticLock = enabled
	}
}

func SecondLevelCachePessimisticLock(enabled bool) OptionFunc {
	return func(r *Rapidash) {
		r.opt.slcPessimisticLock = enabled
	}
}

func SecondLevelCacheTableShardKey(table string, shardKey string) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.slcTableOpt[table]
		opt.shardKey = &shardKey
		r.opt.slcTableOpt[table] = opt
	}
}

func SecondLevelCacheTableServerAddr(table string, serverAddr string) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.slcTableOpt[table]
		opt.server = &serverAddr
		r.opt.slcTableOpt[table] = opt
	}
}

func SecondLevelCacheTableExpiration(table string, expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.slcTableOpt[table]
		opt.expiration = &expiration
		r.opt.slcTableOpt[table] = opt
	}
}

func SecondLevelCacheTableLockExpiration(table string, expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.slcTableOpt[table]
		opt.lockExpiration = &expiration
		r.opt.slcTableOpt[table] = opt
	}
}

func SecondLevelCacheTableOptimisticLock(table string, enabled bool) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.slcTableOpt[table]
		opt.optimisticLock = &enabled
		r.opt.slcTableOpt[table] = opt
	}
}

func SecondLevelCacheTablePessimisticLock(table string, enabled bool) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.slcTableOpt[table]
		opt.pessimisticLock = &enabled
		r.opt.slcTableOpt[table] = opt
	}
}

func LastLevelCacheLockExpiration(expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		r.opt.llcOpt.lockExpiration = expiration
	}
}

func LastLevelCacheExpiration(expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		r.opt.llcOpt.expiration = expiration
	}
}

func LastLevelCacheOptimisticLock(enabled bool) OptionFunc {
	return func(r *Rapidash) {
		r.opt.llcOpt.optimisticLock = enabled
	}
}

func LastLevelCachePessimisticLock(enabled bool) OptionFunc {
	return func(r *Rapidash) {
		r.opt.llcOpt.pessimisticLock = enabled
	}
}

func LastLevelCacheTagServerAddr(tag string, serverAddr string) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.llcOpt.tagOpt[tag]
		opt.server = serverAddr
		r.opt.llcOpt.tagOpt[tag] = opt
	}
}

func LastLevelCacheTagIgnoreStash(tag string) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.llcOpt.tagOpt[tag]
		opt.ignoreStash = true
		r.opt.llcOpt.tagOpt[tag] = opt
	}
}

func LastLevelCacheTagExpiration(tag string, expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.llcOpt.tagOpt[tag]
		opt.expiration = expiration
		r.opt.llcOpt.tagOpt[tag] = opt
	}
}

func LastLevelCacheTagLockExpiration(tag string, expiration time.Duration) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.llcOpt.tagOpt[tag]
		opt.lockExpiration = expiration
		r.opt.llcOpt.tagOpt[tag] = opt
	}
}

func DatabaseAdapter(dbType database.DBType) OptionFunc {
	return func(r *Rapidash) {
		r.opt.adapter = database.NewAdapterWithDBType(dbType)
	}
}

func LastLevelCacheTagOptimisticLock(tag string, enabled bool) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.llcOpt.tagOpt[tag]
		opt.optimisticLock = &enabled
		r.opt.llcOpt.tagOpt[tag] = opt
	}
}

func LastLevelCacheTagPessimisticLock(tag string, enabled bool) OptionFunc {
	return func(r *Rapidash) {
		opt := r.opt.llcOpt.tagOpt[tag]
		opt.pessimisticLock = &enabled
		r.opt.llcOpt.tagOpt[tag] = opt
	}
}
