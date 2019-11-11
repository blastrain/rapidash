package rapidash

import (
	"fmt"
	"time"

	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

type LastLevelCache struct {
	cacheServer server.CacheServer
	opt         *LastLevelCacheOption
}

func NewLastLevelCache(cacheServer server.CacheServer, opt *LastLevelCacheOption) *LastLevelCache {
	return &LastLevelCache{
		cacheServer: cacheServer,
		opt:         opt,
	}
}

func (c *LastLevelCache) cacheKey(tag, key string) (server.CacheKey, error) {
	cacheKey := &CacheKey{
		key: fmt.Sprintf("r/llc/%s", key),
		typ: server.CacheKeyTypeLLC,
	}
	if opt, exists := c.opt.tagOpt[tag]; exists {
		addr, err := getAddr(opt.server)
		if err != nil {
			return nil, xerrors.Errorf("cannot get addr: %w", err)
		}
		cacheKey.addr = addr
	} else if tag != "" {
		cacheKey.hash = NewStringValue(tag).Hash()
	} else {
		cacheKey.hash = NewStringValue(key).Hash()
	}
	return cacheKey, nil
}

func (c *LastLevelCache) lockKey(tx *Tx, key server.CacheKey, expiration time.Duration) error {
	value := &TxValue{
		id:   tx.id,
		key:  key.String(),
		time: time.Now(),
	}
	bytes, err := value.Marshal()
	if err != nil {
		return xerrors.Errorf("cannot marshal value: %w", err)
	}
	lockKey := key.LockKey()
	log.Add(tx.id, lockKey, value)
	if err := c.cacheServer.Add(lockKey, bytes, expiration); err != nil {
		content, getErr := c.cacheServer.Get(lockKey)
		if xerrors.Is(getErr, server.ErrCacheMiss) {
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

func (c *LastLevelCache) Create(tx *Tx, tag, key string, value Type, expiration time.Duration) error {
	cacheKey, err := c.cacheKey(tag, key)
	if err != nil {
		return xerrors.Errorf("failed to get cacheKey: %w", err)
	}
	content, err := value.Encode()
	if err != nil {
		return xerrors.Errorf("failed to encode value: %w", err)
	}
	keyStr := cacheKey.String()
	tx.stash.lastLevelCacheKeyToBytes[keyStr] = content
	if c.opt.pessimisticLock {
		if _, exists := tx.pendingQueries[keyStr]; !exists {
			if err := c.lockKey(tx, cacheKey, expiration); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	var addrStr string
	if addr := cacheKey.Addr(); addr != nil {
		addrStr = addr.String()
	}
	tx.pendingQueries[keyStr] = &PendingQuery{
		QueryLog: &QueryLog{
			Key:  keyStr,
			Hash: cacheKey.Hash(),
			Type: server.CacheKeyTypeLLC,
			Addr: addrStr,
		},
		fn: func() error {
			if err := c.cacheServer.Add(cacheKey, content, expiration); err != nil {
				return xerrors.Errorf("failed to add cache to server: %w", err)
			}
			return nil
		},
	}
	return nil
}

func (c *LastLevelCache) Find(tx *Tx, tag, key string, value Type) error {
	cacheKey, err := c.cacheKey(tag, key)
	if err != nil {
		return xerrors.Errorf("failed to get cacheKey: %w", err)
	}
	if content, exists := tx.stash.lastLevelCacheKeyToBytes[cacheKey.String()]; exists {
		if err := value.Decode(content); err != nil {
			return xerrors.Errorf("failed to decode value: %w", err)
		}
		return nil
	}
	content, err := c.cacheServer.Get(cacheKey)
	if err != nil {
		return xerrors.Errorf("failed to get cache from server: %w", err)
	}
	if err := value.Decode(content.Value); err != nil {
		return xerrors.Errorf("failed to decode value: %w", err)
	}
	return nil
}

func (c *LastLevelCache) Update(tx *Tx, tag, key string, value Type, expiration time.Duration) error {
	content, err := value.Encode()
	if err != nil {
		return xerrors.Errorf("failed to encode value: %w", err)
	}
	cacheKey, err := c.cacheKey(tag, key)
	if err != nil {
		return xerrors.Errorf("failed to get cacheKey: %w", err)
	}
	keyStr := cacheKey.String()
	if c.opt.pessimisticLock {
		if _, exists := tx.pendingQueries[keyStr]; !exists {
			if err := c.lockKey(tx, cacheKey, expiration); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	var addrStr string
	if addr := cacheKey.Addr(); addr != nil {
		addrStr = addr.String()
	}
	tx.stash.lastLevelCacheKeyToBytes[keyStr] = content
	tx.pendingQueries[keyStr] = &PendingQuery{
		QueryLog: &QueryLog{
			Key:  keyStr,
			Hash: cacheKey.Hash(),
			Type: server.CacheKeyTypeLLC,
			Addr: addrStr,
		},
		fn: func() error {
			if err := c.cacheServer.Set(&server.CacheStoreRequest{
				Key:   cacheKey,
				Value: content,
			}); err != nil {
				return xerrors.Errorf("failed to set cache to server: %w", err)
			}
			return nil
		},
	}
	return nil
}

func (c *LastLevelCache) Delete(tx *Tx, tag, key string) error {
	cacheKey, err := c.cacheKey(tag, key)
	if err != nil {
		return xerrors.Errorf("failed to get cacheKey: %w", err)
	}
	keyStr := cacheKey.String()
	delete(tx.stash.lastLevelCacheKeyToBytes, keyStr)
	var addrStr string
	if addr := cacheKey.Addr(); addr != nil {
		addrStr = addr.String()
	}
	tx.pendingQueries[keyStr] = &PendingQuery{
		QueryLog: &QueryLog{
			Key:  keyStr,
			Hash: cacheKey.Hash(),
			Type: server.CacheKeyTypeLLC,
			Addr: addrStr,
		},
		fn: func() error {
			if err := c.cacheServer.Delete(cacheKey); err != nil {
				return xerrors.Errorf("failed to delete cache from server: %w", err)
			}
			return nil
		},
	}
	return nil
}
