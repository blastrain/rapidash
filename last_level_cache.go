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
	if opt := c.opt.tagOpt[tag]; opt.server != "" {
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

func (c *LastLevelCache) enabledStash(tag string) bool {
	opt := c.opt.tagOpt[tag]
	return !opt.ignoreStash
}

func (c *LastLevelCache) shouldPessimisticLock(tag string) bool {
	opt, exists := c.opt.tagOpt[tag];
	if !exists {
		return c.opt.pessimisticLock
	}
	return opt.pessimisticLock
}
func (c *LastLevelCache) shouldOptimisticLock(tag string) bool {
	opt, exists := c.opt.tagOpt[tag];
	if !exists {
		return c.opt.optimisticLock
	}
	return opt.optimisticLock
}

func (c *LastLevelCache) set(tx *Tx, tag string, cacheKey server.CacheKey, content []byte, expiration time.Duration) error {
	casID := uint64(0)
	if c.shouldOptimisticLock(tag) {
		casID = tx.stash.casIDs[cacheKey.String()]
	}
	if err := c.cacheServer.Set(&server.CacheStoreRequest{
		Key:        cacheKey,
		Value:      content,
		Expiration: expiration,
		CasID:      casID,
	}); err != nil {
		return xerrors.Errorf("failed to set cache to server: %w", err)
	}
	return nil
}

func (c *LastLevelCache) existsLockKey(tx *Tx, cacheKey server.CacheKey) bool {
	key := cacheKey.LockKey().String()
	for _, lockKey := range tx.lockKeys {
		if key == lockKey.String() {
			return true
		}
	}
	return false
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
	if c.enabledStash(tag) {
		tx.stash.lastLevelCacheKeyToBytes[keyStr] = content
	}
	if c.shouldPessimisticLock(tag) {
		if !c.existsLockKey(tx, cacheKey) {
			if err := c.lockKey(tx, cacheKey, c.opt.lockExpiration); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	var addrStr string
	if addr := cacheKey.Addr(); addr != nil {
		addrStr = addr.String()
	}
	if c.enabledStash(tag) {
		tx.pendingQueries[keyStr] = &PendingQuery{
			QueryLog: &QueryLog{
				Command: "add",
				Key:     keyStr,
				Hash:    cacheKey.Hash(),
				Type:    server.CacheKeyTypeLLC,
				Addr:    addrStr,
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
	if err := c.cacheServer.Add(cacheKey, content, expiration); err != nil {
		return xerrors.Errorf("failed to add cache to server: %w", err)
	}
	return nil
}

func (c *LastLevelCache) Find(tx *Tx, tag, key string, value Type) error {
	cacheKey, err := c.cacheKey(tag, key)
	if err != nil {
		return xerrors.Errorf("failed to get cacheKey: %w", err)
	}
	if c.enabledStash(tag) {
		if content, exists := tx.stash.lastLevelCacheKeyToBytes[cacheKey.String()]; exists {
			if err := value.Decode(content); err != nil {
				return xerrors.Errorf("failed to decode value: %w", err)
			}
			return nil
		}
	}
	content, err := c.cacheServer.Get(cacheKey)
	if err != nil {
		return xerrors.Errorf("failed to get cache from server: %w", err)
	}
	tx.stash.casIDs[cacheKey.String()] = content.CasID
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

	if c.shouldPessimisticLock(tag) {
		if !c.existsLockKey(tx, cacheKey) {
			if err := c.lockKey(tx, cacheKey, c.opt.lockExpiration); err != nil {
				return xerrors.Errorf("failed to lock key: %w", err)
			}
		}
	}
	var addrStr string
	if addr := cacheKey.Addr(); addr != nil {
		addrStr = addr.String()
	}
	if c.enabledStash(tag) {
		tx.stash.lastLevelCacheKeyToBytes[keyStr] = content
		tx.pendingQueries[keyStr] = &PendingQuery{
			QueryLog: &QueryLog{
				Command: "set",
				Key:     keyStr,
				Hash:    cacheKey.Hash(),
				Type:    server.CacheKeyTypeLLC,
				Addr:    addrStr,
			},
			fn: func() error {
				if err := c.set(tx, tag, cacheKey, content, expiration); err != nil {
					return xerrors.Errorf("failed to set: %w", err)
				}
				return nil
			},
		}
		return nil
	}
	if err := c.set(tx, tag, cacheKey, content, expiration); err != nil {
		return xerrors.Errorf("failed to set: %w", err)
	}
	return nil
}

func (c *LastLevelCache) Delete(tx *Tx, tag, key string) error {
	cacheKey, err := c.cacheKey(tag, key)
	if err != nil {
		return xerrors.Errorf("failed to get cacheKey: %w", err)
	}
	keyStr := cacheKey.String()
	if c.enabledStash(tag) {
		delete(tx.stash.lastLevelCacheKeyToBytes, keyStr)
	}
	var addrStr string
	if addr := cacheKey.Addr(); addr != nil {
		addrStr = addr.String()
	}
	if c.enabledStash(tag) {
		tx.pendingQueries[keyStr] = &PendingQuery{
			QueryLog: &QueryLog{
				Command: "delete",
				Key:     keyStr,
				Hash:    cacheKey.Hash(),
				Type:    server.CacheKeyTypeLLC,
				Addr:    addrStr,
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
	if err := c.cacheServer.Delete(cacheKey); err != nil {
		return xerrors.Errorf("failed to delete cache from server: %w", err)
	}
	return nil
}
