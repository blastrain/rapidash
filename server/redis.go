package server

import (
	"net"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/xerrors"
)

var (
	ErrRedisCacheMiss = xerrors.New("redis: cache miss")
	ErrRedisNotStored = xerrors.New("redis: item not stored")
)

type RedisClient struct {
	client *Client
}

func NewRedisBySelectors(slcSelector *Selector, llcSelector *Selector) CacheServer {
	client := &Client{slcSelector: slcSelector, llcSelector: llcSelector}
	return &RedisClient{client: client}
}

func (c *RedisClient) GetClient() *Client {
	return c.client
}

func (c *RedisClient) SetTimeout(timeout time.Duration) error {
	if timeout == time.Duration(0) {
		return ErrSetTimeout
	}
	c.client.timeout = timeout
	return nil
}

func (c *RedisClient) SetMaxIdleConnections(maxIdle int) error {
	if maxIdle <= 0 {
		return ErrSetMaxIdleConnections
	}
	c.client.maxIdleConns = maxIdle
	return nil
}

func (c *RedisClient) Get(key CacheKey) (*CacheGetResponse, error) {
	item, err := c.get(key)

	if err == ErrRedisCacheMiss {
		return nil, ErrCacheMiss
	}

	if err != nil {
		return nil, xerrors.Errorf("failed to get cache: %w", err)
	}

	return &CacheGetResponse{
		Value: item.Value,
	}, nil
}

func (c *RedisClient) GetMulti(keys []CacheKey) (*Iterator, error) {
	itemMap, err := c.getMulti(keys)
	if err != nil {
		return nil, xerrors.Errorf("failed to get caches: %w", err)
	}
	iter := NewIterator(keys)
	for idx, key := range keys {
		item, exists := itemMap[key.String()]
		if exists {
			if len(item.Value) != 0 {
				iter.SetContent(idx, &CacheGetResponse{
					Value: item.Value,
				})
			} else {
				iter.SetError(idx, ErrCacheMiss)
			}
		} else {
			iter.SetError(idx, ErrCacheMiss)
		}
	}
	return iter, nil
}

func (c *RedisClient) Set(req *CacheStoreRequest) error {
	item := &Item{
		Key:        req.Key,
		Flags:      req.Key.Hash(),
		Value:      req.Value,
		casid:      req.CasID,
		Expiration: int32(req.Expiration),
	}

	if err := c.onItem(
		item,
		(*RedisClient).set,
	); err != nil {
		return xerrors.Errorf("failed set value to %s: %w", req.Key, err)
	}

	return nil
}

func (c *RedisClient) Add(key CacheKey, value []byte, expiration time.Duration) error {
	if err := c.onItem(
		&Item{
			Key:        key,
			Value:      value,
			Expiration: int32(expiration),
		},
		(*RedisClient).add,
	); err != nil {
		return xerrors.Errorf("failed add value to %s: %w", key, err)
	}

	return nil
}

func (c *RedisClient) Delete(key CacheKey) error {
	if err := c.delete(key); err != nil {
		if err == ErrRedisCacheMiss {
			// ignore cache miss
			return nil
		}
		return xerrors.Errorf("failed to delete cache: %w", err)
	}
	return nil
}

func (c *RedisClient) Flush() error {
	if err := c.client.slcSelector.Each(c.flushAllFromAddr); err != nil {
		return xerrors.Errorf("failed to flush second level cache: %w", err)
	}

	if err := c.client.llcSelector.Each(c.flushAllFromAddr); err != nil {
		return xerrors.Errorf("failed to flush last level cache: %w", err)
	}

	return nil
}

func (c *RedisClient) get(key CacheKey) (item *Item, err error) {
	err = c.client.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, []string{key.String()}, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrRedisCacheMiss
	}

	return
}

func (c *RedisClient) getMulti(keys []CacheKey) (map[string]*Item, error) {
	var lk sync.Mutex
	m := make(map[string]*Item, len(keys))
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key.String()] = it
	}

	keyMap := make(map[net.Addr][]string, len(keys))
	for _, key := range keys {
		k := key.String()
		if !legalKey(k) {
			return nil, ErrMalformedKey
		}
		addr, err := c.client.getAddr(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], k)
	}

	addrNum := len(keyMap)
	if addrNum == 1 {
		for addr, keys := range keyMap {
			if err := c.getFromAddr(addr, keys, addItemToMap); err != nil {
				return nil, err
			}
			return m, nil
		}
	}
	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.getFromAddr(addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

func (c *RedisClient) set(rc redis.Conn, item *Item) error {
	res := c.populateOne(rc, "set", item)
	return res
}

func (c *RedisClient) add(rc redis.Conn, item *Item) error {
	res := c.populateOne(rc, "add", item)
	return res
}

func (c *RedisClient) delete(key CacheKey) error {
	return c.client.withKeyAddr(key, func(addr net.Addr) (e error) {
		cn, err := c.client.getConn(addr)
		if err != nil {
			return err
		}
		defer cn.condRelease(&err)

		rc := c.getRedisConn(cn)

		reply, err := rc.Do("del", key)

		status, ok := reply.(int64)
		if ok && status == 0 {
			return ErrRedisCacheMiss
		}
		return nil
	})
}

func (c *RedisClient) getFromAddr(addr net.Addr, keys []string, cb func(*Item)) (err error) {
	cn, err := c.client.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)

	rc := c.getRedisConn(cn)

	replies := make([]*Item, len(keys))
	for i, key := range keys {
		replies[i] = new(Item)
		replies[i].Key = StringCacheKey(key)
	}
	if len(keys) == 1 {
		replies[0].Value, err = redis.Bytes(rc.Do("get", keys[0]))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
	} else {
		var args []interface{}
		for _, key := range keys {
			args = append(args, key)
		}
		byteSlices, err := redis.ByteSlices(rc.Do("mget", args...))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
		for i, byteSlice := range byteSlices {
			replies[i].Value = byteSlice
		}
	}

	parseGetRedisResponse(replies, cb)

	return nil
}

func (c *RedisClient) flushAllFromAddr(addr net.Addr) (err error) {
	cn, err := c.client.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)

	rc := c.getRedisConn(cn)

	if _, err := rc.Do("FLUSHALL"); err != nil {
		return err
	}

	return nil
}

func (c *RedisClient) onItem(item *Item, fn func(*RedisClient, redis.Conn, *Item) error) (err error) {
	addr, err := c.client.getAddr(item.Key)
	if err != nil {
		return err
	}

	cn, err := c.client.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)

	rc := c.getRedisConn(cn)

	if err = fn(c, rc, item); err != nil {
		return err
	}

	return nil
}

func (c *RedisClient) populateOne(conn redis.Conn, verb string, item *Item) error {
	if !legalKey(item.Key.String()) {
		return ErrMalformedKey
	}

	args := []interface{}{item.Key, item.Value}
	if verb == "add" {
		args = append(args, "nx")
	}
	if item.Expiration != 0 {
		args = append(args, "px", item.Expiration)
	}

	reply, err := conn.Do("set", args...)
	if err != nil {
		return err
	}
	if reply == nil {
		return ErrRedisNotStored
	}

	return nil
}

func (c *RedisClient) getRedisConn(cn *conn) redis.Conn {
	return redis.NewConn(cn.nc, c.client.timeout, c.client.timeout)
}

func parseGetRedisResponse(replies []*Item, cb func(*Item)) {
	for _, reply := range replies {
		cb(reply)
	}
}
