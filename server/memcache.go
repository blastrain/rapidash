package server

/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/xerrors"
)

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func NewMemcachedBySelectors(slcSelector *Selector, llcSelector *Selector) CacheServer {
	client := &Client{slcSelector: slcSelector, llcSelector: llcSelector}
	return &MemcachedClient{client: client}
}

func (c *MemcachedClient) GetClient() *Client {
	return c.client
}

func (c *MemcachedClient) SetTimeout(timeout time.Duration) error {
	if timeout == time.Duration(0) {
		return ErrSetTimeout
	}
	c.client.timeout = timeout
	return nil
}

func (c *MemcachedClient) SetMaxIdleConnections(maxIdle int) error {
	if maxIdle <= 0 {
		return ErrSetMaxIdleConnections
	}
	c.client.maxIdleConns = maxIdle
	return nil
}

func (c *MemcachedClient) Get(key CacheKey) (*CacheGetResponse, error) {
	item, err := c.get(key)
	if err == ErrMemcacheCacheMiss {
		return nil, ErrCacheMiss
	}
	if err != nil {
		return nil, xerrors.Errorf("failed to get cache: %w", err)
	}
	return &CacheGetResponse{
		Value: item.Value,
		Flags: item.Flags,
		CasID: item.casid,
	}, nil
}

func (c *MemcachedClient) GetMulti(keys []CacheKey) (*Iterator, error) {
	itemMap, err := c.getMulti(keys)
	if err != nil {
		return nil, xerrors.Errorf("failed to get caches: %w", err)
	}
	iter := NewIterator(keys)
	for idx, key := range keys {
		item, exists := itemMap[key.String()]
		if exists {
			iter.SetContent(idx, &CacheGetResponse{
				Value: item.Value,
				Flags: item.Flags,
				CasID: item.casid,
			})
		} else {
			iter.SetError(idx, ErrCacheMiss)
		}
	}
	return iter, nil
}

func (c *MemcachedClient) Set(req *CacheStoreRequest) error {
	item := &Item{
		Key:        req.Key,
		Flags:      req.Key.Hash(),
		Value:      req.Value,
		casid:      req.CasID,
		Expiration: int32(req.Expiration / time.Second),
	}
	if req.CasID != 0 {
		if err := c.CompareAndSwap(item); err != nil {
			return xerrors.Errorf("failed set value to %s: %w", req.Key, err)
		}
		return nil
	}
	if err := c.onItem(item, (*MemcachedClient).set); err != nil {
		return xerrors.Errorf("failed set value to %s: %w", req.Key, err)
	}
	return nil
}

func (c *MemcachedClient) Add(key CacheKey, value []byte, expiration time.Duration) error {
	if err := c.onItem(
		&Item{
			Key:        key,
			Value:      value,
			Expiration: int32(expiration / time.Second),
		},
		(*MemcachedClient).add,
	); err != nil {
		return xerrors.Errorf("failed add value to %s: %w", key, err)
	}
	return nil
}

func (c *MemcachedClient) Delete(key CacheKey) error {
	if err := c.delete(key); err != nil {
		if err == ErrMemcacheCacheMiss {
			// ignore cache miss
			return nil
		}
		return xerrors.Errorf("failed to delete cache: %w", err)
	}
	return nil
}

func (c *MemcachedClient) Flush() error {
	if err := c.FlushAll(); err != nil {
		return xerrors.Errorf("failed to flush cache: %w", err)
	}
	return nil
}

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrMemcacheCacheMiss means that a Get failed because the item wasn't present.
	ErrMemcacheCacheMiss = xerrors.New("memcache: cache miss")

	// ErrMemcacheCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrMemcacheCASConflict = xerrors.New("memcache: compare-and-swap conflict")

	// ErrMemcacheNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrMemcacheNotStored = xerrors.New("memcache: item not stored")

	// ErrMemcacheServerError means that a server error occurred.
	ErrMemcacheServerError = xerrors.New("memcache: server error")

	// ErrMemcacheNoStats means that no statistics were available.
	ErrMemcacheNoStats = xerrors.New("memcache: no statistics available")

	// ErrMemcacheNoServers is returned when no servers are configured or available.
	ErrMemcacheNoServers = xerrors.New("memcache: no servers configured or available")
)

var (
	crlf            = []byte("\r\n")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
)

func (c *MemcachedClient) onItem(item *Item, fn func(*MemcachedClient, *bufio.ReadWriter, *Item) error) error {
	addr, err := c.client.getAddr(item.Key)
	if err != nil {
		return err
	}
	cn, err := c.client.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	if err = fn(c, cn.rw, item); err != nil {
		return err
	}
	return nil
}

func (c *MemcachedClient) FlushAll() error {
	if err := c.client.slcSelector.Each(c.flushAllFromAddr); err != nil {
		return err
	}
	if err := c.client.llcSelector.Each(c.flushAllFromAddr); err != nil {
		return err
	}
	return nil
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *MemcachedClient) get(key CacheKey) (item *Item, err error) {
	err = c.client.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, []string{key.String()}, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrMemcacheCacheMiss
	}
	return
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. Zero means the item has
// no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *MemcachedClient) Touch(key CacheKey, seconds int32) (err error) {
	return c.client.withKeyAddr(key, func(addr net.Addr) error {
		return c.touchFromAddr(addr, []string{key.String()}, seconds)
	})
}

func (c *MemcachedClient) withAddrRw(addr net.Addr, fn func(*bufio.ReadWriter) error) (err error) {
	cn, err := c.client.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	return fn(cn.rw)
}

func (c *MemcachedClient) withKeyRw(key CacheKey, fn func(*bufio.ReadWriter) error) error {
	return c.client.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRw(addr, fn)
	})
}

func (c *MemcachedClient) getFromAddr(addr net.Addr, keys []string, cb func(*Item)) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		if err := parseGetResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *MemcachedClient) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "flush_all\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOk):
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

func (c *MemcachedClient) touchFromAddr(addr net.Addr, keys []string, expiration int32) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, key := range keys {
			if _, err := fmt.Fprintf(rw, "touch %s %d\r\n", key, expiration); err != nil {
				return err
			}
			if err := rw.Flush(); err != nil {
				return err
			}
			line, err := rw.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				break
			case bytes.Equal(line, resultNotFound):
				return ErrMemcacheCacheMiss
			default:
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		}
		return nil
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *MemcachedClient) getMulti(keys []CacheKey) (map[string]*Item, error) {
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

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item
func parseGetResponse(r *bufio.Reader, cb func(*Item)) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)
		_, err = io.ReadFull(r, it.Value)
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		cb(it)
	}
}

const (
	StateKey int = iota
	StateFlag
	StateSize
	StateCasID
)

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	headerSize := 6 // "VALUE "
	size = -1
	if len(line) <= headerSize {
		err = fmt.Errorf("memcache: unexpected line in get response: %q", line)
		return
	}
	lineWithoutHeader := line[headerSize:]
	state := StateKey
	prevIdx := 0
	for i, b := range lineWithoutHeader {
		if !(b == 0x20 || b == 0x0d) {
			continue
		}

		// byte is SPACE or CR
		bytes := lineWithoutHeader[prevIdx:i]
		s := string(bytes)
		switch state {
		case StateKey:
			it.Key = StringCacheKey(s)
			state = StateFlag
		case StateFlag:
			flag, err := strconv.ParseUint(s, 10, 32)
			if err != nil {
				return size, err
			}
			it.Flags = uint32(flag)
			state = StateSize
		case StateSize:
			size, err = strconv.Atoi(s)
			if err != nil {
				return size, err
			}
			state = StateCasID
		case StateCasID:
			casID, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return size, err
			}
			it.casid = casID
		}
		prevIdx = i + 1
	}
	if size == -1 {
		err = fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, err
}

func (c *MemcachedClient) set(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "set", item)
}

func (c *MemcachedClient) add(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *MemcachedClient) Replace(item *Item) error {
	return c.onItem(item, (*MemcachedClient).replace)
}

func (c *MemcachedClient) replace(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "replace", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *MemcachedClient) CompareAndSwap(item *Item) error {
	return c.onItem(item, (*MemcachedClient).cas)
}

func (c *MemcachedClient) cas(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "cas", item)
}

func (c *MemcachedClient) populateOne(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !legalKey(item.Key.String()) {
		return ErrMalformedKey
	}
	var err error
	if verb == "cas" {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid)
	} else {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value))
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil {
		return err
	}
	if _, err := rw.Write(crlf); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrMemcacheNotStored
	case bytes.Equal(line, resultExists):
		return ErrMemcacheCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrMemcacheCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

func writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...)
	if err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	}
	line, err := rw.ReadSlice('\n')
	return line, err
}

func writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrMemcacheNotStored
	case bytes.Equal(line, resultExists):
		return ErrMemcacheCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrMemcacheCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *MemcachedClient) delete(key CacheKey) error {
	return c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
	})
}

// DeleteAll deletes all items in the cache.
func (c *MemcachedClient) DeleteAll() error {
	return c.withKeyRw(StringCacheKey(""), func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "flush_all\r\n")
	})
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *MemcachedClient) Increment(key CacheKey, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *MemcachedClient) Decrement(key CacheKey, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *MemcachedClient) incrDecr(verb string, key CacheKey, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrMemcacheCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return xerrors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}
