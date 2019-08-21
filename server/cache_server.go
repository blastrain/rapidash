package server

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/xerrors"
)

var (
	ErrCacheMiss = xerrors.New("cache miss hit")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = xerrors.New("malformed: key is too long or contains invalid characters")

	ErrSetTimeout            = xerrors.New("timeout must be 1 or more")
	ErrSetMaxIdleConnections = xerrors.New("maxIdle must be 1 or more")
)

const buffered = 8 // arbitrary buffered channel size, for readability

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrMemcacheCacheMiss, ErrMemcacheCASConflict,
		ErrMemcacheNotStored, ErrMalformedKey:
		return true
	}
	return false
}

type MemcachedClient struct {
	client *Client
}

type StringCacheKey string

func (s StringCacheKey) String() string {
	return string(s)
}

func (s StringCacheKey) Hash() uint32 {
	return 0
}

func (s StringCacheKey) LockKey() CacheKey {
	return s
}

func (s StringCacheKey) Type() CacheKeyType {
	return CacheKeyTypeNone
}

func (s StringCacheKey) Addr() net.Addr {
	return nil
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

type CacheKeyType int

const (
	CacheKeyTypeNone CacheKeyType = iota
	CacheKeyTypeSLC
	CacheKeyTypeLLC
)

func (typ CacheKeyType) String() string {
	switch typ {
	case CacheKeyTypeSLC:
		return "slc"
	case CacheKeyTypeLLC:
		return "llc"
	}
	return "none"
}

func (typ CacheKeyType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, typ.String())), nil
}

func (typ *CacheKeyType) UnmarshalJSON(bytes []byte) error {
	switch string(bytes) {
	case `"slc"`:
		*typ = CacheKeyTypeSLC
	case `"llc"`:
		*typ = CacheKeyTypeLLC
	default:
		*typ = CacheKeyTypeNone
	}
	return nil
}

type CacheKey interface {
	String() string
	Hash() uint32
	Addr() net.Addr
	LockKey() CacheKey
	Type() CacheKeyType
}

type CacheServer interface {
	GetClient() *Client
	Get(CacheKey) (*CacheGetResponse, error)
	GetMulti([]CacheKey) (*Iterator, error)
	Set(*CacheStoreRequest) error
	Add(CacheKey, []byte, time.Duration) error
	Delete(CacheKey) error
	Flush() error
	SetTimeout(time.Duration) error
	SetMaxIdleConnections(int) error
}

type CacheGetResponse struct {
	Value []byte
	Flags uint32
	CasID uint64
}

type CacheStoreRequest struct {
	Key        CacheKey
	Value      []byte
	CasID      uint64
	Expiration time.Duration
}

type Iterator struct {
	currentIndex int
	keys         []CacheKey
	values       []*CacheGetResponse
	errs         []error
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	timeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	maxIdleConns int

	slcSelector *Selector
	llcSelector *Selector

	lk       sync.Mutex
	freeconn map[string][]*conn
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key CacheKey

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

func (i *Iterator) SetContent(idx int, res *CacheGetResponse) {
	i.values[idx] = res
}

func (i *Iterator) SetError(idx int, err error) {
	i.errs[idx] = err
}

func NewIterator(keys []CacheKey) *Iterator {
	return &Iterator{
		currentIndex: -1,
		keys:         keys,
		values:       make([]*CacheGetResponse, len(keys)),
		errs:         make([]error, len(keys)),
	}
}

func (i *Iterator) Next() bool {
	if i.currentIndex < len(i.keys)-1 {
		i.currentIndex++
		return true
	}
	return false
}

func (i *Iterator) Key() CacheKey {
	return i.keys[i.currentIndex]
}

func (i *Iterator) Content() *CacheGetResponse {
	return i.values[i.currentIndex]
}

func (i *Iterator) Error() error {
	return i.errs[i.currentIndex]
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

func (c *Client) addServers(selector *Selector, servers []string) error {
	var hashring *Hashring
	for _, server := range servers {
		addr, err := getAddr(server)
		if err != nil {
			return xerrors.Errorf("failed to get addr: %w", err)
		}
		hashring = selector.ring.Add(addr)
	}
	selector.ring = hashring
	return nil
}

func (c *Client) removeServers(selector *Selector, servers []string) error {
	var hashring *Hashring
	for _, server := range servers {
		addr, err := getAddr(server)
		if err != nil {
			return xerrors.Errorf("failed to get addr: %w", err)
		}
		ring, err := selector.ring.Remove(addr)
		if err != nil {
			return xerrors.Errorf("failed to remove server: %w", err)
		}
		hashring = ring
	}
	selector.ring = hashring
	return nil
}

func (c *Client) AddSecondLevelCacheServers(servers ...string) error {
	if err := c.addServers(c.slcSelector, servers); err != nil {
		return xerrors.Errorf("failed to add servers %v: %w", servers, err)
	}
	return nil
}

func (c *Client) AddLastLevelCacheServers(servers ...string) error {
	if err := c.addServers(c.llcSelector, servers); err != nil {
		return xerrors.Errorf("failed to add servers %v: %w", servers, err)
	}
	return nil
}

func (c *Client) RemoveSecondLevelCacheServers(servers ...string) error {
	if err := c.removeServers(c.slcSelector, servers); err != nil {
		return xerrors.Errorf("failed to remove servers %v: %w", servers, err)
	}
	return nil
}

func (c *Client) RemoveLastLevelCacheServers(servers ...string) error {
	if err := c.removeServers(c.llcSelector, servers); err != nil {
		return xerrors.Errorf("failed to remove servers %v: %w", servers, err)
	}
	return nil
}

func (c *Client) getAddr(key CacheKey) (net.Addr, error) {
	switch key.Type() {
	case CacheKeyTypeSLC:
		return c.slcSelector.PickServer(key)
	case CacheKeyTypeLLC:
		return c.llcSelector.PickServer(key)
	}
	return nil, xerrors.Errorf("cannot pick server by %s", key.String())
}

func (c *Client) withKeyAddr(key CacheKey, fn func(net.Addr) error) (err error) {
	if !legalKey(key.String()) {
		return ErrMalformedKey
	}
	addr, err := c.getAddr(key)
	if err != nil {
		return err
	}
	return fn(addr)
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*conn)
	}
	freelist := c.freeconn[addr.String()]
	if len(freelist) >= c.getMaxIdleConns() {
		cn.nc.Close()
		return
	}
	c.freeconn[addr.String()] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr.String()] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	return c.timeout
}

func (c *Client) getMaxIdleConns() int {
	return c.maxIdleConns
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	nc, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		if err := cn.extendDeadline(); err != nil {
			return nil, err
		}
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:    c,
	}
	if err := cn.extendDeadline(); err != nil {
		return nil, err
	}
	return cn, nil
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() error {
	return cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}
