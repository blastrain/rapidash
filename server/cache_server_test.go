package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"unsafe"
)

const (
	Server1 = "127.0.0.1:11211"
	Server2 = "127.0.0.1:11212"
)

var (
	key              StringCacheKey
	keys             []CacheKey
	valueString      string
	valueByteSlice   []byte
	item             *Item
	cacheAddrServer1 *cacheAddr
	cacheAddrServer2 *cacheAddr
	cte              *ConnectTimeoutError
)

type TestSlcCacheKey struct {
	key  string
	hash uint32
	typ  CacheKeyType
	addr net.Addr
}

func (c *TestSlcCacheKey) String() string {
	return c.key
}

func (c *TestSlcCacheKey) Hash() uint32 {
	return c.hash
}

func (c *TestSlcCacheKey) Addr() net.Addr {
	return c.addr
}

func (c *TestSlcCacheKey) LockKey() CacheKey {
	return &TestSlcCacheKey{
		key:  fmt.Sprintf("%s/lock", c.key),
		hash: c.hash,
		typ:  c.typ,
		addr: c.addr,
	}
}

func (c *TestSlcCacheKey) Type() CacheKeyType {
	if c.typ == CacheKeyTypeNone {
		return CacheKeyTypeSLC
	}
	return c.typ
}

func TestMain(m *testing.M) {
	CacheServerTestSetup()
	SelectorTestSetup()
	MemcacheTestSetup()
	RedisTestSetup()

	code := m.Run()

	RedisTestTeardown()
	MemcacheTestTeardown()
	SelectorTestTeardown()
	CacheServerTestTeardown()

	os.Exit(code)
}

func CacheServerTestSetup() {
	key = "key1"

	keys = []CacheKey{key}

	valueByteSlice = *(*[]byte)(unsafe.Pointer(&valueString))

	item = &Item{
		Key:   key,
		Value: valueByteSlice,
	}

	//iterator = &Iterator{
	//	currentIndex: -1,
	//	keys:         keys,
	//	values:       make([]*CacheGetResponse, len(keys)),
	//	errs:         make([]error, len(keys)),
	//}

	tcpaddr1, err := net.ResolveTCPAddr("tcp", Server1)
	if err != nil {
		panic(err)
	}

	tcpaddr2, err := net.ResolveTCPAddr("tcp", Server2)
	if err != nil {
		panic(err)
	}

	cacheAddrServer1 = &cacheAddr{
		ntw: tcpaddr1.Network(),
		str: tcpaddr1.String(),
	}

	cacheAddrServer2 = &cacheAddr{
		ntw: tcpaddr2.Network(),
		str: tcpaddr2.String(),
	}

	cte = &ConnectTimeoutError{
		cacheAddrServer1,
	}
}

func CacheServerTestTeardown() {}

func TestResumableError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{
			ErrMemcacheCacheMiss,
			true,
		},
		{
			ErrMemcacheCASConflict,
			true,
		},
		{
			ErrMemcacheNotStored,
			true,
		},
		{
			ErrMalformedKey,
			true,
		},
		{
			errors.New("other error"),
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestResumableError:%v\n", i), func(t *testing.T) {
			actual := resumableError(tt.err)
			Equal(t, tt.expected, actual)
		})
	}
}

func TestStringCacheKeyString(t *testing.T) {
	tests := []struct {
		expected string
	}{
		{
			"key1",
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestStringCacheKeyString:%v\n", i), func(t *testing.T) {
			actual := key.String()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestStringCacheKeyHash(t *testing.T) {
	tests := []struct {
		expected uint32
	}{
		{
			0,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestStringCacheKeyHash:%v\n", i), func(t *testing.T) {
			actual := key.Hash()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestStringCachKeyLockKey(t *testing.T) {
	tests := []struct {
		expected CacheKey
	}{
		{
			StringCacheKey("key1"),
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestStringCachKeyLockKey:%v\n", i), func(t *testing.T) {
			actual := key.LockKey()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestStringCachKeyType(t *testing.T) {
	tests := []struct {
		expected CacheKeyType
	}{
		{
			CacheKeyTypeNone,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestStringCachKeyType:%v\n", i), func(t *testing.T) {
			actual := key.Type()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestStringCachKeyAddr(t *testing.T) {
	tests := []struct {
		expected net.Addr
	}{
		{
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestStringCachKeyAddr:%v\n", i), func(t *testing.T) {
			actual := key.Addr()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestConnectTimeoutErrorError(t *testing.T) {
	tests := []struct {
		expected string
	}{
		{
			"memcache: connect timeout to 127.0.0.1:11211",
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestConnectTimeoutErrorError:%v\n", i), func(t *testing.T) {
			actual := cte.Error()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestCacheKeyTypeString(t *testing.T) {
	tests := []struct {
		typ      CacheKeyType
		expected string
	}{
		{
			CacheKeyTypeSLC,
			"slc",
		},
		{
			CacheKeyTypeLLC,
			"llc",
		},
		{
			CacheKeyTypeNone,
			"none",
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestCacheKeyTypeString:%v\n", i), func(t *testing.T) {
			actual := tt.typ.String()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestCacheKeyTypeMarshalJSON(t *testing.T) {
	tests := []struct {
		typ               CacheKeyType
		expectedByteSlice []byte
		expectedError     error
	}{
		{
			CacheKeyTypeSLC,
			[]byte(`"slc"`),
			nil,
		},
		{
			CacheKeyTypeLLC,
			[]byte(`"llc"`),
			nil,
		},
		{
			CacheKeyTypeNone,
			[]byte(`"none"`),
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestCacheKeyTypeMarshalJSON:%v\n", i), func(t *testing.T) {
			actual, err := tt.typ.MarshalJSON()
			Equal(t, tt.expectedByteSlice, actual)
			Equal(t, tt.expectedError, err)
		})
	}
}

func TestCacheKeyTypeUnmarshalJSON(t *testing.T) {
	tests := []struct {
		typ           CacheKeyType
		byteSlice     []byte
		expectedError error
		expectedTyp   CacheKeyType
	}{
		{
			CacheKeyTypeSLC,
			[]byte(`"slc"`),
			nil,
			CacheKeyTypeSLC,
		},
		{
			CacheKeyTypeSLC,
			[]byte(`"llc"`),
			nil,
			CacheKeyTypeLLC,
		},
		{
			CacheKeyTypeSLC,
			[]byte(`"none"`),
			nil,
			CacheKeyTypeNone,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestCacheKeyTypeUnmarshalJSON:%v\n", i), func(t *testing.T) {
			err := tt.typ.UnmarshalJSON(tt.byteSlice)
			Equal(t, tt.expectedError, err)
			Equal(t, tt.expectedTyp, tt.expectedTyp)
		})
	}
}

func TestIteratorSetContent(t *testing.T) {
	tests := []struct {
		iterator *Iterator
		res      *CacheGetResponse
		expected *Iterator
	}{
		{
			&Iterator{
				currentIndex: -1,
				keys:         keys,
				values:       make([]*CacheGetResponse, len(keys)),
				errs:         make([]error, len(keys)),
			},
			&CacheGetResponse{
				Value: item.Value,
				Flags: item.Flags,
			},
			&Iterator{
				currentIndex: -1,
				keys:         keys,
				values: []*CacheGetResponse{
					{
						Value: item.Value,
						Flags: item.Flags,
					},
				},
				errs: make([]error, len(keys)),
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestIteratorSetContent:%v\n", i), func(t *testing.T) {
			tt.iterator.SetContent(0, tt.res)
		})
		Equal(t, tt.expected, tt.iterator)
	}
}

func TestIteratorSetError(t *testing.T) {
	tests := []struct {
		iterator *Iterator
		res      *CacheGetResponse
		expected *Iterator
	}{
		{
			&Iterator{
				currentIndex: -1,
				keys:         keys,
				values:       make([]*CacheGetResponse, len(keys)),
				errs:         make([]error, len(keys)),
			},
			&CacheGetResponse{
				Value: item.Value,
				Flags: item.Flags,
			},
			&Iterator{
				currentIndex: -1,
				keys:         keys,
				values:       make([]*CacheGetResponse, len(keys)),
				errs:         []error{ErrCacheMiss},
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestIteratorSetError:%v\n", i), func(t *testing.T) {
			tt.iterator.SetError(0, ErrCacheMiss)
		})
		Equal(t, tt.expected, tt.iterator)
	}
}

func TestNewIterator(t *testing.T) {
	tests := []struct {
		expected *Iterator
	}{
		{
			&Iterator{
				currentIndex: -1,
				keys:         keys,
				values:       make([]*CacheGetResponse, len(keys)),
				errs:         make([]error, len(keys)),
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestNewIterator:%v\n", i), func(t *testing.T) {
			actual := NewIterator(keys)
			Equal(t, tt.expected, actual)
		})
	}
}

func TestIteratorNext(t *testing.T) {
	tests := []struct {
		iterator *Iterator
		expected bool
	}{
		{
			&Iterator{
				currentIndex: -1,
				keys:         keys,
				values: []*CacheGetResponse{
					{
						Value: item.Value,
						Flags: item.Flags,
					},
				},
				errs: []error{ErrCacheMiss},
			},
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestIteratorNext:%v\n", i), func(t *testing.T) {
			actual := tt.iterator.Next()
			Equal(t, tt.expected, actual)

			tt.expected = false
			actual = tt.iterator.Next()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestIteratorKey(t *testing.T) {
	tests := []struct {
		iterator *Iterator
		expected CacheKey
	}{
		{
			&Iterator{
				currentIndex: 0,
				keys:         keys,
				values: []*CacheGetResponse{
					{
						Value: item.Value,
						Flags: item.Flags,
					},
				},
				errs: []error{ErrCacheMiss},
			},
			key,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestIteratorKey:%v\n", i), func(t *testing.T) {
			actual := tt.iterator.Key()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestIteratorContent(t *testing.T) {
	tests := []struct {
		iterator *Iterator
		expected *CacheGetResponse
	}{
		{
			&Iterator{
				currentIndex: 0,
				keys:         keys,
				values: []*CacheGetResponse{
					{
						Value: item.Value,
						Flags: item.Flags,
					},
				},
				errs: []error{ErrCacheMiss},
			},
			&CacheGetResponse{
				Value: item.Value,
				Flags: item.Flags,
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestIteratorContent:%v\n", i), func(t *testing.T) {
			actual := tt.iterator.Content()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestIteratorError(t *testing.T) {
	tests := []struct {
		iterator *Iterator
		expected error
	}{
		{
			&Iterator{
				currentIndex: 0,
				keys:         keys,
				values: []*CacheGetResponse{
					{
						Value: item.Value,
						Flags: item.Flags,
					},
				},
				errs: []error{ErrCacheMiss},
			},
			ErrCacheMiss,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestIteratorError:%v\n", i), func(t *testing.T) {
			actual := tt.iterator.Error()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestLegalKey(t *testing.T) {
	tests := []struct {
		key      string
		expected bool
	}{
		{
			"key1",
			true,
		},
		{
			"12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901",
			false,
		},
		{
			string(0x00),
			false,
		},
		{
			string(0x7f),
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestLegalKey:%v\n", i), func(t *testing.T) {
			actual := legalKey(tt.key)
			Equal(t, tt.expected, actual)
		})
	}
}

func TestAddSecondLevelCacheServers(t *testing.T) {
	tests := []struct {
		client         *Client
		expectedError  error
		expectedClient *Client
	}{
		{
			&Client{
				slcSelector: &Selector{
					ring: NewHashring([]net.Addr{cacheAddrServer1}),
				},
				llcSelector: &Selector{
					ring: NewHashring([]net.Addr{cacheAddrServer1}),
				},
			},
			nil,
			&Client{
				slcSelector: &Selector{
					ring: &Hashring{
						addrs: []net.Addr{
							&cacheAddr{
								ntw: cacheAddrServer2.Network(),
								str: cacheAddrServer2.String(),
							},
							&cacheAddr{
								ntw: cacheAddrServer1.Network(),
								str: cacheAddrServer1.String(),
							},
						},
					},
				},
				llcSelector: &Selector{
					ring: &Hashring{
						addrs: []net.Addr{
							&cacheAddr{
								ntw: cacheAddrServer1.Network(),
								str: cacheAddrServer1.String(),
							},
						},
					},
				},
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestLegalKey:%v\n", i), func(t *testing.T) {
			err := tt.client.AddSecondLevelCacheServers(Server2)
			Equal(t, tt.expectedError, err)
		})
	}
}
