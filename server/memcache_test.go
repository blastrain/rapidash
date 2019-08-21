package server

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/xerrors"
)

const (
	MemcachedServer1 = "127.0.0.1:11211"
)

var (
	memcachedSelector    *Selector
	memcachedClient      *Client
	memcachedCacheServer CacheServer
)

func MemcacheTestSetup() {
	var err error

	memcachedSelector, err = NewSelector(MemcachedServer1)
	if err != nil {
		panic(err)
	}

	memcachedClient = &Client{slcSelector: memcachedSelector, llcSelector: memcachedSelector}
	memcachedCacheServer = &MemcachedClient{client: memcachedClient}
	if err := memcachedCacheServer.SetTimeout(100 * time.Millisecond); err != nil {
		panic(err)
	}
	if err := memcachedCacheServer.SetMaxIdleConnections(2); err != nil {
		panic(err)
	}

	for i := 1; i <= 3; i++ {
		value := fmt.Sprintf("value%d", i)
		if err := memcachedCacheServer.Set(
			&CacheStoreRequest{
				Key: &TestSlcCacheKey{
					key: fmt.Sprintf("key%d", i),
				},
				Value: *(*[]byte)(unsafe.Pointer(&value)),
			},
		); err != nil {
			panic(err)
		}
	}

}

func MemcacheTestTeardown() {
	if err := memcachedCacheServer.Flush(); err != nil {
		panic(err)
	}
}

func TestNewMemcachedBySelectors(t *testing.T) {
	tests := []struct {
		expected CacheServer
	}{
		{
			expected: &MemcachedClient{client: &Client{slcSelector: memcachedSelector, llcSelector: memcachedSelector}},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestNewMemcachedBySelectors:%v\n", i), func(t *testing.T) {
			actual := NewMemcachedBySelectors(memcachedSelector, memcachedSelector)
			Equal(t, tt.expected, actual)
		})
	}
}

func TestMemcachedGetClient(t *testing.T) {
	tests := []struct {
		expected *Client
	}{
		{
			expected: memcachedClient,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedGetClient:%v\n", i), func(t *testing.T) {
			actual := memcachedCacheServer.GetClient()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestMemcachedSetTimeout(t *testing.T) {
	tests := []struct {
		timeout     time.Duration
		expected    time.Duration
		expectedErr error
	}{
		{
			timeout:  100 * time.Millisecond,
			expected: 100 * time.Millisecond,
		},
		{
			timeout:     0,
			expectedErr: ErrSetTimeout,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedSetTimeout:%v\n", i), func(t *testing.T) {
			err := memcachedCacheServer.SetTimeout(tt.timeout)
			Equal(t, tt.expectedErr, err)
			if err == nil {
				actual := memcachedCacheServer.GetClient().timeout
				Equal(t, tt.expected, actual)
			}
		})
	}
}

func TestMemcachedSetMaxIdleConnections(t *testing.T) {
	tests := []struct {
		maxIdle     int
		expected    int
		expectedErr error
	}{
		{
			maxIdle:  1000,
			expected: 1000,
		},
		{
			maxIdle:     0,
			expectedErr: ErrSetMaxIdleConnections,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedSetMaxIdleConnections:%v\n", i), func(t *testing.T) {
			err := memcachedCacheServer.SetMaxIdleConnections(tt.maxIdle)
			Equal(t, tt.expectedErr, err)
			if err == nil {
				actual := memcachedCacheServer.GetClient().maxIdleConns
				Equal(t, tt.expected, actual)
			}
		})
	}
}

func TestMemcachedGet(t *testing.T) {
	tests := []struct {
		cacheKey      *TestSlcCacheKey
		cacheValue    string
		expected      *CacheGetResponse
		expectedError error
	}{
		{
			cacheKey: &TestSlcCacheKey{
				key: "key1",
			},
			cacheValue: "value1",
			expected:   &CacheGetResponse{},
		},
		{
			cacheKey: &TestSlcCacheKey{
				key: "cachemiss",
			},
			expected:      &CacheGetResponse{},
			expectedError: ErrCacheMiss,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedGet:%v\n", i), func(t *testing.T) {
			tt.expected.Value = *(*[]byte)(unsafe.Pointer(&tt.cacheValue))
			actual, err := memcachedCacheServer.Get(tt.cacheKey)
			if tt.cacheKey.key == "cachemiss" {
				Equal(t, tt.expectedError, err)
			} else {
				Equal(t, tt.expected.Value, actual.Value)
			}
		})
	}
}

func TestMemcachedGetMulti(t *testing.T) {
	tests := []struct {
		cacheKeys     []CacheKey
		cacheValues   []string
		cacheErrors   []error
		expected      *Iterator
		expectedError error
	}{
		{
			cacheKeys: []CacheKey{
				&TestSlcCacheKey{key: "key1"},
				&TestSlcCacheKey{key: "key2"},
				&TestSlcCacheKey{key: "key3"},
			},
			cacheValues: []string{"value1", "value2", "value3"},
		},
		{
			cacheKeys: []CacheKey{
				&TestSlcCacheKey{key: "cachemiss1"},
				&TestSlcCacheKey{key: "cachemiss2"},
				&TestSlcCacheKey{key: "cachemiss3"},
			},
			cacheErrors: []error{ErrCacheMiss, ErrCacheMiss, ErrCacheMiss},
		},
		{
			cacheKeys: []CacheKey{
				&TestSlcCacheKey{key: "key1"},
				&TestSlcCacheKey{key: "key2"},
				&TestSlcCacheKey{key: "cachemiss"},
			},
			cacheValues: []string{"value1", "value2", ""},
			cacheErrors: []error{nil, nil, ErrCacheMiss},
		},
		{
			cacheKeys: []CacheKey{
				&TestSlcCacheKey{key: string(0x00)},
			},
			expectedError: ErrMalformedKey,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedGetMulti:%v\n", i), func(t *testing.T) {
			tt.expected = NewIterator(tt.cacheKeys)
			for i := 0; i < len(tt.cacheValues); i++ {
				tt.expected.SetContent(i, &CacheGetResponse{Value: *(*[]byte)(unsafe.Pointer(&tt.cacheValues[i]))})
			}
			for i := 0; i < len(tt.cacheErrors); i++ {
				tt.expected.SetError(i, tt.cacheErrors[i])
			}

			actual, err := memcachedCacheServer.GetMulti(tt.cacheKeys)
			if tt.cacheKeys[0].String() == string(0x00) {
				if !xerrors.Is(err, tt.expectedError) {
					t.Fatalf("%+v", err)
				}
			} else {
				for actual.Next() {
					tt.expected.Next()

					if err := actual.Error(); err != nil {
						if !xerrors.Is(err, tt.expected.Error()) {
							t.Fatalf("%+v", err)
						}
						continue
					}

					content := actual.Content()
					expectedContent := tt.expected.Content()
					Equal(t, expectedContent.Value, content.Value)
				}
			}
		})
	}
}

func TestMemcachedSet(t *testing.T) {
	tests := []struct {
		cacheStoreRequest *CacheStoreRequest
		requestValue      string
		expectedError     error
		expectedGetValue  string
	}{
		{
			cacheStoreRequest: &CacheStoreRequest{
				Key: &TestSlcCacheKey{
					key: "TestMemcachedSet",
				},
			},
			requestValue:     "value",
			expectedGetValue: "value",
		},
		{
			cacheStoreRequest: &CacheStoreRequest{
				Key: &TestSlcCacheKey{
					key: string(0x00),
				},
			},
			expectedError: xerrors.Errorf("failed set value to %s: %w", string(0x00), ErrMalformedKey),
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedSet:%v\n", i), func(t *testing.T) {
			tt.cacheStoreRequest.Value = *(*[]byte)(unsafe.Pointer(&tt.requestValue))
			err := memcachedCacheServer.Set(tt.cacheStoreRequest)
			if tt.cacheStoreRequest.Key.String() == string(0x00) {
				Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				Equal(t, tt.expectedError, err)

				reply, err := memcachedCacheServer.Get(tt.cacheStoreRequest.Key)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				Equal(t, tt.expectedGetValue, *(*string)(unsafe.Pointer(&reply.Value)))

				tt.cacheStoreRequest.CasID = reply.CasID
				err = memcachedCacheServer.Set(tt.cacheStoreRequest)
				Equal(t, tt.expectedError, err)

				tt.cacheStoreRequest.CasID = reply.CasID
				tt.expectedError = xerrors.Errorf("failed set value to %s: %w", tt.cacheStoreRequest.Key.String(), ErrMemcacheCASConflict)
				err = memcachedCacheServer.Set(tt.cacheStoreRequest)
				Equal(t, tt.expectedError.Error(), err.Error())
			}
		})
	}
}

func TestMemcachedAdd(t *testing.T) {
	tests := []struct {
		key              CacheKey
		value            string
		expiration       time.Duration
		expectedError    error
		expectedGetValue string
	}{
		{
			key: &TestSlcCacheKey{
				key: "TestMemcachedAdd",
			},
			value:            "value",
			expiration:       10,
			expectedGetValue: "value",
		},
		{
			key: &TestSlcCacheKey{
				key: string(0x00),
			},
			expectedError: xerrors.Errorf("failed add value to %s: %w", string(0x00), ErrMalformedKey),
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedAdd:%v\n", i), func(t *testing.T) {
			err := memcachedCacheServer.Add(tt.key, *(*[]byte)(unsafe.Pointer(&tt.value)), tt.expiration)
			if tt.key.String() == string(0x00) {
				Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				Equal(t, tt.expectedError, err)

				reply, err := memcachedCacheServer.Get(tt.key)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				Equal(t, tt.expectedGetValue, *(*string)(unsafe.Pointer(&reply.Value)))
			}
		})
	}
}

func TestMemcachedDelete(t *testing.T) {
	tests := []struct {
		cacheStoreRequest *CacheStoreRequest
		requestValue      string
		expected          error
	}{
		{
			cacheStoreRequest: &CacheStoreRequest{
				Key: &TestSlcCacheKey{
					key: "TestMemcachedDelete",
				},
			},
			requestValue: "value",
			expected:     nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedDelete:%v\n", i), func(t *testing.T) {
			tt.cacheStoreRequest.Value = *(*[]byte)(unsafe.Pointer(&tt.requestValue))
			err := memcachedCacheServer.Set(tt.cacheStoreRequest)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			// fisrt time
			err = memcachedCacheServer.Delete(tt.cacheStoreRequest.Key)
			Equal(t, tt.expected, err)

			// second time
			err = memcachedCacheServer.Delete(tt.cacheStoreRequest.Key)
			Equal(t, tt.expected, err)
		})
	}
}

func TestMemcachedFlush(t *testing.T) {
	tests := []struct {
		expected error
	}{
		{
			expected: nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestMemcachedFlush:%v\n", i), func(t *testing.T) {
			err := memcachedCacheServer.Flush()
			Equal(t, tt.expected, err)
		})
	}
}
