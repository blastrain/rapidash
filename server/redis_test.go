package server

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/xerrors"
)

const (
	RedisServer1 = "127.0.0.1:6379"
)

var (
	redisSelector    *Selector
	redisClient      *Client
	redisCacheServer CacheServer
)

func RedisTestSetup() {
	var err error

	redisSelector, err = NewSelector(RedisServer1)
	if err != nil {
		panic(err)
	}

	redisClient = &Client{slcSelector: redisSelector, llcSelector: redisSelector}
	redisCacheServer = &RedisClient{client: redisClient}
	if err := redisCacheServer.SetTimeout(100 * time.Millisecond); err != nil {
		panic(err)
	}
	if err := redisCacheServer.SetMaxIdleConnections(2); err != nil {
		panic(err)
	}

	for i := 1; i <= 3; i++ {
		value := fmt.Sprintf("value%d", i)
		if err := redisCacheServer.Set(
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

func RedisTestTeardown() {
	if err := redisCacheServer.Flush(); err != nil {
		panic(err)
	}
}

func TestNewRedisBySelectors(t *testing.T) {
	tests := []struct {
		expected CacheServer
	}{
		{
			expected: &RedisClient{client: &Client{slcSelector: redisSelector, llcSelector: redisSelector}},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestNewRedisBySelectors:%v\n", i), func(t *testing.T) {
			actual := NewRedisBySelectors(redisSelector, redisSelector)
			Equal(t, tt.expected, actual)
		})
	}
}

func TestGetClient(t *testing.T) {
	tests := []struct {
		expected *Client
	}{
		{
			expected: redisClient,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestRedisGetClient:%v\n", i), func(t *testing.T) {
			actual := redisCacheServer.GetClient()
			Equal(t, tt.expected, actual)
		})
	}
}

func TestRedisSetTimeout(t *testing.T) {
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
		t.Run(fmt.Sprintf("TestRedisSetTimeout:%v\n", i), func(t *testing.T) {
			err := redisCacheServer.SetTimeout(tt.timeout)
			Equal(t, tt.expectedErr, err)
			if err == nil {
				actual := redisCacheServer.GetClient().timeout
				Equal(t, tt.expected, actual)
			}
		})
	}
}

func TestRedisSetMaxIdleConnections(t *testing.T) {
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
			err := redisCacheServer.SetMaxIdleConnections(tt.maxIdle)
			Equal(t, tt.expectedErr, err)
			if err == nil {
				actual := redisCacheServer.GetClient().maxIdleConns
				Equal(t, tt.expected, actual)
			}
		})
	}
}

func TestRedisGet(t *testing.T) {
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
		t.Run(fmt.Sprintf("TestRedisGet:%v\n", i), func(t *testing.T) {
			tt.expected.Value = *(*[]byte)(unsafe.Pointer(&tt.cacheValue))
			actual, err := redisCacheServer.Get(tt.cacheKey)
			if tt.cacheKey.key == "cachemiss" {
				Equal(t, tt.expectedError, err)
			} else {
				Equal(t, tt.expected.Value, actual.Value)
			}
		})
	}
}

func TestRedisGetMulti(t *testing.T) {
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
		t.Run(fmt.Sprintf("TestRedisGetMulti:%v\n", i), func(t *testing.T) {
			tt.expected = NewIterator(tt.cacheKeys)
			for i := 0; i < len(tt.cacheValues); i++ {
				tt.expected.SetContent(i, &CacheGetResponse{Value: *(*[]byte)(unsafe.Pointer(&tt.cacheValues[i]))})
			}
			for i := 0; i < len(tt.cacheErrors); i++ {
				tt.expected.SetError(i, tt.cacheErrors[i])
			}

			actual, err := redisCacheServer.GetMulti(tt.cacheKeys)
			if err == nil {
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
			} else if !xerrors.Is(err, tt.expectedError) {
				t.Fatalf("%+v", err)
			}
		})
	}
}

func TestRedisSet(t *testing.T) {
	tests := []struct {
		cacheStoreRequest *CacheStoreRequest
		requestValue      string
		expectedError     error
		expectedGetValue  string
	}{
		{
			cacheStoreRequest: &CacheStoreRequest{
				Key: &TestSlcCacheKey{
					key: "TestRedisSet",
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
		t.Run(fmt.Sprintf("TesRedisSet:%v\n", i), func(t *testing.T) {
			tt.cacheStoreRequest.Value = *(*[]byte)(unsafe.Pointer(&tt.requestValue))
			err := redisCacheServer.Set(tt.cacheStoreRequest)
			if tt.cacheStoreRequest.Key.String() == string(0x00) {
				Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				Equal(t, tt.expectedError, err)

				reply, err := redisCacheServer.Get(tt.cacheStoreRequest.Key)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				Equal(t, tt.expectedGetValue, *(*string)(unsafe.Pointer(&reply.Value)))
			}
		})
	}
}

func TestRedisAdd(t *testing.T) {
	tests := []struct {
		key              CacheKey
		value            string
		expiration       time.Duration
		expectedError    error
		expectedGetValue string
	}{
		{
			key: &TestSlcCacheKey{
				key: "TestRedisAdd",
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
		t.Run(fmt.Sprintf("TestRedisAdd:%v\n", i), func(t *testing.T) {
			err := redisCacheServer.Add(tt.key, *(*[]byte)(unsafe.Pointer(&tt.value)), tt.expiration)
			if tt.key.String() == string(0x00) {
				Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				Equal(t, tt.expectedError, err)

				reply, err := redisCacheServer.Get(tt.key)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				Equal(t, tt.expectedGetValue, *(*string)(unsafe.Pointer(&reply.Value)))
			}
		})
	}
}

func TestRedisDelete(t *testing.T) {
	tests := []struct {
		cacheStoreRequest *CacheStoreRequest
		requestValue      string
		expected          error
	}{
		{
			cacheStoreRequest: &CacheStoreRequest{
				Key: &TestSlcCacheKey{
					key: "TestRedisDelete",
				},
			},
			requestValue: "value",
			expected:     nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestRedisDelete:%v\n", i), func(t *testing.T) {
			tt.cacheStoreRequest.Value = *(*[]byte)(unsafe.Pointer(&tt.requestValue))
			err := redisCacheServer.Set(tt.cacheStoreRequest)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			// fisrt time
			err = redisCacheServer.Delete(tt.cacheStoreRequest.Key)
			Equal(t, tt.expected, err)

			// second time
			err = redisCacheServer.Delete(tt.cacheStoreRequest.Key)
			Equal(t, tt.expected, err)
		})
	}
}

func TestRedisFlush(t *testing.T) {
	tests := []struct {
		expected error
	}{
		{
			expected: nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestRedisFlush:%v\n", i), func(t *testing.T) {
			err := redisCacheServer.Flush()
			Equal(t, tt.expected, err)
		})
	}
}
