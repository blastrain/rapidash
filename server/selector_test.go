package server

import (
	"fmt"
	"net"
	"testing"
)

func SelectorTestSetup() {}

func SelectorTestTeardown() {}

func TestNewSelector(t *testing.T) {
	tests := []struct {
		expectedSelector *Selector
		expectedError    error
	}{
		{
			&Selector{
				ring: NewHashring([]net.Addr{cacheAddrServer1}),
			},
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestNewSelector:%v\n", i), func(t *testing.T) {
			actual, err := NewSelector(Server1)
			Equal(t, tt.expectedSelector, actual)
			Equal(t, tt.expectedError, err)
		})
	}
}

func TestSelectorPickServer(t *testing.T) {
	tests := []struct {
		s             *Selector
		expectedAddr  net.Addr
		expectedError error
	}{
		{
			&Selector{
				ring: NewHashring([]net.Addr{cacheAddrServer1}),
			},
			cacheAddrServer1,
			nil,
		},
		{
			&Selector{
				ring: NewHashring([]net.Addr{}),
			},
			nil,
			ErrCannotAssignCacheServer,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestSelectorPickServer:%v\n", i), func(t *testing.T) {
			actual, err := tt.s.PickServer(key)
			Equal(t, tt.expectedAddr, actual)
			Equal(t, tt.expectedError, err)
		})
	}
}

func TestSelectorEach(t *testing.T) {
	tests := []struct {
		s             *Selector
		expectedError error
	}{
		{
			&Selector{
				ring: NewHashring([]net.Addr{cacheAddrServer1}),
			},
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestSelectorEach:%v\n", i), func(t *testing.T) {
			err := tt.s.Each(func(addr net.Addr) error {
				return nil
			})
			Equal(t, tt.expectedError, err)
		})
	}
}
