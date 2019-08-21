package server

import (
	"net"
	"strings"
	"sync"

	"golang.org/x/xerrors"
)

var (
	ErrCannotAssignCacheServer = xerrors.New("cannot assign cache server because server number is zero")
)

type Selector struct {
	mu   sync.RWMutex
	ring *Hashring
}

// cacheAddr caches the Network() and String() values from any net.Addr.
type cacheAddr struct {
	ntw string
	str string
}

func newCacheAddr(a net.Addr) net.Addr {
	return &cacheAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (c *cacheAddr) Network() string { return c.ntw }
func (c *cacheAddr) String() string  { return c.str }

func getAddr(server string) (net.Addr, error) {
	if strings.Contains(server, "/") {
		addr, err := net.ResolveUnixAddr("unix", server)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve unix addr %s: %w", server, err)
		}
		return newCacheAddr(addr), nil
	}
	tcpaddr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve tcp addr %s: %w", server, err)
	}
	return newCacheAddr(tcpaddr), nil
}

func NewSelector(servers ...string) (*Selector, error) {
	addrs := make([]net.Addr, len(servers))
	for idx, server := range servers {
		addr, err := getAddr(server)
		if err != nil {
			return nil, xerrors.Errorf("failed to get addr: %w", err)
		}
		addrs[idx] = addr
	}
	return &Selector{ring: NewHashring(addrs)}, nil
}

func (s *Selector) PickServer(key CacheKey) (net.Addr, error) {
	if len(s.ring.addrs) == 0 {
		return nil, ErrCannotAssignCacheServer
	}
	return s.ring.Get(key), nil
}

func (s *Selector) Each(f func(net.Addr) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.ring.addrs {
		if err := f(a); nil != err {
			return xerrors.Errorf("failed to select server: %w", err)
		}
	}
	return nil
}
