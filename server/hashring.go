package server

import (
	"fmt"
	"hash/crc32"
	"net"
	"sort"

	"golang.org/x/xerrors"
)

type Hashring struct {
	addrs      []net.Addr
	sortedKeys []HashKey
	keyToAddr  map[HashKey]net.Addr
}

type HashKey uint32

type HashKeyOrder []HashKey

func (h HashKeyOrder) Len() int           { return len(h) }
func (h HashKeyOrder) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h HashKeyOrder) Less(i, j int) bool { return h[i] < h[j] }

func NewHashring(addrs []net.Addr) *Hashring {
	ring := &Hashring{
		addrs:      addrs,
		sortedKeys: make([]HashKey, 0),
		keyToAddr:  map[HashKey]net.Addr{},
	}
	ring.setupRing()
	return ring
}

func (h *Hashring) setupRing() {
	virtualNodeNum := 30
	for _, addr := range h.addrs {
		for i := 0; i <= virtualNodeNum; i++ {
			node := addr.String() + "-" + fmt.Sprint(i)
			key := HashKey(crc32.ChecksumIEEE([]byte(node)))
			h.keyToAddr[key] = addr
			h.sortedKeys = append(h.sortedKeys, key)
		}
	}
	sort.Sort(HashKeyOrder(h.sortedKeys))
}

func (h *Hashring) keyIndex(key HashKey) int {
	keys := h.sortedKeys
	idx := sort.Search(len(keys), func(i int) bool {
		return keys[i] > key
	})
	if idx == len(keys) {
		return 0
	}
	return idx
}

func (h *Hashring) cacheKeyToNode(key CacheKey) net.Addr {
	addr := key.Addr()
	if addr != nil {
		return addr
	}
	idx := h.keyIndex(HashKey(key.Hash()))
	nodeKey := h.sortedKeys[idx]
	return h.keyToAddr[nodeKey]
}

func (h *Hashring) Get(key CacheKey) net.Addr {
	return h.cacheKeyToNode(key)
}

func (h *Hashring) Add(node net.Addr) *Hashring {
	ring := &Hashring{
		addrs:      []net.Addr{node},
		sortedKeys: make([]HashKey, 0),
		keyToAddr:  map[HashKey]net.Addr{},
	}
	ring.addrs = append(ring.addrs, h.addrs...)
	ring.setupRing()
	return ring
}

func (h *Hashring) Remove(node net.Addr) (*Hashring, error) {
	newAddrs := []net.Addr{}
	prevLen := len(h.addrs)
	for _, addr := range h.addrs {
		if addr.String() == node.String() {
			continue
		}
		newAddrs = append(newAddrs, addr)
	}
	if len(newAddrs) == prevLen {
		return nil, xerrors.Errorf("cannot remove node %s", node)
	}
	ring := &Hashring{
		addrs:      newAddrs,
		sortedKeys: make([]HashKey, 0),
		keyToAddr:  map[HashKey]net.Addr{},
	}
	ring.setupRing()
	return ring, nil
}
