package server

import (
	"fmt"
	"hash/crc32"
	"net"
	"testing"
)

type TestAddr string

func (a TestAddr) Network() string {
	return "tcp"
}

func (a TestAddr) String() string {
	return string(a)
}

type addrItem struct {
	prevAddr string
	curAddr  string
}

func (i addrItem) isEqual() bool {
	return i.prevAddr == i.curAddr
}

type testCacheKey uint64

func (c testCacheKey) String() string {
	return fmt.Sprint(uint64(c))
}

func (c testCacheKey) Hash() uint32 {
	return crc32.ChecksumIEEE([]byte(fmt.Sprint(uint64(c))))
}

func (c testCacheKey) LockKey() CacheKey {
	return c
}

func (c testCacheKey) Type() CacheKeyType {
	return CacheKeyTypeNone
}

func (c testCacheKey) Addr() net.Addr {
	return nil
}

func TestHashring(t *testing.T) {
	addrs := []net.Addr{
		TestAddr("127.0.0.1:11211"),
		TestAddr("127.0.0.1:11212"),
		TestAddr("127.0.0.1:11213"),
		TestAddr("127.0.0.1:11214"),
		TestAddr("127.0.0.1:11215"),
	}
	ring := NewHashring(addrs)
	addrCount := map[string]int{}
	items := []addrItem{}
	for i := 0; i < 1000; i++ {
		addr := ring.Get(testCacheKey(uint64(i))).String()
		addrCount[addr]++
		items = append(items, addrItem{
			prevAddr: addr,
		})
	}
	if addrCount["127.0.0.1:11211"] != 275 {
		t.Fatal("invalid addr count")
	}
	if addrCount["127.0.0.1:11212"] != 200 {
		t.Fatal("invalid addr count")
	}
	if addrCount["127.0.0.1:11213"] != 174 {
		t.Fatal("invalid addr count")
	}
	if addrCount["127.0.0.1:11214"] != 149 {
		t.Fatal("invalid addr count")
	}
	if addrCount["127.0.0.1:11215"] != 202 {
		t.Fatal("invalid addr count")
	}
	t.Run("add new node", func(t *testing.T) {
		ring := ring.Add(TestAddr("127.0.0.1:11216"))
		notEqualCount := 0
		for i := 0; i < 1000; i++ {
			addr := ring.Get(testCacheKey(uint64(i))).String()
			items[i].curAddr = addr
			if !items[i].isEqual() {
				notEqualCount++
			}
		}
		// 182 is near value of (total item num) / (node num)
		if notEqualCount != 182 {
			t.Fatal("cannot work consistent hashing")
		}
	})
	t.Run("remove node", func(t *testing.T) {
		ring, err := ring.Remove(TestAddr("127.0.0.1:11215"))
		if err != nil {
			t.Fatal(err)
		}
		notEqualCount := 0
		for i := 0; i < 1000; i++ {
			addr := ring.Get(testCacheKey(uint64(i))).String()
			items[i].curAddr = addr
			if !items[i].isEqual() {
				notEqualCount++
			}
		}
		// 202 is near value of (total item num) / (node num)
		if notEqualCount != 202 {
			t.Fatal("cannot work consistent hashing")
		}
	})
}
