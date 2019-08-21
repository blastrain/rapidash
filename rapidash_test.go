package rapidash

import (
	"fmt"
	"testing"
	"time"

	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

func TestServerChanging(t *testing.T) {
	t.Run("remove and add server", func(t *testing.T) {
		cache, err := New(ServerAddrs([]string{"localhost:11211"}), MaxIdleConnections(1000), Timeout(200*time.Millisecond))
		NoError(t, err)
		tx, err := cache.Begin()
		NoErrorf(t, err, "cannot begin cache transaction")
		NoErrorf(t, cache.RemoveServers("localhost:11211"), "cannot remove server")

		defer func() { NoErrorf(t, tx.Rollback(), "cannot rollback") }()
		Errorf(t, tx.Create("int", Int(1)), "create int cache")
		NoErrorf(t, cache.AddServers("localhost:11211"), "cannot add server")
		NoErrorf(t, tx.Create("int", Int(1)), "cannot create cache")
	})

	t.Run("remove and add only slc server", func(t *testing.T) {
		cache, err := New(ServerAddrs([]string{"localhost:11211"}), MaxIdleConnections(1000), Timeout(200*time.Millisecond))
		NoError(t, err)
		NoError(t, cache.WarmUp(conn, userLoginType(), false))
		tx, err := cache.Begin(conn)
		NoErrorf(t, err, "cannot begin cache transaction")
		NoErrorf(t, cache.RemoveSecondLevelCacheServers("localhost:11211"), "cannot remove server")

		builder := NewQueryBuilder("user_logins").Eq("id", uint64(1))
		var v UserLogin
		Errorf(t, tx.FindByQueryBuilder(builder, &v), "find slc cache")
		NoErrorf(t, tx.Create("int", Int(1)), "cannot create cache")

		defer func() { NoErrorf(t, tx.Rollback(), "cannot rollback") }()
		NoErrorf(t, cache.AddSecondLevelCacheServer("localhost:11211"), "cannot add server")
		NoErrorf(t, tx.FindByQueryBuilder(builder, &v), "find slc cache")
		NoErrorf(t, tx.Create("int", Int(1)), "cannot create cache")
	})

	t.Run("remove and add only llc server", func(t *testing.T) {
		cache, err := New(ServerAddrs([]string{"localhost:11211"}), MaxIdleConnections(1000), Timeout(200000000000))
		NoError(t, err)
		tx, err := cache.Begin()
		NoErrorf(t, err, "cannot begin cache transaction")
		NoErrorf(t, cache.RemoveLastLevelCacheServers("localhost:11211"), "cannot remove server")

		defer func() { NoErrorf(t, tx.Rollback(), "cannot rollback") }()
		Errorf(t, tx.Create("int", Int(1)), "create int cache")
		NoErrorf(t, cache.AddLastLevelCacheServer("localhost:11211"), "cannot add server")
		NoErrorf(t, tx.Create("int", Int(1)), "cannot create cache")
	})
}

func TestRecover(t *testing.T) {
	txConn, err := conn.Begin()
	NoError(t, err)
	builder := NewQueryBuilder("user_logins").Eq("id", uint64(1))
	var v UserLogin
	{
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		NoError(t, tx.FindByQueryBuilder(builder, &v))

		Equal(t, v.ID, uint64(1))
		Equal(t, v.UserID, uint64(1))

		NoError(t, tx.CommitCacheOnly())
	}

	{
		_, err := txConn.Exec(fmt.Sprintf("UPDATE user_logins SET user_id = %d WHERE id = %d", 10000, v.ID))
		NoError(t, err)
	}

	{
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		NoError(t, tx.FindByQueryBuilder(builder, &v))

		Equal(t, v.ID, uint64(1))
		Equal(t, v.UserID, uint64(1))

		NoError(t, tx.CommitCacheOnly())
	}

	queryLog := &QueryLog{
		Key:  "r/slc/user_logins/id#1",
		Type: server.CacheKeyTypeSLC,
		Addr: "localhost:11211",
	}
	NoError(t, cache.Recover([]*QueryLog{queryLog}))

	tx, err := cache.Begin(txConn)
	NoError(t, err)

	NoError(t, tx.FindByQueryBuilder(builder, &v))

	Equal(t, v.ID, uint64(1))
	Equal(t, v.UserID, uint64(10000))

	NoError(t, tx.Commit())
}

func TestSetTimeout(t *testing.T) {
	tests := []struct {
		timeout  time.Duration
		expected error
	}{
		{
			100 * time.Millisecond,
			nil,
		},
		{
			0,
			server.ErrSetTimeout,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestSetTimeout:%v\n", i), func(t *testing.T) {
			_, err := New(Timeout(tt.timeout))
			if tt.expected == nil {
				NoError(t, err)
			} else if !xerrors.Is(err, tt.expected) {
				t.Fatalf("%+v", err)
			}
		})
	}
}

func TestSetMaxIdleConnections(t *testing.T) {
	tests := []struct {
		maxIdle  int
		expected error
	}{
		{
			1000,
			nil,
		},
		{
			0,
			server.ErrSetMaxIdleConnections,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("TestSetMaxIdleConnections:%v\n", i), func(t *testing.T) {
			_, err := New(MaxIdleConnections(tt.maxIdle))
			if tt.expected == nil {
				NoError(t, err)
			} else if !xerrors.Is(err, tt.expected) {
				t.Fatalf("%+v", err)
			}
		})
	}
}

func TestCommit(t *testing.T) {
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	NoError(t, tx.CommitDBOnly())
	NoError(t, tx.CommitCacheOnly())
	Error(t, tx.CommitDBOnly())
}

func TestRollback(t *testing.T) {
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	NoError(t, tx.RollbackDBOnly())
	NoError(t, tx.RollbackCacheOnly())
	Error(t, tx.RollbackDBOnly())
}

func TestRollbackUnlessCommitted(t *testing.T) {
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	NoError(t, tx.RollbackDBOnlyUnlessCommitted())
	NoError(t, tx.RollbackCacheOnlyUnlessCommitted())
	Error(t, tx.RollbackDBOnlyUnlessCommitted())
}
