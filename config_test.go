package rapidash

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	cfg, err := NewConfig("testdata/cache.yml")
	NoError(t, err)
	testConfig(t, cfg)
}

func TestConfigWithSlcOptionAndLlcOption(t *testing.T) {
	cfg, err := NewConfig("testdata/cache_with_slc_option_and_llc_option.yml")
	NoError(t, err)
	testConfig(t, cfg)
}

func TestConfigWithSlcTableOptionAndLlcTagOption(t *testing.T) {
	cfg, err := NewConfig("testdata/cache_with_slc_table_option_and_llc_tag_option.yml")
	NoError(t, err)
	testConfig(t, cfg)
}

func testConfig(t *testing.T, cfg *Config) {
	cache, err := New(cfg.Options()...)
	NoError(t, err)
	NoError(t, cache.Flush())
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	NoError(t, err)
	NoError(t, cache.WarmUp(conn, userLoginType(), false))
	NoError(t, cache.WarmUp(conn, userLogType(), false))
	t.Run("create new records use redis cache when table option specify", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		now := time.Now()
		for i := 1001; i <= 1005; i++ {
			id := uint64(i)
			if _, err := tx.CreateByTable("user_logins", &UserLogin{
				UserID:        id,
				UserSessionID: id,
				LoginParamID:  id,
				Name:          fmt.Sprintf("rapidash_%d", i),
				CreatedAt:     &now,
				UpdatedAt:     &now,
			}); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		NoError(t, tx.Commit())
	})
	t.Run("get records use redis cache when table option specify", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		for i := 1001; i <= 1005; i++ {
			builder := NewQueryBuilder("user_logins").
				Eq("user_id", uint64(i)).
				Eq("user_session_id", uint64(i))
			var foundUserLogin UserLogin
			NoError(t, tx.FindByQueryBuilder(builder, &foundUserLogin))
			if foundUserLogin.ID == 0 {
				t.Fatal("cannot find record")
			}
		}
		NoError(t, tx.Commit())
	})
	t.Run("delete records use redis cache when table option specify", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		for i := 1001; i <= 1005; i++ {
			builder := NewQueryBuilder("user_logins").
				Eq("user_id", uint64(i)).
				Eq("user_session_id", uint64(i))
			NoError(t, tx.DeleteByQueryBuilder(builder))
		}
		NoError(t, tx.Commit())
	})
	t.Run("create new records use memcached cache when table option specify", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		now := time.Now()
		for i := 1001; i <= 1005; i++ {
			id := uint64(i)
			if _, err := tx.CreateByTable("user_logs", &UserLog{
				ID:          id,
				UserID:      id,
				ContentType: "web",
				ContentID:   id,
				CreatedAt:   &now,
				UpdatedAt:   &now,
			}); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		NoError(t, tx.Commit())
	})
	t.Run("get records use memcached cache when table option specify", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		for i := 1001; i <= 1005; i++ {
			builder := NewQueryBuilder("user_logs").
				Eq("id", uint64(i))
			var foundUserLog UserLog
			NoError(t, tx.FindByQueryBuilder(builder, &foundUserLog))
			if foundUserLog.ID == 0 {
				t.Fatal("cannot find record")
			}
		}
		NoError(t, tx.Commit())
	})
	t.Run("delete records use memcached cache when table option specify", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		for i := 1001; i <= 1005; i++ {
			builder := NewQueryBuilder("user_logs").
				Eq("id", uint64(i))
			NoError(t, tx.DeleteByQueryBuilder(builder))
		}
		NoError(t, tx.Commit())
	})
	t.Run("last level cache with tag use redis cache when tag option specify", func(t *testing.T) {
		tx, err := cache.Begin()
		NoError(t, err)
		tag := "a"
		for i := 1; i <= 10; i++ {
			key := fmt.Sprintf("key_with_tag_%s_%d", tag, i)
			fmt.Printf("key:%v\n", key)
			NoError(t, tx.CreateWithTag(tag, key, Int(1)))
			var a int
			NoError(t, tx.FindWithTag(tag, key, IntPtr(&a)))
			if a != 1 {
				t.Fatal("cannot work set/get with tag")
			}
		}
		NoError(t, tx.Commit())
	})
	t.Run("last level cache with tag use memcached cache when tag option specify", func(t *testing.T) {
		tx, err := cache.Begin()
		NoError(t, err)
		tag := "b"
		for i := 1; i <= 10; i++ {
			key := fmt.Sprintf("key_with_tag_%s_%d", tag, i)
			fmt.Printf("key:%v\n", key)
			NoError(t, tx.CreateWithTag(tag, key, Int(1)))
			var a int
			NoError(t, tx.FindWithTag(tag, key, IntPtr(&a)))
			if a != 1 {
				t.Fatal("cannot work set/get with tag")
			}
		}
		NoError(t, tx.Commit())
	})
	t.Run("last level cache without tag", func(t *testing.T) {
		tx, err := cache.Begin()
		NoError(t, err)
		for i := 1; i <= 10; i++ {
			key := fmt.Sprintf("key_with_not_tag_%d", i)
			NoError(t, tx.Create(key, Int(1)))
			var a int
			NoError(t, tx.Find(key, IntPtr(&a)))
			if a != 1 {
				t.Fatal("cannot work set/get with tag")
			}
		}
		NoError(t, tx.Commit())
	})
}
