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
	cache, err := New(cfg.Options()...)
	NoError(t, err)
	NoError(t, cache.Flush())
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	NoError(t, err)
	NoError(t, cache.WarmUp(conn, userLoginType(), false))
	t.Run("create new records", func(t *testing.T) {
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
	t.Run("get records", func(t *testing.T) {
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
	t.Run("last level cache", func(t *testing.T) {
		tx, err := cache.Begin()
		NoError(t, err)
		NoError(t, tx.CreateWithTag("a", "key", Int(1)))
		var a int
		NoError(t, tx.FindWithTag("a", "key", IntPtr(&a)))
		if a != 1 {
			t.Fatal("cannot work set/get with tag")
		}
	})

	t.Run("llc tags", func(t *testing.T) {
		t.Run("cache control", func(t *testing.T) {
			t.Run("with single transaction", func(t *testing.T) {
				t.Run("explicit no cache control", func(t *testing.T) {

					tagName := "cache_control_no_lock"

					t.Run("should have false cache control config", func(t *testing.T) {
						tagCacheControlConfig := (*cfg.LLC.Tags)[tagName].CacheControl
						Equalf(t, *tagCacheControlConfig.OptimisticLock, false, "should be false")
						Equalf(t, *tagCacheControlConfig.PessimisticLock, false, "should be false")
					})
					t.Run("options should have false values", func(t *testing.T) {
						tagOption := cache.lastLevelCache.opt.tagOpt[tagName]
						Equalf(t, *tagOption.optimisticLock, false, "should be false")
						Equalf(t, *tagOption.pessimisticLock, false, "should be false")
					})
					t.Run("should retrieve last created value", func(t *testing.T) {
						key := fmt.Sprintf("key_%d", time.Now().UnixNano())
						var result int
						expect := 2

						tx, err := cache.Begin()
						NoError(t, err)

						NoError(t, tx.CreateWithTag(tagName, key, Int(1)))
						NoError(t, tx.CreateWithTag(tagName, key, Int(expect)))
						NoError(t, tx.FindWithTag(tagName, key, IntPtr(&result)))

						Equalf(t, result, expect, "should retrieve last created value")
					})
				})

				t.Run("explicit lock", func(t *testing.T) {

					tagName := "cache_control_lock"

					t.Run("config should have true cache control config", func(t *testing.T) {
						tagCacheControlConfig := (*cfg.LLC.Tags)[tagName].CacheControl
						Equalf(t, *tagCacheControlConfig.OptimisticLock, true, "should be true")
						Equalf(t, *tagCacheControlConfig.PessimisticLock, true, "should be true")
					})
					t.Run("options should have true values", func(t *testing.T) {
						tagOption := cache.lastLevelCache.opt.tagOpt[tagName]
						Equalf(t, *tagOption.optimisticLock, true, "should be true")
						Equalf(t, *tagOption.pessimisticLock, true, "should be true")
					})
					t.Run("should retrieve last created value", func(t *testing.T) {
						key := fmt.Sprintf("key_%d", time.Now().UnixNano())
						var result int
						expect := 2

						tx, err := cache.Begin()
						NoError(t, err)
						NoError(t, tx.CreateWithTag(tagName, key, Int(1)))
						NoError(t, tx.CreateWithTag(tagName, key, Int(expect)))
						NoError(t, tx.FindWithTag(tagName, key, IntPtr(&result)))

						Equalf(t, result, expect, "should retrieve last created value")
					})
				})

				t.Run("implicit cache control", func(t *testing.T) {

					tagName := "cache_control_implicit"

					t.Run("config should have nil values", func(t *testing.T) {
						tagCacheControlConfig := (*cfg.LLC.Tags)[tagName].CacheControl
						IsNilf(t, tagCacheControlConfig, "should be nil")
					})

					t.Run("options should have nil values", func(t *testing.T) {
						tagOption := cache.lastLevelCache.opt.tagOpt[tagName]
						IsNilf(t, tagOption.optimisticLock, "should be nil")
						IsNilf(t, tagOption.pessimisticLock, "should be nil")
					})

					t.Run("should retrieve last created value", func(t *testing.T) {
						key := fmt.Sprintf("key_%d", time.Now().UnixNano())
						var result int
						expect := 2

						tx, err := cache.Begin()
						NoError(t, err)

						NoError(t, err)
						NoError(t, tx.CreateWithTag(tagName, key, Int(1)))
						NoError(t, tx.CreateWithTag(tagName, key, Int(expect)))
						NoError(t, tx.FindWithTag(tagName, key, IntPtr(&result)))

						Equalf(t, result, expect, "should retrieve last created value")
					})
				})
			})

			t.Run("with multiple transaction that handles exact same key", func(t *testing.T) {
				t.Run("explicit no cache control", func(t *testing.T) {

					tagName := "cache_control_no_lock"

					t.Run("should retrieve each handled data", func(t *testing.T) {
						key := fmt.Sprintf("key_%d", time.Now().UnixNano())
						var resultFirst  int
						var resultSecond int
						expectFirst := 1
						expectSecond := 2

						txFirst, err := cache.Begin()
						NoError(t, err)

						txSecond, err := cache.Begin()
						NoError(t, err)

						NoError(t, txFirst.CreateWithTag(tagName, key, Int(expectFirst)))
						NoError(t, txSecond.CreateWithTag(tagName, key, Int(expectSecond)))

						NoError(t, txFirst.FindWithTag(tagName, key, IntPtr(&resultFirst)))
						NoError(t, txSecond.FindWithTag(tagName, key, IntPtr(&resultSecond)))

						Equalf(t, resultFirst, expectFirst, "should retrieve each handled data")
						Equalf(t, resultSecond, expectSecond, "should retrieve each handled data")
					})
				})

				t.Run("explicit lock", func(t *testing.T) {
					t.Run("should occur error on second create", func(t *testing.T) {
						key := fmt.Sprintf("key_%d", time.Now().UnixNano())

						txFirst, err := cache.Begin()
						NoError(t, err)

						txSecond, err := cache.Begin()
						NoError(t, err)

						NoError(t, txFirst.CreateWithTag("cache_control_lock", key, Int(1)))
						Errorf(t, txSecond.CreateWithTag("cache_control_lock", key, Int(2)), "should occur error on second create")
					})
				})

				t.Run("implicit cache control", func(t *testing.T) {
					t.Run("should retrieve each handled data", func(t *testing.T) {
						key := fmt.Sprintf("key_%d", time.Now().UnixNano())
						var resultFirst  int
						var resultSecond int
						expectFirst := 1
						expectSecond := 2

						txFirst, err := cache.Begin()
						NoError(t, err)

						txSecond, err := cache.Begin()
						NoError(t, err)

						NoError(t, txFirst.CreateWithTag("cache_control_implicit", key, Int(expectFirst)))
						NoError(t, txSecond.CreateWithTag("cache_control_implicit", key, Int(expectSecond)))

						NoError(t, txFirst.FindWithTag("cache_control_implicit", key, IntPtr(&resultFirst)))
						NoError(t, txSecond.FindWithTag("cache_control_implicit", key, IntPtr(&resultSecond)))

						Equalf(t, resultFirst, expectFirst, "should retrieve each handled data")
						Equalf(t, resultSecond, expectSecond, "should retrieve each handled data")
					})
				})
			})
		})
	})
}
