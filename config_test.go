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
}
