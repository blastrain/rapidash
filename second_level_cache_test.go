package rapidash

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.knocknote.io/rapidash/database"

	"go.knocknote.io/rapidash/server"
	"golang.org/x/xerrors"
)

type UserLogin struct {
	ID            uint64     `db:"id"              json:"id"`
	UserID        uint64     `db:"user_id"         json:"userId"`
	UserSessionID uint64     `db:"user_session_id" json:"userSessionId"`
	LoginParamID  uint64     `db:"login_param_id"  json:"loginParamId"`
	Name          string     `db:"name"            json:"name"`
	CreatedAt     *time.Time `db:"created_at"      json:"createdAt"`
	UpdatedAt     *time.Time `db:"updated_at"      json:"updatedAt"`
}

type UserLogins []*UserLogin

func (u *UserLogin) EncodeRapidash(enc Encoder) error {
	if u.ID != 0 {
		enc.Uint64("id", u.ID)
	}
	enc.Uint64("user_id", u.UserID)
	enc.Uint64("user_session_id", u.UserSessionID)
	enc.Uint64("login_param_id", u.LoginParamID)
	enc.String("name", u.Name)
	enc.TimePtr("created_at", u.CreatedAt)
	enc.TimePtr("updated_at", u.UpdatedAt)
	if err := enc.Error(); err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	return nil
}

func (u *UserLogin) DecodeRapidash(dec Decoder) error {
	u.ID = dec.Uint64("id")
	u.UserID = dec.Uint64("user_id")
	u.UserSessionID = dec.Uint64("user_session_id")
	u.LoginParamID = dec.Uint64("login_param_id")
	u.Name = dec.String("name")
	u.CreatedAt = dec.TimePtr("created_at")
	u.UpdatedAt = dec.TimePtr("updated_at")
	return nil
}

func (u *UserLogins) EncodeRapidash(enc Encoder) error {
	for _, v := range *u {
		if err := v.EncodeRapidash(enc.New()); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
	}
	return nil
}

func (u *UserLogins) DecodeRapidash(dec Decoder) error {
	len := dec.Len()
	*u = make([]*UserLogin, len)
	for i := 0; i < len; i++ {
		var v UserLogin
		if err := v.DecodeRapidash(dec.At(i)); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
		(*u)[i] = &v
	}
	return nil
}

func userLoginType() *Struct {
	return NewStruct("user_logins").
		FieldUint64("id").
		FieldUint64("user_id").
		FieldUint64("user_session_id").
		FieldUint64("login_param_id").
		FieldString("name").
		FieldTime("created_at").
		FieldTime("updated_at")
}

type UserLoginAfterAddColumn struct {
	UserLogin
	Password string
}

func (u *UserLoginAfterAddColumn) DecodeRapidash(dec Decoder) error {
	if err := u.UserLogin.DecodeRapidash(dec); err != nil {
		return err
	}
	u.Password = dec.String("password")
	return dec.Error()
}

type UserLoginReTyped struct {
	UserLogin
	Password uint64
}

func (u *UserLoginReTyped) DecodeRapidash(dec Decoder) error {
	if err := u.UserLogin.DecodeRapidash(dec); err != nil {
		return err
	}
	u.Password = dec.Uint64("password")
	return dec.Error()
}

func defaultUserLogin() *UserLogin {
	nowTime := time.Now()
	return &UserLogin{
		ID:            1,
		UserID:        1,
		UserSessionID: 1,
		LoginParamID:  1,
		Name:          "rapidash1",
		UpdatedAt:     &nowTime,
		CreatedAt:     &nowTime,
	}
}

type UserLog struct {
	ID          uint64
	UserID      uint64
	ContentType string
	ContentID   uint64
	CreatedAt   *time.Time
	UpdatedAt   *time.Time
}

type UserLogs []*UserLog

func (l *UserLog) EncodeRapidash(enc Encoder) error {
	if l.ID != 0 {
		enc.Uint64("id", l.ID)
	}
	enc.Uint64("user_id", l.UserID)
	enc.Uint64("content_id", l.ContentID)
	enc.String("content_type", l.ContentType)
	enc.TimePtr("created_at", l.CreatedAt)
	enc.TimePtr("updated_at", l.UpdatedAt)
	if err := enc.Error(); err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	return nil
}

func (l *UserLog) DecodeRapidash(dec Decoder) error {
	l.ID = dec.Uint64("id")
	l.UserID = dec.Uint64("user_id")
	l.ContentType = dec.String("content_type")
	l.ContentID = dec.Uint64("content_id")
	l.CreatedAt = dec.TimePtr("created_at")
	l.UpdatedAt = dec.TimePtr("updated_at")
	return nil
}

func (l *UserLogs) EncodeRapidash(enc Encoder) error {
	for _, v := range *l {
		if err := v.EncodeRapidash(enc.New()); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
	}
	return nil
}

func (l *UserLogs) DecodeRapidash(dec Decoder) error {
	len := dec.Len()
	*l = make([]*UserLog, len)
	for i := 0; i < len; i++ {
		var v UserLog
		if err := v.DecodeRapidash(dec.At(i)); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
		(*l)[i] = &v
	}
	return nil
}

func userLogType() *Struct {
	return NewStruct("user_logs").
		FieldUint64("id").
		FieldUint64("user_id").
		FieldString("content_type").
		FieldUint64("content_id").
		FieldTime("created_at").
		FieldTime("updated_at")
}

func TestEncodeDecode(t *testing.T) {
	userLogin := defaultUserLogin()
	enc := NewStructEncoder(userLoginType(), NewValueFactory())
	NoError(t, userLogin.EncodeRapidash(enc))
	content, err := enc.Encode()
	NoError(t, err)
	dec := NewDecoder(userLoginType(), &bytes.Buffer{}, NewValueFactory())
	dec.SetBuffer(content)
	value, err := dec.Decode()
	NoError(t, err)
	var v UserLogin
	NoError(t, v.DecodeRapidash(value))
	if userLogin.ID != v.ID {
		t.Fatal("cannot encode/decode uint64 value")
	}
	if userLogin.Name != v.Name {
		t.Fatal("cannot encode/decode string value")
	}
	if userLogin.CreatedAt.Unix() != v.CreatedAt.Unix() {
		t.Fatal("cannot encode/decode *time.Time value")
	}
}

func TestSimpleRead(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testSimpleRead(t, CacheServerType(cacheServerType))
	}
}

func testSimpleRead(t *testing.T, typ CacheServerType) {
	NoError(t, initCache(conn, typ))
	userLogin := defaultUserLogin()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	t.Run("found value from db", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
		var v UserLogin
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

		Equal(t, v.ID, userLogin.ID)
		Equal(t, v.Name, userLogin.Name)

		NoError(t, tx.Commit())
	})
	t.Run("found value from cache(and stash)", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		t.Run("from cache server", func(t *testing.T) {
			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
			var v UserLogin
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

			Equal(t, v.ID, userLogin.ID)
			Equal(t, v.Name, userLogin.Name)
		})
		t.Run("from stash", func(t *testing.T) {
			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
			var v UserLogin
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

			Equal(t, v.ID, userLogin.ID)
			Equal(t, v.Name, userLogin.Name)
		})
		NoError(t, tx.Commit())
	})
	t.Run("find after ALTER TABLE", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
		var v UserLogin
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

		Equal(t, v.ID, userLogin.ID)
		Equal(t, v.Name, userLogin.Name)

		NoError(t, tx.Commit())

		f, err := os.Open(filepath.Join("testdata", driver.Name, "alter_user_logins.sql"))
		NoError(t, err)
		defer f.Close()
		queryScanner := bufio.NewScanner(f)
		t.Run("ADD COLUMN", func(t *testing.T) {
			{
				txConn, err := conn.Begin()
				NoError(t, err)
				queryScanner.Scan()
				if _, err := txConn.Exec(queryScanner.Text()); err != nil {
					NoError(t, txConn.Rollback())
					t.Fatalf("%+v", err)
				}
				NoError(t, txConn.Commit())
			}
			txConn, err := conn.Begin()
			NoError(t, err)
			NoError(t, cache.WarmUpSecondLevelCache(conn, userLoginType().FieldString("password")))
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			defer func() {
				NoError(t, tx.RollbackUnlessCommitted())
			}()

			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
			var v UserLoginAfterAddColumn
			NoError(t, tx.FindByQueryBuilder(builder, &v))

			Equal(t, v.ID, userLogin.ID)
			Equal(t, v.Name, userLogin.Name)
			Equal(t, v.Password, "100")
			NoError(t, tx.Commit())
		})
		t.Run("MODIFY COLUMN", func(t *testing.T) {
			{
				txConn, err := conn.Begin()
				NoError(t, err)
				queryScanner.Scan()
				if _, err := txConn.Exec(queryScanner.Text()); err != nil {
					NoError(t, txConn.Rollback())
					t.Fatalf("%+v", err)
				}
				NoError(t, txConn.Commit())
			}
			txConn, err := conn.Begin()
			NoError(t, err)
			NoError(t, cache.WarmUpSecondLevelCache(conn, userLoginType().FieldUint64("password")))
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			defer func() {
				NoError(t, tx.RollbackUnlessCommitted())
			}()

			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
			var v UserLoginReTyped
			NoError(t, tx.FindByQueryBuilder(builder, &v))

			Equal(t, v.ID, userLogin.ID)
			Equal(t, v.Name, userLogin.Name)
			Equal(t, v.Password, uint64(100))
			NoError(t, tx.Commit())
		})
		t.Run("DROP COLUMN", func(t *testing.T) {
			{
				txConn, err := conn.Begin()
				NoError(t, err)
				queryScanner.Scan()
				if _, err := txConn.Exec(queryScanner.Text()); err != nil {
					NoError(t, txConn.Rollback())
					t.Fatalf("%+v", err)
				}
				NoError(t, txConn.Commit())
			}
			txConn, err := conn.Begin()
			NoError(t, err)
			NoError(t, cache.WarmUpSecondLevelCache(conn, userLoginType()))

			tx, err := cache.Begin(txConn)
			NoError(t, err)
			defer func() {
				NoError(t, tx.RollbackUnlessCommitted())
			}()

			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
			var v UserLogin
			NoError(t, tx.FindByQueryBuilder(builder, &v))

			Equal(t, v.ID, userLogin.ID)
			Equal(t, v.Name, userLogin.Name)
			NoError(t, tx.Commit())
		})

	})
	t.Run("not found value from db", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(10000))
		var v UserLogin
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

		Equal(t, v.ID, uint64(0))

		NoError(t, tx.Commit())
	})
	t.Run("found nil value from cache", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		t.Run("from cache server", func(t *testing.T) {
			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(10000))
			var v UserLogin
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

			Equal(t, v.ID, uint64(0))
		})
		t.Run("from stash", func(t *testing.T) {
			builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(10000))
			var v UserLogin
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

			Equal(t, v.ID, uint64(0))
		})
		NoError(t, tx.Commit())
	})
}

func TestSimpleReadWithPessimisticLock(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testSimpleReadWithPessimisticLock(t, CacheServerType(cacheServerType))
	}
}

func testSimpleReadWithPessimisticLock(t *testing.T, typ CacheServerType) {
	NoError(t, initCache(conn, typ))
	userLogin := defaultUserLogin()
	pessimisticLock := true
	lockExpiration := time.Duration(6000)
	expiration := time.Duration(6000)
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{
		pessimisticLock: &pessimisticLock,
		lockExpiration:  &lockExpiration,
		expiration:      &expiration,
	}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
	var v UserLogin
	NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))
	if v.ID != userLogin.ID {
		t.Fatal("cannot read uint64 value")
	}
	if v.Name != userLogin.Name {
		t.Fatal("cannot read string value")
	}
	t.Run("find locked value by another tx", func(t *testing.T) {
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
		var v UserLogin
		Error(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))
	})
	NoError(t, tx.Commit())
}

func TestSimpleCreate(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testSimpleCreate(t, CacheServerType(cacheServerType))
	}
}

func testSimpleCreate(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))
	userLogin := defaultUserLogin()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)

	userLogin.ID = 0
	userLogin.UserID = 2
	userLogin.UserSessionID = 2
	userLogin.LoginParamID = 2
	id, err := slc.Create(context.Background(), tx, userLogin)
	NoError(t, err)

	userLogin.ID = uint64(id)
	if userLogin.ID != 1001 {
		t.Fatal("cannot assign id")
	}
	builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("user_id", uint64(2))
	var foundUserLogin UserLogin
	NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &foundUserLogin))

	if foundUserLogin.UserID != userLogin.UserID {
		t.Fatal("cannot insert record")
	}
	NoError(t, tx.Commit())
}

func TestSimpleUpdate(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testSimpleUpdate(t, CacheServerType(cacheServerType))
	}
}

func testSimpleUpdate(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)

	builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
	var v UserLogin
	NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

	newName := "rapidash_2"
	v.Name = newName
	NoError(t, slc.UpdateByPrimaryKey(tx, &v))

	var v2 UserLogin
	NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v2))

	if v2.Name != newName {
		t.Fatal("cannot update value")
	}
	NoError(t, tx.Commit())
}

func TestSimpleDelete(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testSimpleDelete(t, CacheServerType(cacheServerType))
	}
}

func testSimpleDelete(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))

	userLogin := defaultUserLogin()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)

	builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
	var v UserLogin
	NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))

	if v.ID != userLogin.ID {
		t.Fatal("cannot read uint64 value")
	}
	NoError(t, slc.DeleteByPrimaryKey(tx, NewUint64Value(1)))
	NoError(t, tx.Commit())
}

func TestCreateWithoutCache(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testCreateWithoutCache(t, CacheServerType(cacheServerType))
	}
}

func testCreateWithoutCache(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))

	userLogin := defaultUserLogin()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)

	userLogin.ID = 0
	userLogin.UserID = 3
	userLogin.UserSessionID = 2
	userLogin.LoginParamID = 2
	id, err := slc.CreateWithoutCache(context.Background(), tx, userLogin)
	NoError(t, err)

	userLogin.ID = uint64(id)
	if userLogin.ID != 1001 {
		t.Fatal("cannot insert record")
	}
	builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("user_id", uint64(3)).Eq("user_session_id", uint64(2))
	var foundUserLogin UserLogin
	NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &foundUserLogin))

	if foundUserLogin.UserID != 3 {
		t.Fatal("fail to insert")
	}
	NoError(t, tx.Commit())
}

func TestQueryBuilder(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testQueryBuilder(t, CacheServerType(cacheServerType))
	}
}

func testQueryBuilder(t *testing.T, typ CacheServerType) {
	t.Run("WHERE IN AND EQ query", func(t *testing.T) {
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("user_session_id", uint64(1))
		queries, err := builder.BuildWithIndex(slc.valueFactory, slc.indexes, slc.typ)
		NoError(t, err)
		NoError(t, queries.Each(func(q *Query) error {
			return server.ErrCacheMiss
		}))
		query, _ := queries.CacheMissQueriesToSQL(slc.typ)
		if driver.DBType == database.MySQL {
			if query != "SELECT `id`,`user_id`,`user_session_id`,`login_param_id`,`name`,`created_at`,`updated_at` FROM `user_logins` WHERE `user_id` IN (?,?,?,?,?) AND `user_session_id` = ?" {
				t.Fatal("invalid query")
			}
		} else {
			if query != `SELECT "id","user_id","user_session_id","login_param_id","name","created_at","updated_at" FROM "user_logins" WHERE "user_id" IN ($1,$2,$3,$4,$5) AND "user_session_id" = $6` {
				t.Fatal("invalid query")
			}
		}
	})

	t.Run("IS NULL query", func(t *testing.T) {
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("created_at", nil)
		queries, err := builder.BuildWithIndex(slc.valueFactory, slc.indexes, slc.typ)
		NoError(t, err)
		NoError(t, queries.Each(func(q *Query) error {
			return server.ErrCacheMiss
		}))
		query, _ := queries.CacheMissQueriesToSQL(slc.typ)

		if driver.DBType == database.MySQL {
			if query != "SELECT `id`,`user_id`,`user_session_id`,`login_param_id`,`name`,`created_at`,`updated_at` FROM `user_logins` WHERE `user_id` IN (?,?,?,?,?) AND `created_at` IS NULL" {
				t.Fatal("invalid query")
			}
		} else {
			if query != `SELECT "id","user_id","user_session_id","login_param_id","name","created_at","updated_at" FROM "user_logins" WHERE "user_id" IN ($1,$2,$3,$4,$5) AND "created_at" IS NULL` {
				t.Fatal("invalid query")
			}
		}
	})
}

func TestFindByQueryBuilder(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testFindByQueryBuilder(t, CacheServerType(cacheServerType))
	}
}

// nolint: gocyclo
func testFindByQueryBuilder(t *testing.T, typ CacheServerType) {
	t.Run("find by index column query builder", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.cacheServer.Flush())
		NoError(t, slc.WarmUp(conn))

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("login_param_id", uint64(1))

		t.Run("find from db", func(t *testing.T) {
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)

			var userLogins UserLogins
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))

			if len(userLogins) != 5 {
				t.Fatal("cannot work FindByQueryBuilder")
			}
			NoError(t, tx.Commit())
		})

		t.Run("find from cache", func(t *testing.T) {
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			t.Run("from cache", func(t *testing.T) {
				var userLogins UserLogins
				NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
				if len(userLogins) != 5 {
					t.Fatal("cannot work FindByQueryBuilder")
				}
				t.Run("duplicated values", func(t *testing.T) {
					builder := NewQueryBuilder("user_logins", driver.Adapter).
						In("user_id", []uint64{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}).
						Eq("login_param_id", uint64(1))
					var userLogins UserLogins
					NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
					if len(userLogins) != 5 {
						t.Fatal("cannot work FindByQueryBuilder")
					}
				})
			})
			t.Run("from server", func(t *testing.T) {
				var userLogins UserLogins
				NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
				if len(userLogins) != 5 {
					t.Fatal("cannot work FindByQueryBuilder")
				}
			})

			NoError(t, tx.Commit())
		})
	})

	t.Run("cache miss query, find from db", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.cacheServer.Flush())
		NoError(t, slc.WarmUp(conn))

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("user_session_id", uint64(1))
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 5 {
			t.Fatal("cannot work FindByQueryBuilder")
		}
		NoError(t, tx.Commit())
	})

	t.Run("partially found pk and value in cache with uq key", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))
		{
			txConn, err := conn.Begin()
			NoError(t, err)
			defer func() { NoError(t, txConn.Rollback()) }()
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			NoError(t, slc.DeleteByPrimaryKey(tx, NewUint64Value(2)))
			NoError(t, tx.CommitCacheOnly())
		}

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5, 6}).
			Eq("user_session_id", uint64(1))
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 6 {
			t.Fatal("cannot work FindByQueryBuilder")
		}
		NoError(t, tx.Commit())
	})
	t.Run("partially found pk and value in cache with idx key", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))
		{
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			now := time.Now()
			_, err = slc.Create(context.Background(), tx, &UserLogin{
				UserID:        5,
				UserSessionID: 2,
				LoginParamID:  1,
				CreatedAt:     &now,
				UpdatedAt:     &now,
			})
			NoError(t, err)
			_, err = slc.Create(context.Background(), tx, &UserLogin{
				UserID:        5,
				UserSessionID: 3,
				LoginParamID:  1,
				CreatedAt:     &now,
				UpdatedAt:     &now,
			})
			NoError(t, err)
			_, err = slc.Create(context.Background(), tx, &UserLogin{
				UserID:        5,
				UserSessionID: 4,
				LoginParamID:  1,
				CreatedAt:     &now,
				UpdatedAt:     &now,
			})
			NoError(t, err)
			_, err = slc.Create(context.Background(), tx, &UserLogin{
				UserID:        5,
				UserSessionID: 5,
				LoginParamID:  1,
				CreatedAt:     &now,
				UpdatedAt:     &now,
			})
			NoError(t, err)
			NoError(t, tx.Commit())
		}
		{
			builder := NewQueryBuilder("user_logins", driver.Adapter).
				Eq("user_id", uint64(5)).
				In("login_param_id", []uint64{1, 2})
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			var userLogins UserLogins
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
			if len(userLogins) != 5 {
				t.Fatal("cannot work FindByQueryBuilder")
			}
			NoError(t, tx.Commit())
		}
		{
			txConn, err := conn.Begin()
			NoError(t, err)
			defer func() { NoError(t, txConn.Rollback()) }()
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			NoError(t, slc.DeleteByPrimaryKey(tx, NewUint64Value(5)))
			NoError(t, slc.DeleteByPrimaryKey(tx, NewUint64Value(1001)))
			NoError(t, slc.delete(tx, &CacheKey{key: "r/slc/user_logins/idx/user_id#5&login_param_id#2"}))
			NoError(t, tx.CommitCacheOnly())
		}

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(5)).
			In("login_param_id", []uint64{1, 2})
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 5 {
			t.Fatal("cannot work FindByQueryBuilder")
		}
		NoError(t, tx.Commit())
	})

	t.Run("find after updated index column value in same tx", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		NoError(t, initCache(conn, typ))
		slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.cacheServer.Flush())
		NoError(t, slc.WarmUp(conn))

		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		var userLogin *UserLogin
		{
			builder := NewQueryBuilder("user_logins", driver.Adapter).
				In("user_id", []uint64{1, 2, 3, 4, 5}).
				Eq("user_session_id", uint64(1))
			var userLogins UserLogins
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))

			if len(userLogins) != 5 {
				t.Fatal("cannot work FindByQueryBuilder")
			}
			userLogin = userLogins[0]
		}
		updateBuilder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", userLogin.ID)
		updateMap := map[string]interface{}{
			"login_param_id": uint64(5),
		}
		NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, updateBuilder, updateMap))

		{
			builder := NewQueryBuilder("user_logins", driver.Adapter).
				Eq("user_id", userLogin.UserID).
				Eq("login_param_id", uint64(5))
			var userLogins UserLogins
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))

			if len(userLogins) != 1 {
				t.Fatal("cannot work FindByQueryBuilder")
			}
			if userLogins[0].LoginParamID != 5 {
				t.Fatal("cannot work FindByQueryBuilder")
			}
		}

		NoError(t, tx.Commit())
	})
}

func TestUpdateByQueryBuilderUsingMemcached(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached} {
		testUpdateByQueryBuilder(t, CacheServerType(cacheServerType))
	}
}

func TestUpdateByQueryBuilderUsingRedis(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeRedis} {
		testUpdateByQueryBuilder(t, CacheServerType(cacheServerType))
	}
}

func testUpdateByQueryBuilder(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))
	s := "user_id"
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{shardKey: &s}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))

	t.Run("available cache", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("user_session_id", uint64(1))
		name := fmt.Sprintf("rapidash_%d", 2)
		updateParam := map[string]interface{}{
			"name": name,
		}
		NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))
		var newUserLogin UserLogin
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &newUserLogin))
		if newUserLogin.Name != name {
			t.Fatal("cannot update cache")
		}
		NoError(t, tx.Commit())
	})
	t.Run("unavailable cache", func(t *testing.T) {
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Gte("user_id", uint64(6)).
			Lte("user_id", uint64(10))
		t.Run("update without cache", func(t *testing.T) {
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)

			name := fmt.Sprintf("rapidash_%d", 2)
			updateParam := map[string]interface{}{
				"name": name,
			}

			NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))

			var newUserLogins UserLogins
			findBuilder := NewQueryBuilder("user_logins", driver.Adapter).In("user_id", []uint64{6, 7, 8, 9, 10})
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, findBuilder, &newUserLogins))

			Equal(t, len(newUserLogins), 5)
			for _, userLogin := range newUserLogins {
				Equal(t, userLogin.Name, name)
			}
			NoError(t, tx.Commit())
		})

		t.Run("update with cache", func(t *testing.T) {
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)

			name := fmt.Sprintf("rapidash_%d", 1)
			loginParamID := uint64(4)
			updateParam := map[string]interface{}{
				"name":           name,
				"login_param_id": loginParamID,
			}
			NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))

			var newUserLogins UserLogins
			findBuilder := NewQueryBuilder("user_logins", driver.Adapter).In("user_id", []uint64{6, 7, 8, 9, 10})
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, findBuilder, &newUserLogins))
			Equal(t, len(newUserLogins), 5)
			for _, userLogin := range newUserLogins {
				Equal(t, userLogin.Name, name)
				Equal(t, userLogin.LoginParamID, loginParamID)
			}

			NoError(t, tx.Commit())
		})
	})

}

func TestUpdateUniqueKeyColumn(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testUpdateUniqueKeyColumn(t, CacheServerType(cacheServerType))
	}
}

func testUpdateUniqueKeyColumn(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	builder := NewQueryBuilder("user_logins", driver.Adapter).
		In("user_id", []uint64{1, 2, 3, 4, 5}).
		Eq("login_param_id", uint64(1))
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	updateParam := map[string]interface{}{
		"login_param_id": uint64(10),
	}
	NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))
	{
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("login_param_id", uint64(10))
		var newUserLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &newUserLogins))
		if len(newUserLogins) != 5 {
			t.Fatal("cannot update cache")
		}
	}
	NoError(t, tx.Commit())
}

func TestUpdateKeyColumn(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testUpdateKeyColumn(t, CacheServerType(cacheServerType))
	}
}

func testUpdateKeyColumn(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))
	builder := NewQueryBuilder("user_logins", driver.Adapter).
		In("user_id", []uint64{1, 2, 3, 4, 5}).
		Eq("user_session_id", uint64(1))
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	updateParam := map[string]interface{}{
		"user_session_id": uint64(10),
	}
	NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))
	{
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("user_session_id", uint64(10))
		var newUserLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &newUserLogins))
		if len(newUserLogins) != 5 {
			t.Fatal("cannot update cache")
		}
	}
	NoError(t, tx.Commit())
}

func TestUniqueIndexColumnUpdateByPrimaryKey(t *testing.T) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, CacheServerTypeMemcached))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	// set unique index cache
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("user_session_id", uint64(1))

		// create for positive cache
		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 1 {
			t.Fatal("failed to get value by index key")
		}

		builder = NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("user_session_id", uint64(2))

		// create for negative cache
		var newUserLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &newUserLogins))
		if len(newUserLogins) != 0 {
			t.Fatal("failed to get negative cache")
		}
		NoError(t, tx.Commit())
	}

	// update unique index cache by primary key
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		updateParam := map[string]interface{}{
			"user_session_id": uint64(2),
		}
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
		NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))
		NoError(t, tx.Commit())
	}

	// get value by old param
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("user_session_id", uint64(1))

		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 0 {
			t.Fatal("failed to get to updated value")
		}
		NoError(t, tx.Commit())
	}

	// get value by new param
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("user_session_id", uint64(2))

		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 1 {
			t.Fatal("failed to get to updated value")
		}
		NoError(t, tx.Commit())
	}
}

func TestIndexColumnUpdateByPrimaryKey(t *testing.T) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, CacheServerTypeMemcached))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	// set index cache
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("login_param_id", uint64(1))

		// create for positive cache
		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 1 {
			t.Fatal("failed to get value by index key")
		}

		builder = NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("login_param_id", uint64(2))

		// create for negative cache
		var newUserLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &newUserLogins))
		if len(newUserLogins) != 0 {
			t.Fatal("failed to get negative cache")
		}
		NoError(t, tx.Commit())
	}

	// update index cache by primary key
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		updateParam := map[string]interface{}{
			"login_param_id": uint64(2),
		}
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
		NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))
		NoError(t, tx.Commit())
	}

	// get value by old param
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("login_param_id", uint64(1))

		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 0 {
			t.Fatal("failed to get to updated value")
		}
		NoError(t, tx.Commit())
	}

	// get value by new param
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			Eq("login_param_id", uint64(2))

		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 1 {
			t.Fatal("failed to get to updated value")
		}
		NoError(t, tx.Commit())
	}
}

func TestLockingRead(t *testing.T) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, CacheServerTypeMemcached))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.cacheServer.Flush())
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)

	// store cache to stash
	{
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("user_id", uint64(1))

		var userLogin UserLogin
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogin))
		if userLogin.ID == 0 {
			t.Fatal("failed to get cached value")
		}
		if userLogin.LoginParamID != 1 {
			t.Fatal("invalid param")
		}
	}

	// update cache another transaction
	{
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		updateParam := map[string]interface{}{
			"login_param_id": uint64(2),
		}
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("id", uint64(1))
		NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateParam))
		NoError(t, tx.Commit())
	}

	// repeatable read.
	// in this case, cannot get updated value in normal query,
	// but if use locking read query, could read updated value.
	{
		builder := NewQueryBuilder("user_logins", driver.Adapter).Eq("user_id", uint64(1)).ForUpdate()

		var userLogin UserLogin
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogin))
		if userLogin.ID == 0 {
			t.Fatal("failed to get cached value")
		}
		if userLogin.LoginParamID != 2 {
			t.Fatal("invalid param")
		}
	}

	NoError(t, tx.Commit())
}

func TestDeleteByQueryBuilder(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testDeleteByQueryBuilder(t, CacheServerType(cacheServerType))
	}
}

func testDeleteByQueryBuilder(t *testing.T, typ CacheServerType) {
	NoError(t, initCache(conn, typ))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))
	t.Run("cache is available", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("user_id", []uint64{1, 2, 3, 4, 5}).
			Eq("user_session_id", uint64(1))
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		NoError(t, slc.DeleteByQueryBuilder(context.Background(), tx, builder))
		NoError(t, tx.Commit())
	})

	t.Run("not available cache", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			Gte("user_session_id", uint64(1)).
			Lte("user_session_id", uint64(3))
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		NoError(t, slc.DeleteByQueryBuilder(context.Background(), tx, builder))

		var userLogins UserLogins
		findBuilder := NewQueryBuilder("user_logins", driver.Adapter).
			Eq("user_id", uint64(1)).
			In("user_session_id", []uint64{1, 2, 3})
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, findBuilder, &userLogins))
		if len(userLogins) != 0 {
			t.Fatal("fail to delete")
		}
		NoError(t, tx.Commit())
	})

	t.Run("delete by primary keys", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			In("id", []uint64{1, 2, 3, 4, 5})
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		NoError(t, slc.DeleteByQueryBuilder(context.Background(), tx, builder))

		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 0 {
			t.Fatal("fail to delete")
		}
		NoError(t, tx.Commit())
	})

}

func TestRawQuery(t *testing.T) {
	for cacheServerType := range []CacheServerType{CacheServerTypeMemcached, CacheServerTypeRedis} {
		testRawQuery(t, CacheServerType(cacheServerType))
	}
}

func testRawQuery(t *testing.T, typ CacheServerType) {
	NoError(t, initUserLoginTable(conn))
	NoError(t, initCache(conn, typ))
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))

	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)

	defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
	t.Run("raw query", func(t *testing.T) {
		qh := driver.Adapter.QueryHelper()
		builder := NewQueryBuilder("user_logins", driver.Adapter).
			SQL(fmt.Sprintf("ORDER BY id DESC LIMIT %s OFFSET %s", qh.Placeholder(), qh.Placeholder()), 3, 1)
		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 3 &&
			userLogins[0].ID != 999 &&
			userLogins[1].ID != 998 &&
			userLogins[2].ID != 997 {
			t.Fatalf("cannot work raw sql")
		}
	})
	t.Run("all query", func(t *testing.T) {
		builder := NewQueryBuilder("user_logins", driver.Adapter)
		var userLogins UserLogins
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &userLogins))
		if len(userLogins) != 1000 {
			t.Fatal("cannot work all sql")
		}
	})
}

type PtrType struct {
	id         uint64
	intPtr     *int
	int8Ptr    *int8
	int16Ptr   *int16
	int32Ptr   *int32
	int64Ptr   *int64
	uintPtr    *uint
	uint8Ptr   *uint8
	uint16Ptr  *uint16
	uint32Ptr  *uint32
	uint64Ptr  *uint64
	float32Ptr *float32
	float64Ptr *float64
	bytesPtr   *[]byte
	stringPtr  *string
	boolPtr    *bool
	timePtr    *time.Time
}

func (p *PtrType) EncodeRapidash(enc Encoder) error {
	if p.id != 0 {
		enc.Uint64("id", p.id)
	}
	enc.IntPtr("intptr", p.intPtr)
	enc.Int8Ptr("int8ptr", p.int8Ptr)
	enc.Int16Ptr("int16ptr", p.int16Ptr)
	enc.Int32Ptr("int32ptr", p.int32Ptr)
	enc.Int64Ptr("int64ptr", p.int64Ptr)
	enc.UintPtr("uintptr", p.uintPtr)
	enc.Uint8Ptr("uint8ptr", p.uint8Ptr)
	enc.Uint16Ptr("uint16ptr", p.uint16Ptr)
	enc.Uint32Ptr("uint32ptr", p.uint32Ptr)
	enc.Uint64Ptr("uint64ptr", p.uint64Ptr)
	enc.Float32Ptr("float32ptr", p.float32Ptr)
	enc.Float64Ptr("float64ptr", p.float64Ptr)
	enc.BytesPtr("bytesptr", p.bytesPtr)
	enc.StringPtr("stringptr", p.stringPtr)
	enc.BoolPtr("boolptr", p.boolPtr)
	enc.TimePtr("timeptr", p.timePtr)
	return enc.Error()
}

func (p *PtrType) DecodeRapidash(dec Decoder) error {
	p.id = dec.Uint64("id")
	p.intPtr = dec.IntPtr("intptr")
	p.int8Ptr = dec.Int8Ptr("int8ptr")
	p.int16Ptr = dec.Int16Ptr("int16ptr")
	p.int32Ptr = dec.Int32Ptr("int32ptr")
	p.int64Ptr = dec.Int64Ptr("int64ptr")
	p.uintPtr = dec.UintPtr("uintptr")
	p.uint8Ptr = dec.Uint8Ptr("uint8ptr")
	p.uint16Ptr = dec.Uint16Ptr("uint16ptr")
	p.uint32Ptr = dec.Uint32Ptr("uint32ptr")
	p.uint64Ptr = dec.Uint64Ptr("uint64ptr")
	p.float32Ptr = dec.Float32Ptr("float32ptr")
	p.float64Ptr = dec.Float64Ptr("float64ptr")
	p.bytesPtr = dec.BytesPtr("bytesptr")
	p.stringPtr = dec.StringPtr("stringptr")
	p.boolPtr = dec.BoolPtr("boolptr")
	p.timePtr = dec.TimePtr("timeptr")
	return nil
}

func (p *PtrType) Type() *Struct {
	return NewStruct("ptr").
		FieldUint64("id").
		FieldInt("intptr").
		FieldInt8("int8ptr").
		FieldInt16("int16ptr").
		FieldInt32("int32ptr").
		FieldInt64("int64ptr").
		FieldUint("uintptr").
		FieldUint8("uint8ptr").
		FieldUint16("uint16ptr").
		FieldUint32("uint32ptr").
		FieldUint64("uint64ptr").
		FieldFloat32("float32ptr").
		FieldFloat64("float64ptr").
		FieldBytes("bytesptr").
		FieldString("stringptr").
		FieldBool("boolptr").
		FieldTime("timeptr")
}

func validateNilValue(t *testing.T, v *PtrType) {
	if v.intPtr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int8Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int16Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int32Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int64Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uintPtr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint8Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint16Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint32Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint64Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.float32Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.float64Ptr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.bytesPtr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.stringPtr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.boolPtr != nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.timePtr != nil {
		t.Fatal("cannot scan pointer value")
	}
}

func validateNotNilValue(t *testing.T, v *PtrType) {
	if v.intPtr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int8Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int16Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int32Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.int64Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uintPtr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint8Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint16Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint32Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.uint64Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.float32Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.float64Ptr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.bytesPtr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.stringPtr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.boolPtr == nil {
		t.Fatal("cannot scan pointer value")
	}
	if v.timePtr == nil {
		t.Fatal("cannot scan pointer value")
	}
}

func TestPointerType(t *testing.T) {
	NoError(t, initCache(conn, CacheServerTypeMemcached))
	slc := NewSecondLevelCache(new(PtrType).Type(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))

	t.Run("invalid value", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.Rollback()) }()

		builder := NewQueryBuilder("ptr", driver.Adapter).Eq("id", uint64(1))
		var v PtrType
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))
		if v.id != 1 {
			t.Fatal("cannot scan uint64 value")
		}
		validateNilValue(t, &v)
	})
	t.Run("valid value", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.Rollback()) }()

		builder := NewQueryBuilder("ptr", driver.Adapter).Eq("id", uint64(2))
		var v PtrType
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))
		if v.id != 2 {
			t.Fatal("cannot scan uint64 value")
		}
		validateNotNilValue(t, &v)
	})
	t.Run("insert invalid value", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		var v PtrType
		id, err := slc.Create(context.Background(), tx, &v)
		NoError(t, err)
		if id == 0 {
			t.Fatal("cannot insert invalid value")
		}
		var foundValue PtrType
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, NewQueryBuilder("ptr", driver.Adapter).Eq("id", uint64(id)), &foundValue))
		// set invalid value to cache server
		NoError(t, tx.Commit())

		t.Run("fetch from cache server", func(t *testing.T) {
			txConn, err := conn.Begin()
			NoError(t, err)
			tx, err := cache.Begin(txConn)
			NoError(t, err)
			defer func() { NoError(t, tx.Rollback()) }()

			var foundValue PtrType
			NoError(t, slc.FindByQueryBuilder(context.Background(), tx, NewQueryBuilder("ptr", driver.Adapter).Eq("id", uint64(id)), &foundValue))
		})
	})
	t.Run("update valid value", func(t *testing.T) {
		var (
			intValue     = 1
			int8Value    = int8(1)
			int16Value   = int16(1)
			int32Value   = int32(1)
			int64Value   = int64(1)
			uintValue    = uint(1)
			uint8Value   = uint8(1)
			uint16Value  = uint16(1)
			uint32Value  = uint32(1)
			uint64Value  = uint64(1)
			float32Value = float32(1)
			float64Value = float64(1)
			boolValue    = true
			bytesValue   = []byte("hello")
			stringValue  = "world"
			timeValue    = time.Now()
		)
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)

		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		var foundValue PtrType
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, NewQueryBuilder("ptr", driver.Adapter).Eq("id", uint64(1)), &foundValue))

		if foundValue.id == 0 {
			t.Fatal("cannot find value")
		}
		builder := NewQueryBuilder("ptr", driver.Adapter).Eq("id", foundValue.id)
		t.Run("not pointer value map", func(t *testing.T) {
			updateMap := map[string]interface{}{
				"intptr":     intValue,
				"int8ptr":    int8Value,
				"int16ptr":   int16Value,
				"int32ptr":   int32Value,
				"int64ptr":   int64Value,
				"uintptr":    uintValue,
				"uint8ptr":   uint8Value,
				"uint16ptr":  uint16Value,
				"uint32ptr":  uint32Value,
				"uint64ptr":  uint64Value,
				"float32ptr": float32Value,
				"float64ptr": float64Value,
				"boolptr":    boolValue,
				"bytesptr":   bytesValue,
				"stringptr":  stringValue,
				"timeptr":    timeValue,
			}
			NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateMap))
		})
		t.Run("pointer value map", func(t *testing.T) {
			updateMap := map[string]interface{}{
				"intptr":     &intValue,
				"int8ptr":    &int8Value,
				"int16ptr":   &int16Value,
				"int32ptr":   &int32Value,
				"int64ptr":   &int64Value,
				"uintptr":    &uintValue,
				"uint8ptr":   &uint8Value,
				"uint16ptr":  &uint16Value,
				"uint32ptr":  &uint32Value,
				"uint64ptr":  &uint64Value,
				"float32ptr": &float32Value,
				"float64ptr": &float64Value,
				"boolptr":    &boolValue,
				"bytesptr":   &bytesValue,
				"stringptr":  &stringValue,
				"timeptr":    &timeValue,
			}
			NoError(t, slc.UpdateByQueryBuilder(context.Background(), tx, builder, updateMap))
		})

		NoError(t, tx.Commit())
	})

	t.Run("some queries", func(t *testing.T) {

		columns := []string{
			"intptr",
			"int8ptr",
			"int16ptr",
			"int32ptr",
			"int64ptr",
			"uintptr",
			"uint8ptr",
			"uint16ptr",
			"uint32ptr",
			"uint64ptr",
			"float32ptr",
			"float64ptr",
			"boolptr",
			"bytesptr",
			"stringptr",
			"timeptr",
		}
		{
			txConn, err := conn.Begin()
			NoError(t, err)

			alterfmt := map[database.DBType]string{
				database.MySQL:    "ALTER TABLE ptr ADD INDEX idx_%d(%s)",
				database.Postgres: "CREATE INDEX idx_%d ON ptr (%s)",
			}
			for idx, column := range columns {
				if _, err := txConn.Exec(fmt.Sprintf(alterfmt[driver.DBType], idx+1, column)); err != nil {
					t.Fatalf("%+v", err)
				}
			}
			NoError(t, txConn.Commit())
		}
		txConn, err := conn.Begin()
		NoError(t, err)
		NoError(t, slc.WarmUp(conn))
		fmt.Println("WARM UP END")
		tx, err := cache.Begin(txConn)
		fmt.Println("BEGIN END")
		NoError(t, err)

		var ptr PtrType
		builder := NewQueryBuilder("ptr", driver.Adapter).Eq("id", uint64(2))
		NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &ptr))
		t.Run("pointer value query", func(t *testing.T) {
			builders := []*QueryBuilder{
				NewQueryBuilder("ptr", driver.Adapter).Eq("id", &ptr.id),
				NewQueryBuilder("ptr", driver.Adapter).Eq("intptr", ptr.intPtr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("int8ptr", ptr.int8Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("int16ptr", ptr.int16Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("int32ptr", ptr.int32Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("int64ptr", ptr.int64Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("uintptr", ptr.uintPtr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("uint8ptr", ptr.uint8Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("uint16ptr", ptr.uint16Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("uint32ptr", ptr.uint32Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("uint64ptr", ptr.uint64Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("float32ptr", ptr.float32Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("float64ptr", ptr.float64Ptr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("boolptr", ptr.boolPtr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("bytesptr", ptr.bytesPtr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("stringptr", ptr.stringPtr),
				NewQueryBuilder("ptr", driver.Adapter).Eq("timeptr", ptr.timePtr),
			}
			for _, builder := range builders {
				var v PtrType
				NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))
				NotEqualf(t, v.id, uint64(0), "cannot find by pointer value query")
			}
		})
		t.Run("IN condition query", func(t *testing.T) {
			builders := []*QueryBuilder{
				NewQueryBuilder("ptr", driver.Adapter).In("intptr", []int{1}),
				NewQueryBuilder("ptr", driver.Adapter).In("int8ptr", []int8{2}),
				NewQueryBuilder("ptr", driver.Adapter).In("int16ptr", []int16{3}),
				NewQueryBuilder("ptr", driver.Adapter).In("int32ptr", []int32{4}),
				NewQueryBuilder("ptr", driver.Adapter).In("int64ptr", []int64{5}),
				NewQueryBuilder("ptr", driver.Adapter).In("uintptr", []uint{6}),
				NewQueryBuilder("ptr", driver.Adapter).In("uint8ptr", []uint8{7}),
				NewQueryBuilder("ptr", driver.Adapter).In("uint16ptr", []uint16{8}),
				NewQueryBuilder("ptr", driver.Adapter).In("uint32ptr", []uint32{9}),
				NewQueryBuilder("ptr", driver.Adapter).In("uint64ptr", []uint64{10}),
				NewQueryBuilder("ptr", driver.Adapter).In("float32ptr", []float32{1.23}),
				NewQueryBuilder("ptr", driver.Adapter).In("float64ptr", []float64{4.56}),
				NewQueryBuilder("ptr", driver.Adapter).In("boolptr", []bool{true}),
				NewQueryBuilder("ptr", driver.Adapter).In("bytesptr", [][]byte{[]byte("bytes")}),
				NewQueryBuilder("ptr", driver.Adapter).In("stringptr", []string{"string"}),
				NewQueryBuilder("ptr", driver.Adapter).In("timeptr", []time.Time{*ptr.timePtr}),
			}
			for _, builder := range builders {
				var v PtrType
				NoError(t, slc.FindByQueryBuilder(context.Background(), tx, builder, &v))
				NotEqualf(t, v.id, uint64(0), "cannot find by IN query")
			}
		})
		NoError(t, tx.Rollback())
		{
			txConn, err := conn.Begin()
			NoError(t, err)

			alterfmt := map[database.DBType]string{
				database.MySQL:    "ALTER TABLE `ptr` DROP INDEX idx_%d",
				database.Postgres: "DROP INDEX idx_%d",
			}
			for idx := range columns {
				if _, err := txConn.Exec(fmt.Sprintf(alterfmt[driver.DBType], idx+1)); err != nil {
					t.Fatalf("%+v", err)
				}
			}
			NoError(t, txConn.Commit())
		}
	})
}

type StringCacheKey string

func (s StringCacheKey) String() string {
	return string(s)
}

func (s StringCacheKey) Hash() uint32 {
	return NewStringValue(string(s)).Hash()
}

func (s StringCacheKey) Type() server.CacheKeyType {
	return server.CacheKeyTypeLLC
}

func (s StringCacheKey) LockKey() server.CacheKey {
	return StringCacheKey(fmt.Sprintf("%s/lock", string(s)))
}

func (s StringCacheKey) Addr() net.Addr {
	return nil
}

func BenchmarkSLCIN_SimpleMemcachedAccess(b *testing.B) {
	if err := initCache(conn, CacheServerTypeMemcached); err != nil {
		panic(err)
	}
	setNopLogger()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	if err := slc.WarmUp(conn); err != nil {
		panic(err)
	}
	builder := NewQueryBuilder("user_logins", driver.Adapter).
		In("user_id", []uint64{1, 2, 3, 4, 5}).
		Eq("user_session_id", uint64(1))
	tx, err := cache.Begin(conn)
	if err != nil {
		panic(err)
	}
	var v UserLogins
	if err := slc.FindByQueryBuilder(context.Background(), tx, builder, &v); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := slc.cacheServer.GetMulti([]server.CacheKey{
			StringCacheKey("r/slc/user_logins/uq/user_id#1&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#2&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#3&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#4&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#5&user_session_id#1"),
		}); err != nil {
			panic(err)
		}
		if _, err := slc.cacheServer.GetMulti([]server.CacheKey{
			StringCacheKey("r/slc/user_logins/id#1"),
			StringCacheKey("r/slc/user_logins/id#2"),
			StringCacheKey("r/slc/user_logins/id#3"),
			StringCacheKey("r/slc/user_logins/id#4"),
			StringCacheKey("r/slc/user_logins/id#5"),
		}); err != nil {
			panic(err)
		}
	}
}

func BenchmarkSLCIN_SimpleRedisAccess(b *testing.B) {
	if err := initCache(conn, CacheServerTypeRedis); err != nil {
		panic(err)
	}
	setNopLogger()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	if err := slc.WarmUp(conn); err != nil {
		panic(err)
	}
	builder := NewQueryBuilder("user_logins", driver.Adapter).
		In("user_id", []uint64{1, 2, 3, 4, 5}).
		Eq("user_session_id", uint64(1))
	tx, err := cache.Begin(conn)
	if err != nil {
		panic(err)
	}
	var v UserLogins
	if err := slc.FindByQueryBuilder(context.Background(), tx, builder, &v); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := slc.cacheServer.GetMulti([]server.CacheKey{
			StringCacheKey("r/slc/user_logins/uq/user_id#1&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#2&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#3&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#4&user_session_id#1"),
			StringCacheKey("r/slc/user_logins/uq/user_id#5&user_session_id#1"),
		}); err != nil {
			panic(err)
		}
		if _, err := slc.cacheServer.GetMulti([]server.CacheKey{
			StringCacheKey("r/slc/user_logins/id#1"),
			StringCacheKey("r/slc/user_logins/id#2"),
			StringCacheKey("r/slc/user_logins/id#3"),
			StringCacheKey("r/slc/user_logins/id#4"),
			StringCacheKey("r/slc/user_logins/id#5"),
		}); err != nil {
			panic(err)
		}
	}
}

func BenchmarkSLCIN_MySQL(b *testing.B) {
	b.ResetTimer()
	query := fmt.Sprintf("select id,user_id,user_session_id,login_param_id,name,created_at,updated_at from user_logins where user_id IN (1, 2, 3, 4, 5) AND user_session_id = 1")
	userLogins := []*UserLogin{}
	for n := 0; n < b.N; n++ {
		func() {
			rows, err := conn.Query(query)
			if err != nil {
				panic(err)
			}
			defer func() {
				if err := rows.Close(); err != nil {
					panic(err)
				}
			}()
			for rows.Next() {
				var (
					id            uint64
					userID        uint64
					userSessionID uint64
					loginParamID  uint64
					name          string
					createdAt     time.Time
					updatedAt     time.Time
				)
				if err := rows.Scan(&id, &userID, &userSessionID, &loginParamID, &name, &createdAt, &updatedAt); err != nil {
					panic(err)
				}
				userLogins = append(userLogins, &UserLogin{
					ID:            id,
					UserID:        userID,
					UserSessionID: userSessionID,
					LoginParamID:  loginParamID,
					Name:          name,
					CreatedAt:     &createdAt,
					UpdatedAt:     &updatedAt,
				})
			}
		}()
	}
}

func BenchmarkSLCIN_Rapidash_Memcached(b *testing.B) {
	if err := initCache(conn, CacheServerTypeMemcached); err != nil {
		panic(err)
	}
	benchmarkSLCINRapidash(b)
}

func BenchmarkSLCIN_Rapidash_Redis(b *testing.B) {
	if err := initCache(conn, CacheServerTypeRedis); err != nil {
		panic(err)
	}
	benchmarkSLCINRapidash(b)
}

func benchmarkSLCINRapidash(b *testing.B) {
	if err := initUserLoginTable(conn); err != nil {
		panic(err)
	}
	setNopLogger()
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	if err := slc.WarmUp(conn); err != nil {
		panic(err)
	}
	b.ResetTimer()
	builder := NewQueryBuilder("user_logins", driver.Adapter).
		In("user_id", []uint64{1, 2, 3, 4, 5}).
		Eq("user_session_id", uint64(1))
	userLogins := []*UserLogin{}
	for n := 0; n < b.N; n++ {
		tx, err := cache.Begin(conn)
		if err != nil {
			panic(err)
		}
		var v UserLogins
		if err := slc.FindByQueryBuilder(context.Background(), tx, builder, &v); err != nil {
			panic(err)
		}
		userLogins = append(userLogins, v...)
		if err := tx.Commit(); err != nil {
			panic(err)
		}
	}
	if len(userLogins) != b.N*5 {
		panic("invalid user_login number")
	}
}

func TestCountQuerySLC(t *testing.T) {
	slc := NewSecondLevelCache(userLoginType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))
	builder := NewQueryBuilder("user_logins", driver.Adapter).
		Eq("user_id", uint64(1))
	tx, err := cache.Begin(conn)
	if err != nil {
		panic(err)
	}
	count, err := slc.CountByQueryBuilder(context.Background(), tx, builder)
	NoError(t, err)
	if count != 1 {
		t.Fatal("cannot work count")
	}
}

func TestCountByQueryBuilderCaseDatabaseRecordIsEmptySLC(t *testing.T) {
	slc := NewSecondLevelCache(emptyType(), cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
	NoError(t, slc.WarmUp(conn))
	builder := NewQueryBuilder("empties", driver.Adapter).Eq("id", uint64(1))
	tx, err := cache.Begin(conn)
	if err != nil {
		panic(err)
	}
	count, err := slc.CountByQueryBuilder(context.Background(), tx, builder)
	NoError(t, err)
	if count != 0 {
		t.Fatal("cannot work count")
	}
}

func TestWarmUp(t *testing.T) {
	NoError(t, initTable(conn, "warm_up_users"))
	f, err := os.Open(filepath.Join("testdata", driver.Name, "alter_warm_up_users.sql"))
	NoError(t, err)
	defer f.Close()
	queryScanner := bufio.NewScanner(f)

	strc := NewStruct("warm_up_users").
		FieldUint64("id").
		FieldUint64("user_id").
		FieldUint64("nickname").
		FieldUint64("age").
		FieldUint64("created_at")

	t.Run("only a single pk", func(t *testing.T) {
		slc := NewSecondLevelCache(strc, cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))

		Equal(t, len(slc.indexes), 1)
		v, exists := slc.indexes["id"]
		Equal(t, exists, true)
		Equal(t, len(v.Columns), 1)
		Equal(t, v.Type, IndexTypePrimaryKey)
		Equal(t, v.Columns[0], "id")

		t.Run("with shard_key", func(t *testing.T) {
			shardKey := "user_id"
			slc := NewSecondLevelCache(strc, cache.cacheServer, TableOption{shardKey: &shardKey}, database.NewAdapterWithDBType(driver.DBType))
			NoError(t, slc.WarmUp(conn))

			Equal(t, len(slc.indexes), 1)
			v, exists := slc.indexes["id:user_id"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 2)
			Equal(t, v.Type, IndexTypePrimaryKey)
			Equal(t, v.Columns[0], "id")
			Equal(t, v.Columns[1], "user_id")
		})
	})

	t.Run("pk multiple pair", func(t *testing.T) {
		queryScanner.Scan()
		_, err := conn.Exec(queryScanner.Text())
		NoError(t, err)
		slc := NewSecondLevelCache(strc, cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))
		Equal(t, len(slc.indexes), 2)
		{
			v, exists := slc.indexes["id"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 1)
			Equal(t, v.Type, IndexTypeKey)
			Equal(t, v.Columns[0], "id")
		}
		{
			v, exists := slc.indexes["id:created_at"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 2)
			Equal(t, v.Type, IndexTypePrimaryKey)
			Equal(t, v.Columns[0], "id")
			Equal(t, v.Columns[1], "created_at")
		}

		t.Run("with shard_key", func(t *testing.T) {
			shardKey := "user_id"
			slc := NewSecondLevelCache(strc, cache.cacheServer, TableOption{shardKey: &shardKey}, database.NewAdapterWithDBType(driver.DBType))
			NoError(t, slc.WarmUp(conn))

			Equal(t, len(slc.indexes), 2)
			{
				v, exists := slc.indexes["id:user_id"]
				Equal(t, exists, true)
				Equal(t, len(v.Columns), 2)
				Equal(t, v.Type, IndexTypeKey)
				Equal(t, v.Columns[0], "id")
				Equal(t, v.Columns[1], "user_id")
			}
			{
				v, exists := slc.indexes["id:created_at:user_id"]
				Equal(t, exists, true)
				Equal(t, len(v.Columns), 3)
				Equal(t, v.Type, IndexTypePrimaryKey)
				Equal(t, v.Columns[0], "id")
				Equal(t, v.Columns[1], "created_at")
				Equal(t, v.Columns[2], "user_id")
			}
		})
	})

	t.Run("index key", func(t *testing.T) {
		queryScanner.Scan()
		_, err := conn.Exec(queryScanner.Text())
		NoError(t, err)
		slc := NewSecondLevelCache(strc, cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))
		Equal(t, len(slc.indexes), 3)
		{
			v, exists := slc.indexes["id"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 1)
			Equal(t, v.Type, IndexTypePrimaryKey)
			Equal(t, v.Columns[0], "id")
		}
		{
			v, exists := slc.indexes["user_id"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 1)
			Equal(t, v.Type, IndexTypeKey)
			Equal(t, v.Columns[0], "user_id")
		}
		{
			v, exists := slc.indexes["user_id:nickname"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 2)
			Equal(t, v.Type, IndexTypeKey)
			Equal(t, v.Columns[0], "user_id")
			Equal(t, v.Columns[1], "nickname")
		}
	})

	t.Run("unique key", func(t *testing.T) {
		queryScanner.Scan()
		_, err := conn.Exec(queryScanner.Text())
		NoError(t, err)
		slc := NewSecondLevelCache(strc, cache.cacheServer, TableOption{}, database.NewAdapterWithDBType(driver.DBType))
		NoError(t, slc.WarmUp(conn))
		Equal(t, len(slc.indexes), 3)
		{
			v, exists := slc.indexes["id"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 1)
			Equal(t, v.Type, IndexTypePrimaryKey)
			Equal(t, v.Columns[0], "id")
		}
		{
			v, exists := slc.indexes["user_id"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 1)
			Equal(t, v.Type, IndexTypeKey)
			Equal(t, v.Columns[0], "user_id")
		}
		{
			v, exists := slc.indexes["user_id:nickname"]
			Equal(t, exists, true)
			Equal(t, len(v.Columns), 2)
			Equal(t, v.Type, IndexTypeUniqueKey)
			Equal(t, v.Columns[0], "user_id")
			Equal(t, v.Columns[1], "nickname")
		}
	})
}
