package rapidash

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"go.knocknote.io/rapidash/database"
	"golang.org/x/xerrors"
)

var (
	conn  *sql.DB
	cache *Rapidash
)

var (
	drivers = map[string]string{
		"mysql":    "root:@tcp(localhost:3306)/rapidash?parseTime=true",
		"postgres": "host=localhost user=root dbname=rapidash sslmode=disable",
	}
	driver = os.Getenv("RAPIDASH_DB_DRIVER")
)

func setUp(conn *sql.DB) error {
	if err := initDB(); err != nil {
		return xerrors.Errorf("failed to initDB: %w", err)
	}
	if err := initEventTable(conn); err != nil {
		return xerrors.Errorf("failed to initEventTable: %w", err)
	}
	if err := initUserLoginTable(conn); err != nil {
		return xerrors.Errorf("failed to initUserLoginTable: %w", err)
	}
	if err := initPtrTable(conn); err != nil {
		return xerrors.Errorf("failed to initPtrTable: %w", err)
	}
	if err := initUserLogTable(conn); err != nil {
		return xerrors.Errorf("failed to initUserLogTable: %w", err)
	}
	if err := initEmptyTable(conn); err != nil {
		return xerrors.Errorf("failed to initEmptyTable: %w", err)
	}
	if err := initCache(conn, CacheServerTypeMemcached); err != nil {
		return xerrors.Errorf("failed to initCache: %w", err)
	}
	return nil
}

func initDB() error {
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/?parseTime=true")
	if err != nil {
		return xerrors.Errorf("failed to open database connection: %w", err)
	}
	if _, err := conn.Exec("CREATE DATABASE IF NOT EXISTS rapidash"); err != nil {
		return xerrors.Errorf("failed to create database for test: %w", err)
	}
	return nil
}

func initTable(conn *sql.DB, tableName string) error {
	sql, err := ioutil.ReadFile(filepath.Join("testdata", driver, tableName+".sql"))
	if err != nil {
		return xerrors.Errorf("failed to read sql file: %w", err)
	}
	queries := strings.Split(string(sql), ";")
	for _, query := range queries[:len(queries)-1] {
		if _, err := conn.Exec(query); err != nil {
			return xerrors.Errorf("failed to exec query: %w", err)
		}
	}
	return nil
}

func initEventTable(conn *sql.DB) error {
	if err := initTable(conn, "events"); err != nil {
		return xerrors.Errorf("failed to init events: %w", err)
	}
	id := 1
	for eventID := 1; eventID <= 1000; eventID++ {
		startWeek := 1
		endWeek := 12
		term := "daytime"
		eventCategoryID := eventID
		for j := 0; j < 4; j++ {
			if _, err := conn.Exec("insert into events values(?, ?, ?, ?, ?, ?, ?, ?)", id, eventID, eventCategoryID, term, startWeek, endWeek, time.Now(), time.Now()); err != nil {
				return xerrors.Errorf("failed to insert into events table: %w", err)
			}
			id++
			startWeek += 12
			endWeek += 12
		}
	}
	return nil
}

func initUserLoginTable(conn *sql.DB) error {
	if err := initTable(conn, "user_logins"); err != nil {
		return xerrors.Errorf("failed to exec user_logins: %w", err)
	}
	userID := 1
	userSessionID := 1
	loginParamID := 1
	name := "rapidash1"
	for ; userID <= 1000; userID++ {
		if _, err := conn.Exec("INSERT INTO `user_logins` (`user_id`,`user_session_id`,`login_param_id`,`name`,`created_at`,`updated_at`) VALUES (?, ?, ?, ?, ?, ?)",
			userID, userSessionID, loginParamID, name, time.Now(), time.Now()); err != nil {
			return xerrors.Errorf("failed to insert into user_logins table: %w", err)
		}
	}
	return nil
}

func initPtrTable(conn *sql.DB) error {
	if err := initTable(conn, "ptr"); err != nil {
		return xerrors.Errorf("failed to exec ptr: %w", err)
	}
	if _, err := conn.Exec("INSERT INTO `ptr` () values ()"); err != nil {
		return xerrors.Errorf("failed to insert empty record to ptr table: %w", err)
	}
	if _, err := conn.Exec(`
INSERT INTO ptr
 (
  intptr,
  int8ptr,
  int16ptr,
  int32ptr,
  int64ptr,
  uintptr,
  uint8ptr,
  uint16ptr,
  uint32ptr,
  uint64ptr,
  float32ptr,
  float64ptr,
  boolptr,
  bytesptr,
  stringptr,
  timeptr
 )
  values
 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
`, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1.23, 4.56, true, "bytes", "string"); err != nil {
		return xerrors.Errorf("failed to insert default value to ptr table: %w", err)
	}
	return nil
}

func initUserLogTable(conn *sql.DB) error {
	if err := initTable(conn, "user_logs"); err != nil {
		return xerrors.Errorf("failed to exec user_logs: %w", err)
	}
	if _, err := conn.Exec("INSERT INTO `user_logs` (`user_id`,`content_type`,`content_id`,`created_at`,`updated_at`) VALUES (?, ?, ?, ?, ?)", 1, "rapidash", 1, time.Now(), time.Now()); err != nil {
		return xerrors.Errorf("failed to insert into user_logs table: %w", err)
	}

	return nil
}

func initEmptyTable(conn *sql.DB) error {
	if err := initTable(conn, "empties"); err != nil {
		return xerrors.Errorf("failed to exec empties: %w", err)
	}
	return nil
}

func initCache(conn *sql.DB, typ CacheServerType) error {
	var serverAddrs []string
	if typ == CacheServerTypeMemcached {
		serverAddrs = []string{"localhost:11211"}
	} else if typ == CacheServerTypeRedis {
		serverAddrs = []string{"localhost:6379"}
	} else {
		panic("not defined cache server type")
	}
	var err error
	cache, err = New(
		ServerType(typ),
		ServerAddrs(serverAddrs),
		LogMode(LogModeJSON),
		LogEnabled(true),
	)
	if err != nil {
		return xerrors.Errorf("failed to create rapidash instance: %w", err)
	}
	if err := cache.Flush(); err != nil {
		return xerrors.Errorf("failed to flush cache: %w", err)
	}

	if err := cache.WarmUp(conn, eventType(), true); err != nil {
		return xerrors.Errorf("failed to warm up cache: %w", err)
	}

	if err := cache.WarmUp(conn, userLoginType(), false); err != nil {
		return xerrors.Errorf("failed to warm up cache: %w", err)
	}

	if err := cache.WarmUp(conn, new(PtrType).Type(), true); err != nil {
		return xerrors.Errorf("failed to warm up cache: %w", err)
	}

	if err := cache.Ignore(conn, userLogType()); err != nil {
		return xerrors.Errorf("failed to ignore cache: %w", err)
	}

	cache.BeforeCommitCallback(func(tx *Tx, queries []*QueryLog) error {
		bytes, err := json.Marshal(queries)
		if err != nil {
			panic(err)
		}
		var logs []*QueryLog
		if err := json.Unmarshal(bytes, &logs); err != nil {
			panic(err)
		}
		return nil
	})
	cache.AfterCommitCallback(func(tx *Tx) error {
		return nil
	}, func(tx *Tx, queries []*QueryLog) error {
		return nil
	})

	return nil
}

func TestMain(m *testing.M) {
	source := drivers[driver]
	var err error
	conn, err = sql.Open(driver, source)
	if err != nil {
		panic(err)
	}
	if err := setUp(conn); err != nil {
		panic(err)
	}

	result := m.Run()
	os.Exit(result)

}

func TestBegin(t *testing.T) {
	t.Run("conn is nil", func(t *testing.T) {
		tx, err := cache.Begin(nil)
		NoError(t, err)
		NotEqualf(t, tx.ID(), "", "invalid tx id")
	})
	t.Run("invalid conn length", func(t *testing.T) {
		_, err := cache.Begin(conn, conn)
		if !xerrors.Is(err, ErrBeginTransaction) {
			t.Fatalf("%+v", err)
		}
	})
}

func TestTx_CreateByTableContext(t *testing.T) {
	t.Run("already committed", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		Equal(t, false, tx.IsCommitted())
		NoError(t, tx.Commit())

		Equal(t, true, tx.IsCommitted())
		userLogin := defaultUserLogin()
		if _, err := tx.CreateByTableContext(context.Background(), "user_logins", userLogin); err != nil {
			if !xerrors.Is(err, ErrAlreadyCommittedTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error")
		}
	})
	t.Run("create by flc table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		if _, err := tx.CreateByTableContext(context.Background(), "events", nil); err == nil {
			t.Fatal("err is nil")
		}
	})
	t.Run("create by ignore cache table", func(t *testing.T) {
		NoError(t, cache.Ignore(conn, userLoginType()))
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		userLogin := defaultUserLogin()
		userLogin.ID = 0
		userLogin.UserSessionID = 1000
		id, err := tx.CreateByTableContext(context.Background(), "user_logins", userLogin)
		NoError(t, err)
		NotEqualf(t, id, 0, "last insert id is zero")
		NoError(t, tx.Commit())
		NoError(t, initCache(conn, CacheServerTypeMemcached))
	})
	t.Run("create by slc table", func(t *testing.T) {
		NoError(t, initUserLoginTable(conn))
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		userLogin := defaultUserLogin()
		userLogin.ID = 0
		userLogin.UserSessionID = 1000
		id, err := tx.CreateByTableContext(context.Background(), "user_logins", userLogin)
		NoError(t, err)
		NotEqualf(t, id, 0, "last insert id is zero")
		var findUserFromSLCByPrimaryKey UserLogin
		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", uint64(0))
		NoError(t, tx.FindByQueryBuilder(builder, &findUserFromSLCByPrimaryKey))
		Equal(t, findUserFromSLCByPrimaryKey.ID, userLogin.ID)
		var findUserFromSLCByUniqueKey UserLogin
		builder = NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("user_id", uint64(0)).Eq("user_session_id", uint64(1000))
		NoError(t, tx.FindByQueryBuilder(builder, &findUserFromSLCByUniqueKey))
		Equal(t, findUserFromSLCByPrimaryKey.ID, userLogin.ID)
		NoError(t, tx.Commit())
	})
	t.Run("create by unknown table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		if _, err := tx.CreateByTableContext(context.Background(), "rapidash", nil); err == nil {
			t.Fatalf("err is nil")
		}
	})
}

func TestTx_FindByQueryBuilderContext(t *testing.T) {
	t.Run("already committed", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		NoError(t, tx.Commit())

		builder := NewQueryBuilder("events", database.NewDBAdapter())
		var events EventSlice
		if err := tx.FindByQueryBuilderContext(context.Background(), builder, &events); err != nil {
			if !xerrors.Is(err, ErrAlreadyCommittedTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error")
		}
	})
	t.Run("find flc table", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("events", database.NewDBAdapter())
		var events EventSlice
		NoError(t, tx.FindByQueryBuilderContext(context.Background(), builder, &events))
		NoError(t, tx.Commit())
	})
	t.Run("find slc table", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", uint64(1))
		var userLogin UserLogin
		NoError(t, tx.FindByQueryBuilderContext(context.Background(), builder, &userLogin))
		NoError(t, tx.Commit())
	})
	t.Run("conn is nil", func(t *testing.T) {
		tx, err := cache.Begin()
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter())
		var userLogins UserLogins
		if err := tx.FindByQueryBuilderContext(context.Background(), builder, &userLogins); err != nil {
			if !xerrors.Is(err, ErrConnectionOfTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error")
		}
	})
	t.Run("unknown table name", func(t *testing.T) {
		tx, err := cache.Begin()
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("users", database.NewDBAdapter())
		var userLogins UserLogins
		if err := tx.FindByQueryBuilderContext(context.Background(), builder, &userLogins); err == nil {
			t.Fatal("err is nil\n")
		}
	})
	t.Run("find ignore table", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		builder := NewQueryBuilder("user_logs", database.NewDBAdapter()).Eq("id", uint64(1)).Gte("content_id", uint64(1)).Lte("content_id", uint64(1))
		var userLogs UserLogs
		NoError(t, tx.FindByQueryBuilderContext(context.Background(), builder, &userLogs))
		NoError(t, tx.Commit())
		if len(userLogs) != 1 &&
			userLogs[0].UserID != 1 &&
			userLogs[0].ContentID != 1 {
			t.Fatal("cannot work all sql")
		}
	})
}

func TestTx_CountByQueryBuilder(t *testing.T) {
	t.Run("count flc table", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("events", database.NewDBAdapter())
		count, err := tx.CountByQueryBuilder(builder)
		NoError(t, err)
		NotEqualf(t, count, 0, "failed count")
		NoError(t, tx.Commit())
	})
	t.Run("count slc table", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter())
		count, err := tx.CountByQueryBuilder(builder)
		NoError(t, err)
		NotEqualf(t, count, 0, "failed count")
		NoError(t, tx.Commit())
	})
	t.Run("unknown table)", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("unknown", database.NewDBAdapter())
		if _, err := tx.CountByQueryBuilder(builder); err == nil {
			t.Fatal("err is nil")
		}
	})
}

func TestTx_FindAllByTable(t *testing.T) {
	t.Run("findAll flc table", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("events", database.NewDBAdapter())
		count, err := tx.CountByQueryBuilder(builder)
		NoError(t, err)
		var events EventSlice
		NoError(t, tx.FindAllByTable("events", &events))

		Equalf(t, len(events), int(count), "invalid events length")

		builder = NewQueryBuilder("user_logins", database.NewDBAdapter())
		count, err = tx.CountByQueryBuilder(builder)
		NoError(t, err)
		var userLogins UserLogins
		NoError(t, tx.FindByQueryBuilder(builder, &userLogins))

		Equalf(t, len(userLogins), int(count), "invalid user_logins length")

		NoError(t, tx.Commit())
	})
	t.Run("findAll slc table(or unknown table)", func(t *testing.T) {
		tx, err := cache.Begin(conn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		var userLogins UserLogins
		Error(t, tx.FindAllByTable("user_logins", &userLogins))
	})
}

func TestTx_UpdateByQueryBuilder(t *testing.T) {
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

	findBuilder := NewQueryBuilder("user_logins", database.NewDBAdapter()).
		Eq("user_id", uint64(1)).
		Eq("user_session_id", uint64(1))
	var userLogin UserLogin
	NoError(t, tx.FindByQueryBuilder(findBuilder, &userLogin))
	NotEqualf(t, userLogin.ID, 0, "cannot find userLogin")

	builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", userLogin.ID)
	NoError(t, tx.UpdateByQueryBuilder(builder, map[string]interface{}{
		"login_param_id": uint64(10),
	}))
	NoError(t, tx.Commit())
}

func TestTx_UpdateByQueryBuilderContext(t *testing.T) {
	t.Run("already committed", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		NoError(t, tx.Commit())

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", uint64(1))
		if err := tx.UpdateByQueryBuilderContext(context.Background(), builder, map[string]interface{}{
			"login_param_id": uint64(10),
		}); err != nil {
			if !xerrors.Is(err, ErrAlreadyCommittedTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error\n")
		}
	})
	t.Run("update flc table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("events", database.NewDBAdapter()).Eq("id", uint64(1))
		var event Event
		NoError(t, tx.FindByQueryBuilder(builder, &event))
		NotEqualf(t, event.ID, 0, "cannot find event")

		if err := tx.UpdateByQueryBuilderContext(context.Background(), builder, map[string]interface{}{
			"start_week": uint8(10),
		}); err == nil {
			t.Fatal("err is nil")
		}
	})
	t.Run("conn is nil", func(t *testing.T) {
		tx, err := cache.Begin(nil)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", uint64(1))
		if err := tx.UpdateByQueryBuilderContext(context.Background(), builder, map[string]interface{}{
			"login_param_id": uint64(10),
		}); err != nil {
			if !xerrors.Is(err, ErrConnectionOfTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error")
		}
	})
	t.Run("update slc table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		findBuilder := NewQueryBuilder("user_logins", database.NewDBAdapter()).
			Eq("user_id", uint64(1)).
			Eq("user_session_id", uint64(1))
		var userLogin UserLogin
		NoError(t, tx.FindByQueryBuilder(findBuilder, &userLogin))
		NotEqualf(t, userLogin.ID, 0, "cannot find userLogin")

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", userLogin.ID)
		NoError(t, tx.UpdateByQueryBuilderContext(context.Background(), builder, map[string]interface{}{
			"login_param_id": uint64(10),
		}))
		NoError(t, tx.Commit())
	})
	t.Run("update unknown table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("rapidash", database.NewDBAdapter()).Eq("id", uint64(1))
		if err := tx.UpdateByQueryBuilderContext(context.Background(), builder, map[string]interface{}{
			"start_week": uint8(10),
		}); err == nil {
			t.Fatalf("err is nil")
		}
	})
}

func TestTx_DeleteByQueryBuilder(t *testing.T) {
	txConn, err := conn.Begin()
	NoError(t, err)
	tx, err := cache.Begin(txConn)
	NoError(t, err)
	defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

	findBuilder := NewQueryBuilder("user_logins", database.NewDBAdapter()).
		Eq("user_id", uint64(1)).
		Eq("user_session_id", uint64(1))
	var userLogin UserLogin
	NoError(t, tx.FindByQueryBuilder(findBuilder, &userLogin))
	NotEqualf(t, userLogin.ID, 0, "cannot find userLogin")

	builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", userLogin.ID)
	NoError(t, tx.DeleteByQueryBuilder(builder))
	NoError(t, tx.Commit())
}

func TestTx_DeleteByQueryBuilderContext(t *testing.T) {
	t.Run("already committed", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()
		NoError(t, tx.Commit())

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", uint64(1))
		if err := tx.DeleteByQueryBuilderContext(context.Background(), builder); err != nil {
			if !xerrors.Is(err, ErrAlreadyCommittedTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error")
		}
	})
	t.Run("update flc table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("events", database.NewDBAdapter()).Eq("id", uint64(1))
		var event Event
		NoError(t, tx.FindByQueryBuilder(builder, &event))
		NotEqualf(t, event.ID, 0, "cannot find event")

		if err := tx.DeleteByQueryBuilderContext(context.Background(), builder); err == nil {
			t.Fatalf("err is nil")
		}
	})
	t.Run("conn is nil", func(t *testing.T) {
		tx, err := cache.Begin(nil)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", uint64(1))
		if err := tx.DeleteByQueryBuilderContext(context.Background(), builder); err != nil {
			if !xerrors.Is(err, ErrConnectionOfTransaction) {
				t.Fatalf("unexpected type err: %+v", err)
			}
		} else {
			t.Fatal("required not nil error")
		}
	})
	t.Run("update slc table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		findBuilder := NewQueryBuilder("user_logins", database.NewDBAdapter()).
			Eq("user_id", uint64(1)).
			Eq("user_session_id", uint64(1))
		var userLogin UserLogin
		NoError(t, tx.FindByQueryBuilder(findBuilder, &userLogin))
		NotEqualf(t, userLogin.ID, 0, "cannot find userLogin")

		builder := NewQueryBuilder("user_logins", database.NewDBAdapter()).Eq("id", userLogin.ID)
		NoError(t, tx.DeleteByQueryBuilderContext(context.Background(), builder))
		NoError(t, tx.Commit())
	})
	t.Run("update unknown table", func(t *testing.T) {
		txConn, err := conn.Begin()
		NoError(t, err)
		tx, err := cache.Begin(txConn)
		NoError(t, err)
		defer func() { NoError(t, tx.RollbackUnlessCommitted()) }()

		builder := NewQueryBuilder("rapidash", database.NewDBAdapter()).Eq("id", uint64(1))
		if err := tx.DeleteByQueryBuilderContext(context.Background(), builder); err == nil {
			t.Fatalf("err is nil")
		}
	})
}
