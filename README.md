# Rapidash [![GoDoc](https://godoc.org/go.knocknote.io/rapidash?status.svg)](https://godoc.org/go.knocknote.io/rapidash) [![CircleCI](https://circleci.com/gh/blastrain/rapidash.svg?style=shield&circle-token=ca664bdc3cf8add85ee60dab6dbdd32386b429c4)](https://circleci.com/gh/blastrain/rapidash) [![codecov](https://codecov.io/gh/blastrain/rapidash/branch/master/graph/badge.svg?token=k1H9XkVVgn)](https://codecov.io/gh/blastrain/rapidash) [![README_JP](https://img.shields.io/badge/readme-JP-red.svg)](https://github.com/blastrain/rapidash/blob/master/README.ja.md)

<img width="300" alt="2" src="https://user-images.githubusercontent.com/209884/34214640-697191ce-e5e6-11e7-8a12-c1cd45af2320.png">

`Rapidash` is a Go package for the database record or other data caching.  
It not only supports `memcached` or `Redis` for generic caching (e.g. get/set ) 
but also supports fast access to read-only data by fetching all read-only records from the database and caching them to memory on application.  
Also, It supports Read-Through / Write-Through caching for read/write records on the database.

Main features are the following.

# 1. Features

- Fetches all read-only records from database at application startup and according to the index definition it expand as `B+Tree` structure on the memory. To get caching data, can use Query Builder and it available range searching.
- Caching read/write table records to `memcached` or `Redis` for searching records fastly or load balancing database.
- Supports generic caching (e.g. get/set ) for `memcached` or `Redis`
- Supports transaction for caching
- Supports select caching server by cache-key pattern from multiple cache servers
- Supports Consistent Hashing for distributed caching
- Fast encoding/decoding without `reflect` package
- Compress caching data by `msgpack`

Also, Rapidash has beautiful access log visualizer. 
It visualize query and value between stash ( on the application ) and caching server and database like the following.

### Visualize by HTML
<img width="600" alt="スクリーンショット 2019-08-15 22 40 15" src="https://user-images.githubusercontent.com/209884/63098838-c0b7da80-bfae-11e9-9f8d-02cf3190d68d.png">

### Visualize by Console

<img width="600" alt="スクリーンショット 2019-08-15 22 47 09" src="https://user-images.githubusercontent.com/209884/63098848-c6152500-bfae-11e9-9b32-d37cd7c18839.png">

`Rapidash` has three components.  
First, we call component for caching the read-only records `FirstLevelCache`.  
Second, we call component for caching the read/write records `SecondLevelCache`.  
Finaly, we call component for generic caching `LastLevelCache`.

# 2. Benchmarks

## 2.1. FirstLevelCache Benchmarks

### 2.1.1. SELECT

By Primary Key
```
database/sql   200           9596890 ns/op          180199 B/op       4594 allocs/op
rapidash     50000             43565 ns/op           10734 B/op        100 allocs/op
```

**x250** faster than `database/sql`

By Multiple Primary Keys ( `IN` query )

```
database/sql   100          13149288 ns/op          423101 B/op      13500 allocs/op
rapidash      5000            273461 ns/op          114952 B/op       2500 allocs/op
```

**x50** faster than `database/sql`

## 2.2. SecondLevelCache Benchmarks

### 2.2.1. SELECT

Select by PRIMARY INDEX ( like `SELECT * FROM table WHERE id = ?` )

```
database/sql                   10000            127838 ns/op            1443 B/op         41 allocs/op
gorm                           10000            163271 ns/op           10122 B/op        201 allocs/op
rapidash worst ( all miss hit)  5000            234159 ns/op            9948 B/op        240 allocs/op
rapidash best ( all hit )      30000             46576 ns/op            5339 B/op        120 allocs/op
```

If cache is all hits, **x3** faster than `datbase/sql`

### 2.2.2. INSERT

```
database/sql  3000            461925 ns/op            1235 B/op         25 allocs/op
gorm          3000            475054 ns/op            5831 B/op        118 allocs/op
rapidash      2000            602111 ns/op           13548 B/op        305 allocs/op
```

### 2.2.3. UPDATE

```
database/sql                    3000            502141 ns/op             676 B/op         17 allocs/op
gorm                            3000            553302 ns/op           11815 B/op        229 allocs/op
rapidash worst ( all miss hit ) 2000            775627 ns/op           12553 B/op        307 allocs/op
rapidash best ( all hit )       2000            594131 ns/op            8241 B/op        192 allocs/op
```

### 2.2.4. DELETE

```
database/sql  3000            485844 ns/op             579 B/op         17 allocs/op
gorm          3000            502079 ns/op            3789 B/op         80 allocs/op
rapidash      3000            543378 ns/op            3169 B/op         80 allocs/op
```

# 3. Install

## 3.1. Install as a Library

```
go get -u go.knocknote.io/rapidash
```

## 3.2 Install as a CLI tool

```
go get -u go.knocknote.io/rapidash/cmd/rapidash
```

# 4. Document

[GoDoc](https://godoc.org/go.knocknote.io/rapidash)



# 5. Usage

## 5.1 Fastly access to the read-only records

For example, if your application has immutable data-set by user actions like `master data` for gaming application, `Rapidash` fetch all them from database at application startup and according to the index definition they expand as `B+Tree` structure.

For example, we create `events` table on the database and insert some records to it.

```sql
CREATE TABLE events (
  id bigint(20) unsigned NOT NULL,
  event_id bigint(20) unsigned NOT NULL,
  term enum('early_morning', 'morning', 'daytime', 'evening', 'night', 'midnight') NOT NULL,
  start_week int(10) unsigned NOT NULL,
  end_week int(10) unsigned NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (event_id, start_week)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

For caching records of `events` table, we could write the following.

```go
package main

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.knocknote.io/rapidash"
)

// Go structure for schema of `events`
type Event struct {
	ID        int64
	EventID   int64
	Term      string
	StartWeek uint8
	EndWeek   uint8
	CreatedAt time.Time
	UpdatedAt time.Time
}

// For decoding record
func (e *Event) DecodeRapidash(dec rapidash.Decoder) error {
	e.ID = dec.Int64("id")
	e.EventID = dec.Int64("event_id")
	e.Term = dec.String("term")
	e.StartWeek = dec.Uint8("start_week")
	e.EndWeek = dec.Uint8("end_week")
	e.CreatedAt = dec.Time("created_at")
	e.UpdatedAt = dec.Time("updated_at")
	return dec.Error()
}

// Map column of `events` table to Go type
func (e *Event) Struct() *rapidash.Struct {
	return rapidash.NewStruct("events").
		FieldInt64("id").
		FieldInt64("event_id").
		FieldString("term").
		FieldUint8("start_week").
		FieldUint8("end_week").
		FieldTime("created_at").
		FieldTime("updated_at")
}

func main() {
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}

	// Create `*rapidash.Rapidash` instance
	cache, err := rapidash.New()
	if err != nil {
		panic(err)
	}
	
	// Cache all records on the `events` table
	if err := cache.WarmUp(conn, new(Event).Struct(), true); err != nil {
		panic(err)
	}

	// Create `*rapidash.Tx` instance from `*rapidash.Rapidash`
	tx, err := cache.Begin()
	if err != nil {
		panic(err)
	}

	// SELECT * FROM events
	//   WHERE `event_id` = 1 AND
	//      `start_week` <= 3 AND
	//      `end_week` >= 3   AND
	//      `term` = daytime
	builder := rapidash.NewQueryBuilder("events").
		Eq("event_id", uint64(1)).
		Lte("start_week", uint8(3)).
		Gte("end_week", uint8(3)).
		Eq("term", "daytime")
	var event Event
	if err := tx.FindByQueryBuilder(builder, &event); err != nil {
		panic(err)
	}
}
```


## 5.2 Fastly access to the read/write records

```sql
CREATE TABLE IF NOT EXISTS user_logins (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  user_id bigint(20) unsigned NOT NULL,
  user_session_id bigint(20) unsigned NOT NULL,
  login_param_id bigint(20) unsigned NOT NULL,
  name varchar(255) NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (user_id, user_session_id),
  KEY (user_id, login_param_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

For example, we create `user_logins` table on the database and insert some records to it.
For caching records of `user_logins` table, we could write the following.

※ Previously, we start `memcached` with `11211` port.

```go
package main

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.knocknote.io/rapidash"
)

// Go structure for schema of `user_logins`
type UserLogin struct {
	ID            int64
	UserID        int64
	UserSessionID int64
	LoginParamID  int64
	Name          string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// For encoding record
func (u *UserLogin) EncodeRapidash(enc Encoder) error {
	if u.ID != 0 {
		enc.Int64("id", u.ID)
	}
	enc.Int64("user_id", u.UserID)
	enc.Int64("user_session_id", u.UserSessionID)
	enc.Int64("login_param_id", u.LoginParamID)
	enc.String("name", u.Name)
	enc.Time("created_at", u.CreatedAt)
	enc.Time("updated_at", u.UpdatedAt)
	return enc.Error()
}

// For decoding record
func (u *UserLogin) DecodeRapidash(dec Decoder) error {
	u.ID = dec.Int64("id")
	u.UserID = dec.Int64("user_id")
	u.UserSessionID = dec.Int64("user_session_id")
	u.LoginParamID = dec.Int64("login_param_id")
	u.Name = dec.String("name")
	u.CreatedAt = dec.Time("created_at")
	u.UpdatedAt = dec.Time("updated_at")
	return dec.Error()
}

// Map column of `user_logins` table to Go type
func (u *UserLogin) Struct() *rapidash.Struct {
	return rapidash.NewStruct("user_logins").
		FieldInt64("id").
		FieldInt64("user_id").
		FieldInt64("user_session_id").
		FieldInt64("login_param_id").
		FieldString("name").
		FieldTime("created_at").
		FieldTime("updated_at")
}

func main() {
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}

	// Create `*rapidash.Rapidash` instance with ServerAddrs option
	cache, err := rapidash.New(rapidash.ServerAddrs([]string{"localhost:11211"}))
	if err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(UserLogin).Struct(), false); err != nil {
		panic(err)
	}

	// Create `*sql.Tx` instance
	txConn, err := conn.Begin()
	if err != nil {
		panic(err)
	}
	// Create `*rapidash.Tx` instance from `*sql.Tx`
	tx, err := cache.Begin(txConn)
	if err != nil {
		panic(err)
	}

	// SELECT * FROM user_logins
	//   WHERE `user_id` = 1 AND `user_session_id` = 1
	builder := rapidash.NewQueryBuilder("user_logins").
		Eq("user_id", int64(1)).
		Eq("user_session_id", int64(1))

	// Search from memcached first, fetch it from database if without cache on memcached
	var userLogin UserLogin
	if err := tx.FindByQueryBuilder(builder, &userLogin); err != nil {
		panic(err)
	}

	// Set cache to memcached
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}
```

## 5.3 Generic caching

### 5.3.1 Encode/Decode

#### Primitive Types

`int` , `int8` , `int16` , `int32` , `int64` ,
`uint` , `uint8` , `uint16` , `uint32`, `uint64` ,
`float32`, `float64` , `[]byte` , `string` , `bool`

The above types can use API like `rapidash.Int(1)` and `rapidash.IntPtr(v)` ( ※ `v` is `*int` type ) for encoding and decoding .

#### Primitive Slice Types

`[]int` , `[]int8` , `[]int16` , `[]int32` , `[]int64` ,
`[]uint` , `[]uint8` , `[]uint16` , `[]uint32`, `[]uint64` ,
`[]float32`, `[]float64` , `[][]byte` , `[]string` , `[]bool`

The above types can use API like `rapidash.Ints([]int{1})` and `rapidash.IntsPtr(v)` ( ※ `v` is `*[]int` type ) for encoding and decoding .

#### Struct type

*Struct* type can encode/decode by `(*rapidash.Struct).Cast()` like the following.

```go

type User struct {
  ID int64
  Name string
}

func (u *User) EncodeRapidash(enc rapidash.Encoder) error {
    enc.Int64("id", u.ID)
    enc.String("name", u.Name)
    return enc.Error()
}

func (u *User) DecodeRapidash(dec rapidash.Decoder) error {
    u.ID = dec.Int64("id")
    u.Name = dec.String("name")
    return dec.Error()
}

func (u *User) Struct() *rapidash.Struct {
    return rapidash.NewStruct("users").FieldInt64("id").FieldString("name")
}

UserType := new(User).Struct()
tx.Create("user", UserType.Cast(&User{ID: 1, Name: "john"})) // encode

var user User
tx.Find("user", UserType.Cast(&user)) // decode
```

#### Struct Slice Type

*Struct Slice* type can encode/decode by `rapidash.Structs()` like the following.

```go

type Users []*User

func (u *Users) EncodeRapidash(enc rapidash.Encoder) error {
	for _, v := range *u {
		if err := v.EncodeRapidash(enc.New()); err != nil {
			return err
		}
	}
	return nil
}

func (u *Users) DecodeRapidash(dec rapidash.Decoder) error {
	len := dec.Len()
	*u = make([]*User, len)
	for i := 0; i < len; i++ {
		var v User
		if err := v.DecodeRapidash(dec.At(i)); err != nil {
			return err
		}
		(*u)[i] = &v
	}
	return nil
}

UserType := new(User).Struct()
users := Users{}
users = append(users, &User{ID: 1, Name: "john"})
tx.Create("user", rapidash.Structs(users, UserType)) // encode

var u Users
tx.Find("user", rapidash.Structs(&u, UserType)) // decode
```


### 5.3.2 Example

※ Previously, we start `memcached` with `11211` port.

```go
package main

import (
	"go.knocknote.io/rapidash"
)

func main() {
	// Create `*rapidash.Rapidash` instance with ServerAddrs option
	cache, err := rapidash.New(rapidash.ServerAddrs([]string{"localhost:11211"}))
	if err != nil {
		panic(err)
	}
	tx, err := cache.Begin()
	if err != nil {
		panic(err)
	}
	
	// Create cache for int value
	if err := tx.Create("key", rapidash.Int(1)); err != nil {
		panic(err)
	}

	// Get cache for int value
	var v int
	if err := tx.Find("key", rapidash.IntPtr(&v)); err != nil {
		panic(err)
	}

	// Set cache to memcached
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}
```

# 6. Committers

- Masaaki Goshima ( [@goccy](https://github.com/goccy) )
- Sota Itoh ( [@kanataxa](https://github.com/kanataxa) )
- Yoichi Tatsuzumi ( [@TatsuNet](https://github.com/TatsuNet) )

# 7. LICENSE
MIT
