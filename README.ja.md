# Rapidash [![README_EN](https://img.shields.io/badge/readme-EN-green.svg)](https://github.com/knocknote/rapidash/blob/master/README.md)

<img width="300" alt="2" src="https://user-images.githubusercontent.com/209884/34214640-697191ce-e5e6-11e7-8a12-c1cd45af2320.png">

`Rapidash` はGo言語から利用できる高機能なキャッシュライブラリです。  
`memcached` や `Redis` といった一般的なキャッシュサーバーを利用したキャッシュの利用はもちろん、  
データベース上の検索しか行わないテーブルのデータをアプリケーションのメモリ上に展開して高速にアクセスすることや、  
読み書きをおこなうテーブルのデータをデータベースから読み込むタイミングでキャッシュサーバーに格納し、以降の同じ検索クエリをキャッシュサーバーから取得することによってクエリの応答性能を上げたりデータベースの負荷を下げることができる機能(いわゆる `Read-Through/Write-Through` )などがあります。  

主要な機能を並べると以下のようになります。  

# 1. 主な機能

1. 検索しか行わないテーブルのデータをアプリケーション起動時にデータベースからすべて吸い上げ、インデックスの定義に従ってメモリ上にB＋Tree構造で展開する。検索時はクエリビルダーを通して行い、範囲検索も可能
2. 読み書きを行うテーブルのレコードを `memcached` や `Redis` といったキャッシュサーバーに格納し、高速に検索したり検索クエリの負荷分散をおこなう
3. `memcached` や `Redis` を利用した汎用的なキャッシュ操作
4. 2 や 3 を行う際に擬似的なトランザクションを利用できる
5. キャッシュサーバーが複数ある場合に、キーの種類によって格納するサーバーを制御する機能
6. Consistent Hashing によるキャッシュサーバーの増減に対する効率的な分散手法
7. `reflect` を使わない高速なエンコード・デコード処理
8. `msgpack` による値の自動圧縮

`Rapidash` の内部は、機能によって大きく3つのコンポーネントに分けられています。  
1 で挙げた、検索しか行わないテーブルのレコードをキャッシュするためのコンポーネントを `FirstLevelCache`   
2 で挙げた、読み書きを行うテーブルのレコードをキャッシュするためのコンポーネントを `SecondLevelCache`  
3 で挙げた、汎用的なキャッシュ操作をおこなうためのコンポーネントを `LastLevelCache` と呼んでいます。  

`Rapidash` が提供するインターフェースを利用する限りにおいて、これらのコンポーネントの存在を意識する必要はありませんが、
以降では、上記で挙げた機能の詳細について説明する際にこれらの名前を利用します。

# 2. インストール

```
go get -u go.knocknote.io/rapidash
```

# 3. ドキュメント

GoDoc は [こちら](https://godoc.org/go.knocknote.io/rapidash)

# 4. 使い方

## 4.1 検索のみのテーブルのデータを高速に検索する

アプリケーションには、マスターデータのようにユーザの行動起因で変化しないデータセットが存在する場合があります。  
そういった変化しないデータをアプリケーション起動時にキャッシュすることで、高速に検索するための仕組みを提供しています。  

例えば以下のような `events` テーブルがあり、あらかじめイベント情報を登録していたとします。

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

この `events` テーブルの情報をすべてキャッシュするには、以下のように記述します。

```go
package main

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.knocknote.io/rapidash"
)

// スキーマに対応するGo側の型を用意する
type Event struct {
	ID        int64
	EventID   int64
	Term      string
	StartWeek uint8
	EndWeek   uint8
	CreatedAt time.Time
	UpdatedAt time.Time
}

// デコードのためのメソッドを定義する
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

// events テーブルの各カラムとGoの型のマッピングを作成
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

	// Rapidash インスタンスの作成
	cache, err := rapidash.New()
	if err != nil {
		panic(err)
	}
	
	// events テーブルを参照できるコネクションをもった sql.DB と 型情報を利用して、メモリ上にキャッシュを構築
	if err := cache.WarmUp(conn, new(Event).Struct(), true); err != nil {
		panic(err)
	}

	// Rapidash インスタンスから `*rapidash.Tx` インスタンスを作成
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


## 4.2 読み書きを行うテーブルのデータを高速に検索する

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

上記のようなスキーマのテーブルのデータをキャッシュサーバに適宜格納・削除しながら高速に検索するには
以下のように記述します

※ 事前に `memcached` が `11211` ポートで起動済みとします

```go
package main

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.knocknote.io/rapidash"
)

// スキーマに対応するGo側の型を用意する
type UserLogin struct {
	ID            int64
	UserID        int64
	UserSessionID int64
	LoginParamID  int64
	Name          string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// エンコードするためのメソッドを定義する
func (u *UserLogin) EncodeRapidash(enc Encoder) error {
	if u.ID != 0 {
		enc.Int64("id", u.ID)
	}
	enc.Int64("user_id", u.UserID)
	enc.Int64("user_session_id", u.UserSessionID)
	enc.Int64("login_param_id", u.LoginParamID)
	enc.String("name", u.Name)
	enc.Time("created_at", u.CreatedAt)
	enc.Time("updated_at", u.UpdatedAt)
	return enc.Error()
}

// デコードするためのメソッドを定義する
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

// user_logins テーブルの各カラムとGoの型のマッピングを作成
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

	// Rapidash インスタンスの作成
	cache, err := rapidash.New(rapidash.ServerAddrs("localhost:11211"))
	if err != nil {
		panic(err)
	}
	// user_logins テーブルを参照できるコネクションをもった sql.DB と 型情報を利用してキャッシュを利用する準備をする
	if err := cache.WarmUp(conn, new(UserLogin).Struct(), false); err != nil {
		panic(err)
	}

	// sql.Tx の作成
	txConn, err := conn.Begin()
	if err != nil {
		panic(err)
	}
	// Rapidash インスタンスから `*rapidash.Tx` インスタンスを作成
	tx, err := cache.Begin(txConn)
	if err != nil {
		panic(err)
	}

	// SELECT * FROM user_logins
	//   WHERE `user_id` = 1 AND `user_session_id` = 1
	builder := rapidash.NewQueryBuilder("user_logins").
		Eq("user_id", int64(1)).
		Eq("user_session_id", int64(1))

	// キャッシュから検索し、なければデータベースから検索する
	var userLogin UserLogin
	if err := tx.FindByQueryBuilder(builder, &userLogin); err != nil {
		panic(err)
	}

	// 作成したキャッシュをキャッシュサーバーに保存
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}
```

## 4.3 任意の値のキャッシュの読み書きを行う

※ 事前に `memcached` が `11211` ポートで起動済みとします

```go
package main

import (
	"go.knocknote.io/rapidash"
)

func main() {
	// Rapidash インスタンスの作成
	cache, err := rapidash.New(rapidash.ServerAddrs("localhost:11211"))
	if err != nil {
		panic(err)
	}
	tx, err := cache.Begin()
	if err != nil {
		panic(err)
	}
	
	// int の値の読み書き
	if err := tx.Create("key", rapidash.Int(1)); err != nil {
		panic(err)
	}
	var v int
	if err := tx.Find("key", rapidash.IntPtr(&v)); err != nil {
		panic(err)
	}

	// 作ったキャッシュを保存する
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}
```

# 5. LICENSE
MIT
