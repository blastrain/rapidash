package rapidash

import (
	"database/sql"
	"testing"
	"time"

	"github.com/jinzhu/gorm"
)

type A struct {
	id        uint64
	uniqueID  uint64
	keyID     uint64
	name      string
	createdAt time.Time
	updatedAt time.Time
}

type B struct {
	ID        uint64    `gorm:"column:id"`
	UniqueID  uint64    `gorm:"column:unique_id"`
	KeyID     uint64    `gorm:"column:key_id"`
	Name      string    `gorm:"column:name"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func (b *B) TableName() string {
	return "a"
}

func (a *A) EncodeRapidash(enc Encoder) error {
	if a.id != 0 {
		enc.Uint64("id", a.id)
	}
	enc.Uint64("unique_id", a.uniqueID)
	enc.Uint64("key_id", a.keyID)
	enc.String("name", a.name)
	enc.Time("created_at", a.createdAt)
	enc.Time("updated_at", a.updatedAt)
	return enc.Error()
}

func (a *A) DecodeRapidash(dec Decoder) error {
	a.id = dec.Uint64("id")
	a.uniqueID = dec.Uint64("unique_id")
	a.keyID = dec.Uint64("key_id")
	a.name = dec.String("name")
	a.createdAt = dec.Time("created_at")
	a.updatedAt = dec.Time("updated_at")
	return nil
}

func (a *A) Type() *Struct {
	return NewStruct("a").
		FieldUint64("id").
		FieldUint64("unique_id").
		FieldUint64("key_id").
		FieldString("name").
		FieldTime("created_at").
		FieldTime("updated_at")
}

func resetTable() *sql.DB {
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	if _, err := conn.Exec("DROP TABLE IF EXISTS a"); err != nil {
		panic(err)
	}
	sql := `
CREATE TABLE IF NOT EXISTS a (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  unique_id bigint(20) unsigned NOT NULL,
  key_id bigint(20) unsigned NOT NULL,
  name varchar(255) NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (unique_id),
  KEY (key_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
`
	if _, err := conn.Exec(sql); err != nil {
		panic(err)
	}
	return conn
}

func insertRecord(b *testing.B) {
	query := `INSERT INTO a(id,unique_id,key_id,name,created_at,updated_at) VALUES (?, ?, ?, ?, ?, ?)`
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		if _, err := tx.Exec(query, id, id, id, "bench", time.Now(), time.Now()); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkGetByPrimaryKey_DatabaseSQL(b *testing.B) {
	resetTable()
	insertRecord(b)
	b.ResetTimer()
	query := `
        SELECT id,unique_id,key_id,name,created_at,updated_at
          FROM a
          WHERE id = ?`
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	records := make([]*A, b.N)
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		var (
			fetchID   uint64
			uniqueID  uint64
			keyID     uint64
			name      string
			createdAt time.Time
			updatedAt time.Time
		)
		if err := conn.QueryRow(query, id).Scan(&fetchID, &uniqueID, &keyID, &name, &createdAt, &updatedAt); err != nil {
			panic(err)
		}
		records[n] = &A{
			id:        fetchID,
			uniqueID:  uniqueID,
			keyID:     keyID,
			name:      name,
			createdAt: createdAt,
			updatedAt: updatedAt,
		}
		id++
	}
}

func BenchmarkGetByPrimaryKey_GORM(b *testing.B) {
	resetTable()
	insertRecord(b)
	db, err := gorm.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	b.ResetTimer()
	id := uint64(1)
	records := make([]*B, b.N)
	for n := 0; n < b.N; n++ {
		b := &B{
			ID: id,
		}
		db.First(&b)
		records[n] = b
		id++
	}
}

func BenchmarkGetByPrimaryKey_RapidashWorst(b *testing.B) {
	resetTable()
	insertRecord(b)
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	cache, err := New(
		ServerAddrs([]string{"localhost:11211"}),
		SecondLevelCachePessimisticLock(false),
		SecondLevelCacheOptimisticLock(false),
	)
	if err != nil {
		panic(err)
	}
	if err := cache.Flush(); err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(A).Type(), false); err != nil {
		panic(err)
	}
	b.ResetTimer()
	records := make([]*A, b.N)
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := cache.Begin(conn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		var a A
		if err := tx.FindByQueryBuilder(builder, &a); err != nil {
			panic(err)
		}
		records[n] = &a
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkGetByPrimaryKey_RapidashBest(b *testing.B) {
	resetTable()
	insertRecord(b)
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cache, err := New(
		ServerAddrs([]string{"localhost:11211"}),
		SecondLevelCachePessimisticLock(false),
		SecondLevelCacheOptimisticLock(false),
	)
	if err != nil {
		panic(err)
	}
	if err := cache.Flush(); err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(A).Type(), false); err != nil {
		panic(err)
	}
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := cache.Begin(conn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		var a A
		if err := tx.FindByQueryBuilder(builder, &a); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
	b.ResetTimer()
	records := make([]*A, b.N)
	id = uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := cache.Begin(conn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		var a A
		if err := tx.FindByQueryBuilder(builder, &a); err != nil {
			panic(err)
		}
		records[n] = &a
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkInsert_DatabaseSQL(b *testing.B) {
	resetTable()
	b.ResetTimer()
	query := `INSERT INTO a(id,unique_id,key_id,name,created_at,updated_at) VALUES (?, ?, ?, ?, ?, ?)`
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		if _, err := tx.Exec(query, id, id, id, "bench", time.Now(), time.Now()); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkInsert_GORM(b *testing.B) {
	resetTable()
	db, err := gorm.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	b.ResetTimer()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx := db.Begin()
		b := &B{
			ID:        id,
			UniqueID:  id,
			KeyID:     id,
			Name:      "bench",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if result := tx.Create(b); result.Error != nil {
			panic(result.Error)
		}
		if result := tx.Commit(); result.Error != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkInsert_Rapidash(b *testing.B) {
	resetTable()
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cache, err := New(
		ServerAddrs([]string{"localhost:11211"}),
		SecondLevelCachePessimisticLock(false),
		SecondLevelCacheOptimisticLock(false),
	)
	if err != nil {
		panic(err)
	}
	if err := cache.Flush(); err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(A).Type(), false); err != nil {
		panic(err)
	}
	b.ResetTimer()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		txConn, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		tx, err := cache.Begin(txConn)
		if err != nil {
			panic(err)
		}
		a := &A{
			id:        id,
			uniqueID:  id,
			keyID:     id,
			name:      "bench",
			createdAt: time.Now(),
			updatedAt: time.Now(),
		}
		if _, err := tx.CreateByTable("a", a); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkUpdateByPrimaryKey_DatabaseSQL(b *testing.B) {
	resetTable()
	insertRecord(b)
	b.ResetTimer()
	query := `UPDATE a SET name = ? WHERE id = ?`
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		if _, err := tx.Exec(query, "bench2", id); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkUpdateByPrimaryKey_GORM(b *testing.B) {
	resetTable()
	insertRecord(b)
	db, err := gorm.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	b.ResetTimer()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx := db.Begin()
		b := &B{
			ID: id,
		}
		if err := tx.Model(&b).Where("id = ?", id).Update("name", "bench2").Error; err != nil {
			panic(err)
		}
		if err := tx.Commit().Error; err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkUpdateByPrimaryKey_RapidashWorst(b *testing.B) {
	resetTable()
	insertRecord(b)
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cache, err := New(
		ServerAddrs([]string{"localhost:11211"}),
		SecondLevelCachePessimisticLock(false),
		SecondLevelCacheOptimisticLock(false),
	)
	if err != nil {
		panic(err)
	}
	if err := cache.Flush(); err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(A).Type(), false); err != nil {
		panic(err)
	}
	b.ResetTimer()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		txConn, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		tx, err := cache.Begin(txConn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		if err := tx.UpdateByQueryBuilder(builder, map[string]interface{}{
			"name": "bench2",
		}); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkUpdateByPrimaryKey_RapidashBest(b *testing.B) {
	resetTable()
	insertRecord(b)
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cache, err := New(
		ServerAddrs([]string{"localhost:11211"}),
		SecondLevelCachePessimisticLock(false),
		SecondLevelCacheOptimisticLock(false),
	)
	if err != nil {
		panic(err)
	}
	if err := cache.Flush(); err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(A).Type(), false); err != nil {
		panic(err)
	}
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := cache.Begin(conn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		var a A
		if err := tx.FindByQueryBuilder(builder, &a); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
	b.ResetTimer()
	id = uint64(1)
	for n := 0; n < b.N; n++ {
		txConn, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		tx, err := cache.Begin(txConn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		if err := tx.UpdateByQueryBuilder(builder, map[string]interface{}{
			"name": "bench2",
		}); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkDeleteByPrimaryKey_DatabaseSQL(b *testing.B) {
	resetTable()
	insertRecord(b)
	b.ResetTimer()
	query := `DELETE FROM a WHERE id = ?`
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		if _, err := tx.Exec(query, id); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkDeleteByPrimaryKey_GORM(b *testing.B) {
	resetTable()
	insertRecord(b)
	b.ResetTimer()
	db, err := gorm.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	b.ResetTimer()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		tx := db.Begin()
		b := &B{
			ID: id,
		}
		if err := tx.Delete(b).Error; err != nil {
			panic(err)
		}
		if err := tx.Commit().Error; err != nil {
			panic(err)
		}
		id++
	}
}

func BenchmarkDeleteByPrimaryKey_Rapidash(b *testing.B) {
	resetTable()
	insertRecord(b)
	conn, err := sql.Open("mysql", "root:@tcp(localhost:3306)/rapidash?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cache, err := New(
		ServerAddrs([]string{"localhost:11211"}),
		SecondLevelCachePessimisticLock(false),
		SecondLevelCacheOptimisticLock(false),
	)
	if err != nil {
		panic(err)
	}
	if err := cache.Flush(); err != nil {
		panic(err)
	}
	if err := cache.WarmUp(conn, new(A).Type(), false); err != nil {
		panic(err)
	}
	b.ResetTimer()
	id := uint64(1)
	for n := 0; n < b.N; n++ {
		txConn, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		tx, err := cache.Begin(txConn)
		if err != nil {
			panic(err)
		}
		builder := NewQueryBuilder("a").Eq("id", id)
		if err := tx.DeleteByQueryBuilder(builder); err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		id++
	}
}
