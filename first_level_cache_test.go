package rapidash

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/xerrors"
)

type Event struct {
	ID              uint64
	EventID         uint64
	EventCategoryID uint64
	Term            string
	StartWeek       uint8
	EndWeek         uint8
	CreatedAt       *time.Time
	UpdatedAt       *time.Time
}

func (e *Event) DecodeRapidash(decoder Decoder) error {
	e.ID = decoder.Uint64("id")
	e.EventID = decoder.Uint64("event_id")
	e.EventCategoryID = decoder.Uint64("event_category_id")
	e.Term = decoder.String("term")
	e.StartWeek = decoder.Uint8("start_week")
	e.EndWeek = decoder.Uint8("end_week")
	e.CreatedAt = decoder.TimePtr("created_at")
	e.UpdatedAt = decoder.TimePtr("updated_at")
	return nil
}

type EventSlice []*Event

func (e *EventSlice) DecodeRapidash(decoder Decoder) error {
	len := decoder.Len()
	*e = make([]*Event, len)
	for i := 0; i < len; i++ {
		var event Event
		if err := event.DecodeRapidash(decoder.At(i)); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
		(*e)[i] = &event
	}
	return nil
}

func eventType() *Struct {
	return NewStruct("events").
		FieldUint64("id").
		FieldUint64("event_id").
		FieldUint64("event_category_id").
		FieldString("term").
		FieldUint8("start_week").
		FieldUint8("end_week").
		FieldTime("created_at").
		FieldTime("updated_at")
}

type Empty struct {
	ID uint64
}

func (e *Empty) DecodeRapidash(decoder Decoder) error {
	e.ID = decoder.Uint64("id")
	return nil
}

type EmptySlice []*Empty

func (e *EmptySlice) DecodeRapidash(decoder Decoder) error {
	len := decoder.Len()
	*e = make([]*Empty, len)
	for i := 0; i < len; i++ {
		var event Empty
		if err := event.DecodeRapidash(decoder.At(i)); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
		(*e)[i] = &event
	}
	return nil
}

func emptyType() *Struct {
	return NewStruct("empties").
		FieldUint64("id")
}

func TestPK(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	var event Event
	NoError(t, flc.FindByPrimaryKey(NewUint64Value(uint64(1)), &event))
}

func TestINQuery(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	builder := NewQueryBuilder("events").In("id", []uint64{1, 2, 3, 4, 5})
	var events EventSlice
	NoError(t, flc.FindByQueryBuilder(builder, &events))
	if len(events) != 5 {
		t.Fatalf("cannot find by where id in (1, 2, 3, 4, 5). len = %d", len(events))
	}
}

func TestFindAll(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	var events EventSlice
	NoError(t, flc.FindAll(&events))
	if len(events) != 4000 {
		t.Fatal("cannot work findAll")
	}
}

func TestComplicatedQuery(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	builder := NewQueryBuilder("events").
		Eq("event_id", uint64(1)).
		Gte("start_week", uint8(12)).
		Lte("end_week", uint8(24)).
		Eq("term", "daytime")
	var events EventSlice
	NoError(t, flc.FindByQueryBuilder(builder, &events))
	if len(events) != 1 {
		t.Fatal("cannot work FirstLevelCache")
	}
}

func TestNEQQuery(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	t.Run("index column", func(t *testing.T) {
		{
			builder := NewQueryBuilder("events").
				Eq("term", "daytime").
				Gte("start_week", uint8(1)).
				Neq("end_week", uint8(12))
			var events EventSlice
			NoError(t, tx.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 3000)
		}
		{
			builder := NewQueryBuilder("events").Neq("id", uint64(1))
			var events EventSlice
			NoError(t, tx.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 3999)
		}
	})
	t.Run("not index column", func(t *testing.T) {
		{
			builder := NewQueryBuilder("events").Neq("end_week", uint8(12))
			var events EventSlice
			NoError(t, tx.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 3000)
		}
		{
			builder := NewQueryBuilder("events").Lte("end_week", uint8(100)).Neq("end_week", uint8(12))
			var events EventSlice
			NoError(t, tx.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 3000)
		}
	})

}

func TestGteAndLteQuery(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	t.Run("primary key column", func(t *testing.T) {
		builder := NewQueryBuilder("events").
			Gte("id", uint64(1)).
			Lte("id", uint64(5))
		var events EventSlice
		NoError(t, flc.FindByQueryBuilder(builder, &events))
		Equal(t, len(events), 5)
	})

	t.Run("index column", func(t *testing.T) {
		{
			builder := NewQueryBuilder("events").
				Gte("event_id", uint64(900))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 404)
		}
		{
			builder := NewQueryBuilder("events").
				Lte("event_id", uint64(1000)).
				Gte("event_id", uint64(900)).
				Eq("start_week", uint8(1))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 101)
		}
		{
			builder := NewQueryBuilder("events").
				Lte("event_id", uint64(1000)).
				Gte("event_id", uint64(900)).
				Eq("event_id", uint64(1))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 0)
		}
		{
			builder := NewQueryBuilder("events").
				In("term", []string{"morning", "daytime"}).
				Gte("start_week", uint8(12)).
				Lte("start_week", uint8(25))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 2000)
		}
	})

	t.Run("not index column", func(t *testing.T) {
		{
			builder := NewQueryBuilder("events").
				Gte("start_week", uint8(12)).
				Lte("start_week", uint8(25))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 2000)
		}
		{
			now := time.Now()
			builder := NewQueryBuilder("events").
				Lte("updated_at", now).
				Gte("updated_at", now.Add(time.Hour*24*7))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 0)
		}
	})
}

func TestGtAndLtQuery(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	t.Run("primary key column", func(t *testing.T) {
		builder := NewQueryBuilder("events").
			Gt("id", uint64(0)).
			Lt("id", uint64(6))
		var events EventSlice
		NoError(t, flc.FindByQueryBuilder(builder, &events))
		Equal(t, len(events), 5)
	})

	t.Run("index column", func(t *testing.T) {
		{
			builder := NewQueryBuilder("events").
				Gt("event_id", uint64(900))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 400)
		}
		{
			builder := NewQueryBuilder("events").
				Gt("event_id", uint64(900)).
				Lt("event_id", uint64(1000))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 396)
		}
		{
			builder := NewQueryBuilder("events").
				Lt("event_id", uint64(1000)).
				Gt("event_id", uint64(900)).
				Eq("start_week", uint8(1))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 99)
		}
	})

	t.Run("not index column", func(t *testing.T) {
		{
			builder := NewQueryBuilder("events").
				Gt("start_week", uint8(12)).
				Lt("start_week", uint8(25))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 1000)
		}
		{
			now := time.Now()
			builder := NewQueryBuilder("events").
				Lt("updated_at", now).
				Gt("updated_at", now.Add(time.Hour*24*7))
			var events EventSlice
			NoError(t, flc.FindByQueryBuilder(builder, &events))
			Equal(t, len(events), 0)
		}
	})
}

func TestOrderQuery(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	builder := NewQueryBuilder("events").
		Eq("event_id", uint64(1)).
		Gte("start_week", uint8(12)).
		OrderDesc("id").
		OrderDesc("start_week")
	var events EventSlice
	NoError(t, flc.FindByQueryBuilder(builder, &events))
	if len(events) != 3 {
		t.Fatal("cannot work order query")
	}
	prevID := events[0].ID
	for _, event := range events {
		if prevID < event.ID {
			t.Fatal("cannot work order query")
		}
		prevID = event.ID
	}
}

func TestCountQuery(t *testing.T) {
	flc := NewFirstLevelCache(eventType())
	NoError(t, flc.WarmUp(conn))
	builder := NewQueryBuilder("events").
		Eq("event_id", uint64(1))
	count, err := flc.CountByQueryBuilder(builder)
	NoError(t, err)
	if count != 4 {
		t.Fatal("cannot work count")
	}
}

func TestPtrType(t *testing.T) {
	NoError(t, initCache(conn, CacheServerTypeMemcached))
	NoError(t, cache.WarmUp(conn, new(PtrType).Type(), false))
	defer func() {
		NoError(t, initCache(conn, CacheServerTypeMemcached))
	}()

	tx, err := cache.Begin()
	NoError(t, err)
	t.Run("EQ", func(t *testing.T) {
		builder := NewQueryBuilder("ptr").Eq("intptr", 1).Eq("int8ptr", int8(2)).
			Eq("int16ptr", int16(3)).Eq("int32ptr", int32(4)).Eq("int64ptr", int64(5)).
			Eq("uintptr", uint(6)).Eq("uint8ptr", uint8(7)).Eq("uint16ptr", uint16(8)).
			Eq("uint32ptr", uint32(9)).Eq("uint64ptr", uint64(10)).Eq("float32ptr", float32(1.23)).
			Eq("float64ptr", float64(4.56)).Eq("bytesptr", []byte("bytes")).
			Eq("stringptr", "string").Eq("boolptr", true)
		var v PtrType
		NoError(t, tx.FindByQueryBuilder(builder, &v))

		Equal(t, v.id, uint64(2))

		{
			builder.Eq("timeptr", v.timePtr)
			NoError(t, tx.FindByQueryBuilder(builder, &v))

			Equal(t, v.id, uint64(2))
		}

	})
	t.Run("NEQ", func(t *testing.T) {
		builder := NewQueryBuilder("ptr").Neq("intptr", 1).Neq("int8ptr", int8(2)).
			Neq("int16ptr", int16(3)).Neq("int32ptr", int32(4)).Neq("int64ptr", int64(5)).
			Neq("uintptr", uint(6)).Neq("uint8ptr", uint8(7)).Neq("uint16ptr", uint16(8)).
			Neq("uint32ptr", uint32(9)).Neq("uint64ptr", uint64(10)).Neq("float32ptr", float32(1.23)).
			Neq("float64ptr", float64(4.56)).Neq("bytesptr", []byte("bytes")).Neq("stringptr", "string").
			Neq("boolptr", true).Neq("timeptr", time.Now())
		var v PtrType
		NoError(t, tx.FindByQueryBuilder(builder, &v))
		Equal(t, v.id, uint64(1))
	})
	t.Run("LTE AND GTE", func(t *testing.T) {
		builders := []*QueryBuilder{
			NewQueryBuilder("ptr").Gte("intptr", 1).Lte("intptr", 1),
			NewQueryBuilder("ptr").Gte("int8ptr", int8(2)).Lte("int8ptr", int8(2)),
			NewQueryBuilder("ptr").Gte("int16ptr", int16(3)).Lte("int16ptr", int16(3)),
			NewQueryBuilder("ptr").Gte("int32ptr", int32(4)).Lte("int32ptr", int32(4)),
			NewQueryBuilder("ptr").Gte("int64ptr", int64(5)).Lte("int64ptr", int64(5)),
			NewQueryBuilder("ptr").Gte("uintptr", uint(6)).Lte("uintptr", uint(6)),
			NewQueryBuilder("ptr").Gte("uint8ptr", uint8(7)).Lte("uint8ptr", uint8(7)),
			NewQueryBuilder("ptr").Gte("uint16ptr", uint16(8)).Lte("uint16ptr", uint16(8)),
			NewQueryBuilder("ptr").Gte("uint32ptr", uint32(9)).Lte("uint32ptr", uint32(9)),
			NewQueryBuilder("ptr").Gte("uint64ptr", uint64(10)).Lte("uint64ptr", uint64(10)),
			NewQueryBuilder("ptr").Gte("float32ptr", float32(1.23)).Lte("float32ptr", float32(1.23)),
			NewQueryBuilder("ptr").Gte("float64ptr", float64(4.56)).Lte("float64ptr", float64(4.56)),
			NewQueryBuilder("ptr").Gte("bytesptr", []byte("bytes")).Lte("bytesptr", []byte("bytes")),
			NewQueryBuilder("ptr").Gte("stringptr", "string").Lte("stringptr", "string"),
			NewQueryBuilder("ptr").Gte("boolptr", false),
			NewQueryBuilder("ptr").Lte("boolptr", true),
			NewQueryBuilder("ptr").Gte("timeptr", time.Now().Add(-time.Hour*24)).Lte("timeptr", time.Now().Add(time.Hour*24)),
		}

		for _, builder := range builders {
			var v PtrType
			NoError(t, tx.FindByQueryBuilder(builder, &v))

			if builder.Conditions().conditions[0].Column() == "boolptr" {
				Equal(t, v.id, uint64(0))
			} else {
				Equal(t, v.id, uint64(2))
			}
		}
	})
	t.Run("LT AND GT", func(t *testing.T) {
		builders := []*QueryBuilder{
			NewQueryBuilder("ptr").Gt("intptr", 0).Lt("intptr", 2),
			NewQueryBuilder("ptr").Gt("int8ptr", int8(1)).Lt("int8ptr", int8(3)),
			NewQueryBuilder("ptr").Gt("int16ptr", int16(2)).Lt("int16ptr", int16(4)),
			NewQueryBuilder("ptr").Gt("int32ptr", int32(3)).Lt("int32ptr", int32(5)),
			NewQueryBuilder("ptr").Gt("int64ptr", int64(4)).Lt("int64ptr", int64(6)),
			NewQueryBuilder("ptr").Gt("uintptr", uint(5)).Lt("uintptr", uint(7)),
			NewQueryBuilder("ptr").Gt("uint8ptr", uint8(6)).Lt("uint8ptr", uint8(8)),
			NewQueryBuilder("ptr").Gt("uint16ptr", uint16(7)).Lt("uint16ptr", uint16(9)),
			NewQueryBuilder("ptr").Gt("uint32ptr", uint32(8)).Lt("uint32ptr", uint32(10)),
			NewQueryBuilder("ptr").Gt("uint64ptr", uint64(9)).Lt("uint64ptr", uint64(11)),
			NewQueryBuilder("ptr").Gt("float32ptr", float32(1.22)).Lt("float32ptr", float32(1.24)),
			NewQueryBuilder("ptr").Gt("float64ptr", float64(4.55)).Lt("float64ptr", float64(4.57)),
			NewQueryBuilder("ptr").Gt("bytesptr", []byte("byte")).Lt("bytesptr", []byte("bytess")),
			NewQueryBuilder("ptr").Gt("stringptr", "strin").Lt("stringptr", "strings"),
			NewQueryBuilder("ptr").Gt("boolptr", false),
			NewQueryBuilder("ptr").Lt("boolptr", true),
			NewQueryBuilder("ptr").Gt("timeptr", time.Now().Add(-time.Hour*24)).Lt("timeptr", time.Now().Add(time.Hour*24)),
		}

		for _, builder := range builders {
			var v PtrType
			NoError(t, tx.FindByQueryBuilder(builder, &v))

			if builder.Conditions().conditions[0].Column() == "boolptr" {
				Equal(t, v.id, uint64(0))
			} else {
				Equal(t, v.id, uint64(2))
			}
		}
	})

}

func TestFindByPrimaryKeyCaseDatabaseRecordIsEmpty(t *testing.T) {
	flc := NewFirstLevelCache(emptyType())
	NoError(t, flc.WarmUp(conn))
	var empty Empty
	NoError(t, flc.FindByPrimaryKey(NewUint64Value(uint64(1)), &empty))
	if empty.ID != 0 {
		t.Fatal("cannot work FindByPrimaryKey")
	}
}

func TestFindByQueryBuilderCaseDatabaseRecordIsEmpty(t *testing.T) {
	flc := NewFirstLevelCache(emptyType())
	NoError(t, flc.WarmUp(conn))
	builder := NewQueryBuilder("empties").In("id", []uint64{1})
	var empties EmptySlice
	NoError(t, flc.FindByQueryBuilder(builder, &empties))
	if len(empties) != 0 {
		t.Fatalf("cannot work FindByQueryBuilder. len = %d", len(empties))
	}
}

func TestCountByQueryBuilderCaseDatabaseRecordIsEmpty(t *testing.T) {
	flc := NewFirstLevelCache(emptyType())
	NoError(t, flc.WarmUp(conn))
	builder := NewQueryBuilder("empties").Eq("id", uint64(1))
	count, err := flc.CountByQueryBuilder(builder)
	NoError(t, err)
	if count != 0 {
		t.Fatal("cannot work count")
	}
}

func TestFindAllCaseDatabaseRecordIsEmpty(t *testing.T) {
	flc := NewFirstLevelCache(emptyType())
	NoError(t, flc.WarmUp(conn))
	var empties EmptySlice
	NoError(t, flc.FindAll(&empties))
	if len(empties) != 0 {
		t.Fatal("cannot work findAll")
	}
}

func BenchmarkPK_MySQL(b *testing.B) {
	b.ResetTimer()
	var events []*Event
	for n := 0; n < b.N; n++ {
		for i := 1; i <= 100; i++ {
			var (
				id              uint64
				eventID         uint64
				eventCategoryID uint64
				term            string
				startWeek       uint8
				endWeek         uint8
				createdAt       time.Time
				updatedAt       time.Time
			)
			if err := conn.QueryRow(fmt.Sprintf("select id,event_id,event_category_id,term,start_week,end_week,created_at,updated_at from events where id = %d", i)).Scan(&id, &eventID, &eventCategoryID, &term, &startWeek, &endWeek, &createdAt, &updatedAt); err != nil {
				panic(err)
			}
			events = append(events, &Event{
				ID:              id,
				EventID:         eventID,
				EventCategoryID: eventCategoryID,
				Term:            term,
				StartWeek:       startWeek,
				EndWeek:         endWeek,
				CreatedAt:       &createdAt,
				UpdatedAt:       &updatedAt,
			})
		}
	}
	if len(events) != b.N*100 {
		panic("invalid event number")
	}
}

func BenchmarkPK_Rapidash(b *testing.B) {
	flc := NewFirstLevelCache(eventType())
	if err := flc.WarmUp(conn); err != nil {
		panic(err)
	}
	b.ResetTimer()
	v := NewUint64Value(0)
	events := []*Event{}
	for n := 0; n < b.N; n++ {
		for i := 1; i <= 100; i++ {
			var event Event
			v.uint64Value = uint64(i)
			if err := flc.FindByPrimaryKey(v, &event); err != nil {
				panic(err)
			}
			events = append(events, &event)
		}
	}
	if len(events) != b.N*100 {
		panic("invalid event number")
	}
}

func BenchmarkIN_MySQL(b *testing.B) {
	b.ResetTimer()
	events := []*Event{}
	for n := 0; n < b.N; n++ {
		for i := 1; i <= 100; i++ {
			rows, err := conn.Query(fmt.Sprintf("select id,event_id,event_category_id,term,start_week,end_week,created_at,updated_at from events where id IN (1, 2, 3, 4, 5)"))
			if err != nil {
				panic(err)
			}
			for rows.Next() {
				var (
					id              uint64
					eventID         uint64
					eventCategoryID uint64
					term            string
					startWeek       uint8
					endWeek         uint8
					createdAt       time.Time
					updatedAt       time.Time
				)
				if err := rows.Scan(&id, &eventID, &eventCategoryID, &term, &startWeek, &endWeek, &createdAt, &updatedAt); err != nil {
					panic(err)
				}
				events = append(events, &Event{
					ID:              id,
					EventID:         eventID,
					EventCategoryID: eventCategoryID,
					Term:            term,
					StartWeek:       startWeek,
					EndWeek:         endWeek,
					CreatedAt:       &createdAt,
					UpdatedAt:       &updatedAt,
				})
			}
		}
	}
	if len(events) != b.N*100*5 {
		panic("invalid event number")
	}
}

func BenchmarkIN_Rapidash(b *testing.B) {
	flc := NewFirstLevelCache(eventType())
	if err := flc.WarmUp(conn); err != nil {
		panic(err)
	}
	builder := NewQueryBuilder("events").In("id", []uint64{1, 2, 3, 4, 5})
	b.ResetTimer()
	events := []*Event{}
	for n := 0; n < b.N; n++ {
		for i := 1; i <= 100; i++ {
			var e EventSlice
			if err := flc.FindByQueryBuilder(builder, &e); err != nil {
				panic(err)
			}
			events = append(events, e...)
		}
	}
	if len(events) != b.N*100*5 {
		panic("invalid event number")
	}
}
