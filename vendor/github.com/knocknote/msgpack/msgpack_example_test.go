package msgpack_test

import (
	"fmt"
	"time"

	msgpack "github.com/lestrrat-go/msgpack"
	"github.com/pkg/errors"
)

type EventTime struct {
	time.Time
}

func init() {
	if err := msgpack.RegisterExt(0, EventTime{}); err != nil {
		panic(err)
	}
}

func (t *EventTime) DecodeMsgpack(d *msgpack.Decoder) error {
	r := d.Reader()
	sec, err := r.ReadUint32()
	if err != nil {
		return errors.Wrap(err, `failed to read uint32 from first 4 bytes`)
	}

	nsec, err := r.ReadUint32()
	if err != nil {
		return errors.Wrap(err, `failed to read uint32 from second 4 bytes`)
	}

	t.Time = time.Unix(int64(sec), int64(nsec)).UTC()
	return nil
}

func (t EventTime) EncodeMsgpack(e *msgpack.Encoder) error {
	w := e.Writer()
	if err := w.WriteUint32(uint32(t.Unix())); err != nil {
		return errors.Wrap(err, `failed to write EventTime seconds payload`)
	}

	if err := w.WriteUint32(uint32(t.Nanosecond())); err != nil {
		return errors.Wrap(err, `failed to write EventTime nanoseconds payload`)
	}

	return nil
}

type FluentdMessage struct {
	Tag    string
	Time   EventTime
	Record map[string]interface{}
	Option map[string]interface{}
}

func (m FluentdMessage) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayHeader(4); err != nil {
		return errors.Wrap(err, `failed to encode array header`)
	}
	if err := e.EncodeString(m.Tag); err != nil {
		return errors.Wrap(err, `failed to encode tag`)
	}
	if err := e.EncodeStruct(m.Time); err != nil {
		return errors.Wrap(err, `failed to encode time`)
	}
	if err := e.EncodeMap(m.Record); err != nil {
		return errors.Wrap(err, `failed to encode record`)
	}
	if err := e.EncodeMap(m.Option); err != nil {
		return errors.Wrap(err, `failed to encode option`)
	}
	return nil
}

func (m *FluentdMessage) DecodeMsgpack(e *msgpack.Decoder) error {
	var l int
	if err := e.DecodeArrayLength(&l); err != nil {
		return errors.Wrap(err, `failed to decode msgpack array length`)
	}

	if l != 4 {
		return errors.Errorf(`invalid array length %d (expected 4)`, l)
	}

	if err := e.DecodeString(&m.Tag); err != nil {
		return errors.Wrap(err, `failed to decode fluentd message tag`)
	}

	if err := e.DecodeStruct(&m.Time); err != nil {
		return errors.Wrap(err, `failed to decode fluentd time`)
	}

	if err := e.DecodeMap(&m.Record); err != nil {
		return errors.Wrap(err, `failed to decode fluentd record`)
	}

	if err := e.DecodeMap(&m.Option); err != nil {
		return errors.Wrap(err, `failed to decode fluentd option`)
	}

	return nil
}

func ExampleFluentdMessage() {
	var f1 = FluentdMessage{
		Tag:  "foo",
		Time: EventTime{Time: time.Unix(1234567890, 123).UTC()},
		Record: map[string]interface{}{
			"count": 1000,
		},
	}

	b, err := msgpack.Marshal(f1)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	var f2 FluentdMessage
	if err := msgpack.Unmarshal(b, &f2); err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	fmt.Printf("%s %s %v %v\n", f2.Tag, f2.Time, f2.Record, f2.Option)
	// OUTPUT:
	// foo 2009-02-13 23:31:30.000000123 +0000 UTC map[count:1000] map[]
}

func ExampleEventTime() {
	var e1 = EventTime{Time: time.Unix(1234567890, 123).UTC()}

	b, err := msgpack.Marshal(e1)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	var e2 interface{}
	if err := msgpack.Unmarshal(b, &e2); err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	// OUTPUT:
}

func ExampleStructTag() {
	var v struct {
		Foo string `msgpack:"foo"`
		Bar string `msgpack:"bar,omitempty"`
	}

	b, err := msgpack.Marshal(v)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	var m map[string]interface{}
	if err := msgpack.Unmarshal(b, &m); err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	if _, ok := m["foo"]; !ok {
		fmt.Printf(`payload should have contained "foo"`)
		return
	}

	if _, ok := m["bar"]; ok {
		fmt.Printf(`payload should NOT contain "bar"`)
		return
	}

	v.Bar = "bar"

	b, err = msgpack.Marshal(v)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	m = map[string]interface{}{} // reinitialize
	if err := msgpack.Unmarshal(b, &m); err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	if _, ok := m["foo"]; !ok {
		fmt.Printf(`payload should have contained "foo"`)
		return
	}

	if _, ok := m["bar"]; !ok {
		fmt.Printf(`payload should have contained "bar"`)
		return
	}

	// OUTPUT:
}
