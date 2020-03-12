package msgpack_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"

	"github.com/lestrrat-go/msgpack"
	"github.com/stretchr/testify/assert"
)

func unmarshalMatch(t *testing.T, src []byte, v interface{}, expected interface{}) {
	if !assert.NoError(t, msgpack.Unmarshal(src, v), "Unmarshal should succeed") {
		return
	}

	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr {
		v = rv.Elem().Interface()
	}
	if !assert.Equal(t, expected, v, "value should be %v", expected) {
		return
	}
}

func decodeMatch(t *testing.T, src io.Reader, v interface{}, expected interface{}) {
	if !assert.NoError(t, msgpack.NewDecoder(src).Decode(v), "Decode should succeed") {
		return
	}

	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr {
		v = rv.Elem().Interface()
	}
	if !assert.Equal(t, expected, v, "value should be %v", expected) {
		return
	}
}

func decodeTest(t *testing.T, code msgpack.Code, b []byte, e interface{}) {
	decodeTestConcrete(t, code, b, e)
	decodeTestInterface(t, code, b, e)
}

func decodeTestConcrete(t *testing.T, code msgpack.Code, b []byte, e interface{}) {
	typ := reflect.TypeOf(e)
	t.Run(fmt.Sprintf("decode %s via Unmarshal (concrete)", code), func(t *testing.T) {
		v := reflect.New(typ).Elem().Interface()
		unmarshalMatch(t, b, &v, e)
	})

	t.Run(fmt.Sprintf("decode %s via Decoder (concrete)", code), func(t *testing.T) {
		v := reflect.New(typ).Elem().Interface()
		decodeMatch(t, bytes.NewBuffer(b), &v, e)
	})
}

func decodeTestInterface(t *testing.T, code msgpack.Code, b []byte, e interface{}) {
	t.Run(fmt.Sprintf("decode %s via Unmarshal (interface{})", code), func(t *testing.T) {
		var v interface{}
		unmarshalMatch(t, b, &v, e)
	})

	t.Run(fmt.Sprintf("decode %s via Decoder (interface{})", code), func(t *testing.T) {
		var v interface{}
		decodeMatch(t, bytes.NewBuffer(b), &v, e)
	})
}

func decodeTestString(t *testing.T, code msgpack.Code, b []byte, e interface{}) {
	decodeTest(t, code, b, e)
	//	decodeTestMethod(t, code, "DecodeString", b, e)
}

func decodeTestMethod(t *testing.T, code msgpack.Code, method string, b []byte, e interface{}) {
	t.Run(fmt.Sprintf("decode %s via %s", code, method), func(t *testing.T) {
		val := reflect.New(reflect.TypeOf(e))
		dec := msgpack.NewDecoder(bytes.NewBuffer(b))
		ret := reflect.ValueOf(dec).MethodByName(method).Call([]reflect.Value{val})
		if !assert.Len(t, ret, 1, "%s should return 1 values", method) {
			return
		}

		if !assert.Nil(t, ret[0].Interface(), "DecodeString should succeed") {
			return
		}

		if !assert.Equal(t, e, val.Elem().Interface(), "value should be %s", e) {
			return
		}
	})
}

func TestDecodeNil(t *testing.T) {
	var e interface{}
	var b = []byte{msgpack.Nil.Byte()}

	t.Run("decode via Unmarshal", func(t *testing.T) {
		var v interface{}
		unmarshalMatch(t, b, &v, e)
	})
	t.Run("decode via DecodeNil", func(t *testing.T) {
		var v interface{} = &struct{}{}
		buf := bytes.NewBuffer(b)
		if !assert.NoError(t, msgpack.NewDecoder(buf).DecodeNil(&v), "DecodeNil should succeed") {
			return
		}
		if !assert.Nil(t, v, "value should be nil") {
			return
		}
	})
	t.Run("decode via Decoder (interface{})", func(t *testing.T) {
		var v interface{} = 0xdeadcafe
		decodeMatch(t, bytes.NewBuffer(b), &v, e)
	})
}

func TestDecodeBool(t *testing.T) {
	for _, code := range []msgpack.Code{msgpack.True, msgpack.False} {
		var e bool
		if code == msgpack.True {
			e = true
		}
		var b = []byte{code.Byte()}

		decodeTest(t, code, b, e)
		decodeTestMethod(t, code, "DecodeBool", b, e)
	}
}

func TestDecodeFloat32(t *testing.T) {
	var e = float32(math.MaxFloat32)
	var b = make([]byte, 5)
	b[0] = msgpack.Float.Byte()
	binary.BigEndian.PutUint32(b[1:], math.Float32bits(e))

	decodeTest(t, msgpack.Float, b, e)
	decodeTestMethod(t, msgpack.Float, "DecodeFloat32", b, e)
}

func TestDecodeFloat64(t *testing.T) {
	var e = float64(math.MaxFloat64)
	var b = make([]byte, 9)
	b[0] = msgpack.Double.Byte()
	binary.BigEndian.PutUint64(b[1:], math.Float64bits(e))

	decodeTest(t, msgpack.Double, b, e)
	decodeTestMethod(t, msgpack.Float, "DecodeFloat64", b, e)
}

func TestDecodeUint8(t *testing.T) {
	var e = uint8(math.MaxUint8)
	var b = []byte{msgpack.Uint8.Byte(), byte(e)}

	decodeTest(t, msgpack.Uint8, b, e)
	decodeTestMethod(t, msgpack.Uint8, "DecodeUint8", b, e)
}

func TestDecodeUint16(t *testing.T) {
	var e = uint16(math.MaxUint16)
	var b = make([]byte, 3)
	b[0] = msgpack.Uint16.Byte()
	binary.BigEndian.PutUint16(b[1:], uint16(e))

	decodeTest(t, msgpack.Uint16, b, e)
	decodeTestMethod(t, msgpack.Uint16, "DecodeUint16", b, e)
}

func TestDecodeUint32(t *testing.T) {
	var e = uint32(math.MaxUint32)
	var b = make([]byte, 5)
	b[0] = msgpack.Uint32.Byte()
	binary.BigEndian.PutUint32(b[1:], uint32(e))

	decodeTest(t, msgpack.Uint32, b, e)
	decodeTestMethod(t, msgpack.Uint32, "DecodeUint32", b, e)
}

func TestDecodeUint64(t *testing.T) {
	var e = uint64(math.MaxUint64)
	var b = make([]byte, 9)
	b[0] = msgpack.Uint64.Byte()
	binary.BigEndian.PutUint64(b[1:], uint64(e))

	decodeTest(t, msgpack.Uint64, b, e)
	decodeTestMethod(t, msgpack.Uint64, "DecodeUint64", b, e)
}

func TestDecodeInt8(t *testing.T) {
	var e = int8(math.MaxInt8)
	var b = []byte{msgpack.Int8.Byte(), byte(e)}

	decodeTest(t, msgpack.Int8, b, e)
	decodeTestMethod(t, msgpack.Int8, "DecodeInt8", b, e)
}

func TestDecodeInt16(t *testing.T) {
	var e = int16(math.MaxInt16)
	var b = make([]byte, 3)
	b[0] = msgpack.Int16.Byte()
	binary.BigEndian.PutUint16(b[1:], uint16(e))

	decodeTest(t, msgpack.Int16, b, e)
	decodeTestMethod(t, msgpack.Int16, "DecodeInt16", b, e)
}

func TestDecodeInt32(t *testing.T) {
	var e = int32(math.MaxInt32)
	var b = make([]byte, 5)
	b[0] = msgpack.Int32.Byte()
	binary.BigEndian.PutUint32(b[1:], uint32(e))

	decodeTest(t, msgpack.Int32, b, e)
	decodeTestMethod(t, msgpack.Int32, "DecodeInt32", b, e)
}

func TestDecodeInt64(t *testing.T) {
	var e = int64(math.MaxInt64)
	var b = make([]byte, 9)
	b[0] = msgpack.Int64.Byte()
	binary.BigEndian.PutUint64(b[1:], uint64(e))

	decodeTest(t, msgpack.Int64, b, e)
	decodeTestMethod(t, msgpack.Int64, "DecodeInt64", b, e)
}

func TestDecodeStr8(t *testing.T) {
	var l = math.MaxUint8
	var e = makeString(l)
	var b = make([]byte, l+2)
	b[0] = msgpack.Str8.Byte()
	b[1] = byte(l)
	copy(b[2:], []byte(e))

	decodeTestString(t, msgpack.Str8, b, e)
}

func TestDecodeStr16(t *testing.T) {
	var l = math.MaxUint16
	var e = makeString(l)
	var b = make([]byte, l+3)
	b[0] = msgpack.Str16.Byte()
	binary.BigEndian.PutUint16(b[1:], uint16(l))
	copy(b[3:], []byte(e))

	decodeTestString(t, msgpack.Str16, b, e)
}

func TestDecodeStr32(t *testing.T) {
	var l = math.MaxUint16 + 1
	var e = makeString(l)
	var b = make([]byte, l+5)
	b[0] = msgpack.Str32.Byte()
	binary.BigEndian.PutUint32(b[1:], uint32(l))
	copy(b[5:], []byte(e))

	decodeTestString(t, msgpack.Str32, b, e)
}

func TestDecodeFixStr(t *testing.T) {
	for l := 1; l < 32; l++ {
		var e = makeString(l)
		var b = make([]byte, l+1)
		b[0] = byte(msgpack.FixStr0.Byte() + byte(l))
		copy(b[1:], []byte(e))

		decodeTestString(t, msgpack.Code(b[0]), b, e)
	}
}

func TestDecodeStruct(t *testing.T) {
	var buf bytes.Buffer
	w := msgpack.NewWriter(&buf)
	w.WriteByte(msgpack.FixMap2.Byte())
	w.WriteByte(msgpack.FixStr3.Byte())
	w.Write([]byte("Foo"))
	w.WriteByte(msgpack.Int8.Byte())
	w.WriteUint8(uint8(100))
	w.WriteByte(msgpack.FixStr3.Byte())
	w.Write([]byte("bar"))
	w.WriteByte(msgpack.FixMap1.Byte())
	w.WriteByte(msgpack.FixStr11.Byte())
	w.Write([]byte("bar.content"))
	w.WriteByte(msgpack.FixStr13.Byte())
	w.Write([]byte("Hello, World!"))
	b := buf.Bytes()

	var e = testStruct{
		Foo: 100,
	}
	e.Bar.BarContent = "Hello, World!"

	decodeTestConcrete(t, msgpack.FixMap2, b, e)
}

type nestedInner struct {
	Foo string
}

type nestedOuter struct {
	InnerStruct *nestedInner
	InnerSlice  []*nestedInner
}

func TestDecodeArray(t *testing.T) {
	var e []nestedOuter
	e = append(e, nestedOuter{InnerStruct: &nestedInner{Foo: "Hello"}})

	buf, err := msgpack.Marshal(e)
	if !assert.NoError(t, err, "Marshal should succeed") {
		return
	}

	var r []nestedOuter
	if !assert.NoError(t, msgpack.Unmarshal(buf, &r), "Unmarshal should succeed") {
		return
	}

	if !assert.Len(t, r, 1, `r should be length 1`) {
		return
	}
}

func TestDecodeNestedStruct(t *testing.T) {
	t.Run("regular case", func(t *testing.T) {
		var e *nestedOuter = &nestedOuter{
			InnerStruct: &nestedInner{Foo: "Hello"},
			InnerSlice: []*nestedInner{
				&nestedInner{Foo: "World"},
			},
		}

		buf, err := msgpack.Marshal(e)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		var r nestedOuter
		if !assert.NoError(t, msgpack.Unmarshal(buf, &r), "Unmarshal should succeed") {
			return
		}

	})
	t.Run("unitinialized case", func(t *testing.T) {
		var e *nestedOuter = &nestedOuter{
			InnerSlice: []*nestedInner{
				&nestedInner{Foo: "World"},
			},
		}

		buf, err := msgpack.Marshal(e)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		var r nestedOuter
		if !assert.NoError(t, msgpack.Unmarshal(buf, &r), "Unmarshal should succeed") {
			return
		}
		if !assert.Nil(t, r.InnerStruct, "r.InnerStruct should be nil") {
			return
		}
	})
}
