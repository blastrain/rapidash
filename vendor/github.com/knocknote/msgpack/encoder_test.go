package msgpack_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"testing"

	msgpack "github.com/lestrrat-go/msgpack"
	"github.com/stretchr/testify/assert"
)

type tagTest struct {
	Foo string `msg:"foooo"`
	Bar string `msgpack:"baaaaaaar"`
	Baz string `msgpack:"-"`
}

func TestTag(t *testing.T) {
	v := tagTest{
		Foo: "Hello",
		Bar: "World!",
		Baz: "Abracadabra",
	}

	data, err := msgpack.Marshal(v)
	if !assert.NoError(t, err, "msgpack.Marshal should succeed") {
		return
	}

	var m = make(map[string]interface{})
	if !assert.NoError(t, msgpack.Unmarshal(data, &m), "msgpack.Unmarshal should succeed") {
		return
	}

	if !assert.Equal(t, m["foooo"], "Hello", "key foooo should match") {
		return
	}
	if !assert.Equal(t, m["baaaaaaar"], "World!", "key baaaaaaar should match") {
		return
	}
	if !assert.Empty(t, m["Baz"], "key baaaaaaar should match") {
		return
	}
}

func TestEncodeMapInvalidValue(t *testing.T) {
	var f struct {
		Foo string
	}

	// shouldn't panic
	msgpack.NewEncoder(ioutil.Discard).EncodeMap(f)
}

func TestEncodeNil(t *testing.T) {
	var e = []byte{msgpack.Nil.Byte()}

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(nil)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})
	t.Run("encode via Encode", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(nil), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
	t.Run("encode via EncodeNil", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeNil(), "EncodeNil should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeBool(t *testing.T) {
	for _, code := range []msgpack.Code{msgpack.True, msgpack.False} {
		var v bool
		if code == msgpack.True {
			v = true
		}
		var e = []byte{code.Byte()}

		t.Run(fmt.Sprintf("encode %s via Marshal", code), func(t *testing.T) {
			b, err := msgpack.Marshal(v)
			if !assert.NoError(t, err, "Marshal should succeed") {
				return
			}
			if !assert.Equal(t, e, b, "Output should match") {
				return
			}
		})
		t.Run(fmt.Sprintf("encode %s via EncodeBool", code), func(t *testing.T) {
			var buf bytes.Buffer
			if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeBool(v), "EncodeBool should succeed") {
				return
			}
			if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
				return
			}
		})
		t.Run(fmt.Sprintf("encode %s via Encode", code), func(t *testing.T) {
			var buf bytes.Buffer
			if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
				return
			}

			if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
				return
			}
		})
	}
}

func TestEncodeFloat32(t *testing.T) {
	var v = float32(math.MaxFloat32)
	var e = make([]byte, 5)
	e[0] = msgpack.Float.Byte()
	binary.BigEndian.PutUint32(e[1:], math.Float32bits(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}
		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})
	t.Run("encode via EncodeFloat32", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeFloat32(v), "EncodeFloat32 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
	t.Run("encode via Encode", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeFloat64(t *testing.T) {
	var v = float64(math.MaxFloat64)
	var e = make([]byte, 9)
	e[0] = msgpack.Double.Byte()
	binary.BigEndian.PutUint64(e[1:], math.Float64bits(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}
		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})
	t.Run("encode via EncodeFloat64", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeFloat64(v), "EncodeFloat64 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
	t.Run("encode via Encode", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeUint8(t *testing.T) {
	var v = uint8(math.MaxUint8)
	var e = []byte{msgpack.Uint8.Byte(), byte(v)}

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeUint8", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeUint8(v), "EncodeUint8 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeUint16(t *testing.T) {
	var v = uint16(math.MaxUint16)
	var e = make([]byte, 3)
	e[0] = msgpack.Uint16.Byte()
	binary.BigEndian.PutUint16(e[1:], v)

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeUint16", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeUint16(v), "EncodeUint16 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeUint32(t *testing.T) {
	var v = uint32(math.MaxUint32)
	var e = make([]byte, 5)
	e[0] = msgpack.Uint32.Byte()
	binary.BigEndian.PutUint32(e[1:], v)

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeUint32", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeUint32(v), "EncodeUint32 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeUint64(t *testing.T) {
	var v = uint64(math.MaxUint64)
	var e = make([]byte, 9)
	e[0] = msgpack.Uint64.Byte()
	binary.BigEndian.PutUint64(e[1:], v)

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeUint64", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeUint64(v), "EncodeUint64 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodePositiveFixNum(t *testing.T) {
	for i := 0; i < 127; i++ {
		t.Run(fmt.Sprintf("encode %d should result in fix num", i), func(t *testing.T) {
			b, err := msgpack.Marshal(uint(i))
			if !assert.NoError(t, err, `msgpack.Marshal should succeed`) {
				return
			}

			if !assert.Len(t, b, 1, "encoded number should be 1 byte") {
				return
			}

			if !assert.True(t, b[0] >= 0 && b[0] <= 127, "b should be 0 <= b <= 127") {
				return
			}
		})
	}
}

func TestEncodeNegativeFixNum(t *testing.T) {
	for i := -1; i < 0; i++ {
		t.Run(fmt.Sprintf("encode %d should result in fix num", i), func(t *testing.T) {
			b, err := msgpack.Marshal(int(i))
			if !assert.NoError(t, err, `msgpack.Marshal should succeed`) {
				return
			}

			if !assert.Len(t, b, 1, "encoded number should be 1 byte") {
				return
			}

			if !assert.Equal(t, i, int(int8(b[0])), "b should be negative number") {
				return
			}
		})
	}
}

func TestEncodeInt8(t *testing.T) {
	var v = int8(math.MaxInt8)
	var e = []byte{msgpack.Int8.Byte(), byte(v)}

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeInt8", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeInt8(v), "EncodeInt8 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeInt16(t *testing.T) {
	var v = int16(math.MaxInt16)
	var e = make([]byte, 3)
	e[0] = msgpack.Int16.Byte()
	binary.BigEndian.PutUint16(e[1:], uint16(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeInt16", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeInt16(v), "EncodeInt16 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeInt32(t *testing.T) {
	var v = int32(math.MaxInt32)
	var e = make([]byte, 5)
	e[0] = msgpack.Int32.Byte()
	binary.BigEndian.PutUint32(e[1:], uint32(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeInt32", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeInt32(v), "EncodeInt32 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeInt64(t *testing.T) {
	var v = int64(math.MaxInt64)
	var e = make([]byte, 9)
	e[0] = msgpack.Int64.Byte()
	binary.BigEndian.PutUint64(e[1:], uint64(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeInt64", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeInt64(v), "EncodeInt64 should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func makeString(l int) string {
	var buf bytes.Buffer
	var x int
	for i := 0; i < l; i++ {
		if x >= 10 {
			x = 0
		}
		buf.WriteByte(byte(x + 48))
		x++
	}
	return buf.String()
}

func TestEncodeStr8(t *testing.T) {
	var v = makeString(math.MaxUint8)
	var e = make([]byte, math.MaxUint8+2)
	e[0] = msgpack.Str8.Byte()
	e[1] = math.MaxUint8
	copy(e[2:], []byte(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeString", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeString(v), "EncodeString should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeStr16(t *testing.T) {
	var v = makeString(math.MaxUint16)
	var e = make([]byte, math.MaxUint16+3)
	e[0] = msgpack.Str16.Byte()
	binary.BigEndian.PutUint16(e[1:], math.MaxUint16)
	copy(e[3:], []byte(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeString", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeString(v), "EncodeString should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeStr32(t *testing.T) {
	var l = math.MaxUint16 + 1
	var v = makeString(l)
	var e = make([]byte, l+5)
	e[0] = msgpack.Str32.Byte()
	binary.BigEndian.PutUint32(e[1:], uint32(l))
	copy(e[5:], []byte(v))

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeString", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeString(v), "EncodeString should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

func TestEncodeFixStr(t *testing.T) {
	for l := 1; l < 32; l++ {
		var v = makeString(l)
		var e = make([]byte, l+1)
		e[0] = msgpack.FixStr0.Byte() + byte(l)
		copy(e[1:], []byte(v))

		t.Run(fmt.Sprintf("encode via Marshal (fixstr%d)", l), func(t *testing.T) {
			b, err := msgpack.Marshal(v)
			if !assert.NoError(t, err, "Marshal should succeed") {
				return
			}

			if !assert.Equal(t, e, b, "Output should match") {
				return
			}
		})

		t.Run(fmt.Sprintf("encode via EncodeString (fixstr%d)", l), func(t *testing.T) {
			var buf bytes.Buffer
			if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeString(v), "EncodeString should succeed") {
				return
			}

			if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
				return
			}
		})

		t.Run(fmt.Sprintf("encode via Encoder (fixstr%d)", l), func(t *testing.T) {
			var buf bytes.Buffer
			if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
				return
			}

			if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
				return
			}
		})
	}
}

func TestEncodeBytes(t *testing.T) {
	var v = []byte(makeString(math.MaxUint8))
	var e = make([]byte, math.MaxUint8+2)
	e[0] = msgpack.Bin8.Byte()
	e[1] = math.MaxUint8
	copy(e[2:], v)

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, "Marshal should succeed") {
			return
		}

		if !assert.Equal(t, e, b, "Output should match") {
			return
		}
	})

	t.Run("encode via EncodeString", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).EncodeBytes(v), "EncodeBytes should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})

	t.Run("encode via Encoder", func(t *testing.T) {
		var buf bytes.Buffer
		if !assert.NoError(t, msgpack.NewEncoder(&buf).Encode(v), "Encode should succeed") {
			return
		}

		if !assert.Equal(t, e, buf.Bytes(), "Output should match") {
			return
		}
	})
}

type testStruct struct {
	Foo int
	Bar struct {
		BarContent string `msgpack:"bar.content"`
	} `msgpack:"bar"`
	Baz  string `msgpack:",omitempty"`
	Quux string `msgpack:"-"`
}

func TestEncodeStruct(t *testing.T) {
	var v = testStruct{
		Foo:  100,
		Quux: "quux",
	}
	v.Bar.BarContent = "Hello, World!"

	mapb := msgpack.NewMapBuilder()
	mapb.Add("Foo", 100)
	mapb.Add("bar", v.Bar)
	e, err := mapb.Bytes()
	if !assert.NoError(t, err, "MapBuilder.Bytes() should succeed") {
		return
	}

	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, `Marshal should succeed`) {
			return
		}

		if !assert.Equal(t, e, b, `Output should match`) {
			return
		}
	})
}

func TestEncodeArray(t *testing.T) {
	var buf bytes.Buffer
	arrayb := msgpack.NewArrayBuilder()
	arrayb.Add(int32(100))
	arrayb.Add("foo")
	arrayb.Add(float32(0))
	if !assert.NoError(t, arrayb.Encode(&buf), "Encode should succeed") {
		return
	}

	e := buf.Bytes()
	v := []interface{}{int32(100), "foo", float32(0)}
	t.Run("encode via Marshal", func(t *testing.T) {
		b, err := msgpack.Marshal(v)
		if !assert.NoError(t, err, `Marshal should succeed`) {
			return
		}

		if !assert.Equal(t, e, b, `Output should match`) {
			return
		}
	})
}
