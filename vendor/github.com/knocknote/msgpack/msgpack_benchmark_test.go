// +build bench

package msgpack_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"testing"

	lestrrat "github.com/lestrrat-go/msgpack"
	vmihailenco "gopkg.in/vmihailenco/msgpack.v2"
)

type Decoder interface {
	Decode(interface{}) error
}

type DecodeArrayer interface {
	DecodeArray(*[]interface{}) error
}

type DecodeArrayReturner interface {
	DecodeArray() ([]interface{}, error)
}

type DecodeMapper interface {
	DecodeMap(*map[string]interface{}) error
}

type DecodeStringer interface {
	DecodeString(*string) error
}

type DecodeStringReturner interface {
	DecodeString() (string, error)
}

type DecodeUinter interface {
	DecodeUint(*uint) error
}

type DecodeUint8er interface {
	DecodeUint8(*uint8) error
}

type DecodeUint8Returner interface {
	DecodeUint8() (uint8, error)
}

type DecodeUint16er interface {
	DecodeUint16(*uint16) error
}

type DecodeUint16Returner interface {
	DecodeUint16() (uint16, error)
}

type DecodeUint32er interface {
	DecodeUint32(*uint32) error
}

type DecodeUint32Returner interface {
	DecodeUint32() (uint32, error)
}
type DecodeUint64er interface {
	DecodeUint64(*uint64) error
}

type DecodeUint64Returner interface {
	DecodeUint64() (uint64, error)
}

type DecodeInter interface {
	DecodeInt(*int) error
}

type DecodeInt8er interface {
	DecodeInt8(*int8) error
}

type DecodeInt8Returner interface {
	DecodeInt8() (int8, error)
}

type DecodeInt16er interface {
	DecodeInt16(*int16) error
}

type DecodeInt16Returner interface {
	DecodeInt16() (int16, error)
}

type DecodeInt32er interface {
	DecodeInt32(*int32) error
}

type DecodeInt32Returner interface {
	DecodeInt32() (int32, error)
}
type DecodeInt64er interface {
	DecodeInt64(*int64) error
}

type DecodeInt64Returner interface {
	DecodeInt64() (int64, error)
}

type DecodeFloat32er interface {
	DecodeFloat32(*float32) error
}

type DecodeFloat32Returner interface {
	DecodeFloat32() (float32, error)
}
type DecodeFloat64er interface {
	DecodeFloat64(*float64) error
}

type DecodeFloat64Returner interface {
	DecodeFloat64() (float64, error)
}

type Encoder interface {
	Encode(interface{}) error
}

type EncodeStringer interface {
	EncodeString(string) error
}

type EncodeMapper interface {
	EncodeMap(interface{}) error
}

type EncodeArrayer interface {
	EncodeArray(interface{}) error
}

type EncodeUinter interface {
	EncodeUint(uint) error
}

type EncodeUint8er interface {
	EncodeUint8(uint8) error
}

type EncodeUint16er interface {
	EncodeUint16(uint16) error
}

type EncodeUint32er interface {
	EncodeUint32(uint32) error
}

type EncodeUint64er interface {
	EncodeUint64(uint64) error
}

type EncodeFloat32er interface {
	EncodeFloat32(float32) error
}

type EncodeFloat64er interface {
	EncodeFloat64(float64) error
}

type EncodeInter interface {
	EncodeInt(int) error
}

type EncodeInt8er interface {
	EncodeInt8(int8) error
}

type EncodeInt16er interface {
	EncodeInt16(int16) error
}

type EncodeInt32er interface {
	EncodeInt32(int32) error
}

type EncodeInt64er interface {
	EncodeInt64(int64) error
}

// Oh why, why did you need to declare your Decode with variadic
// input list?
type VmihailencoDecoder struct {
	*vmihailenco.Decoder
}

func (e VmihailencoDecoder) Decode(v interface{}) error {
	return e.Decoder.Decode(v)
}

// Oh why, why did you need to declare your Encode with variadic
// input list?
type VmihailencoEncoder struct {
	*vmihailenco.Encoder
}

func (e VmihailencoEncoder) Encode(v interface{}) error {
	return e.Encoder.Encode(v)
}

var encoders = []struct {
	Name        string
	Encoder     interface{}
	MakeDecoder func(io.Reader) interface{}
}{
	{
		Name:    "___lestrrat",
		Encoder: lestrrat.NewEncoder(ioutil.Discard),
		MakeDecoder: func(r io.Reader) interface{} {
			return lestrrat.NewDecoder(r)
		},
	},
	{
		Name:    "vmihailenco",
		Encoder: VmihailencoEncoder{Encoder: vmihailenco.NewEncoder(ioutil.Discard)},
		MakeDecoder: func(r io.Reader) interface{} {
			return VmihailencoDecoder{Decoder: vmihailenco.NewDecoder(r)}
		},
	},
}

func BenchmarkEncodeFloat32(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/float32 via Encode()", data.Name), func(b *testing.B) {
				var v float32 = math.MaxFloat32
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeFloat32er); ok {
			b.Run(fmt.Sprintf("%s/float32 via EncodeFloat32()", data.Name), func(b *testing.B) {
				var v float32 = math.MaxFloat32
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeFloat32(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeFloat64(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/float64 via Encode()", data.Name), func(b *testing.B) {
				var v float64 = math.MaxFloat64
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeFloat64er); ok {
			b.Run(fmt.Sprintf("%s/float64 via EncodeFloat64()", data.Name), func(b *testing.B) {
				var v float64 = math.MaxFloat64
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeFloat64(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeUint8(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/uint8 via Encode()", data.Name), func(b *testing.B) {
				var v uint8 = math.MaxUint8
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeUint8er); ok {
			b.Run(fmt.Sprintf("%s/uint8 via EncodeUint8()", data.Name), func(b *testing.B) {
				var v uint8 = math.MaxUint8
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeUint8(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeUint16(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/uint16 via Encode()", data.Name), func(b *testing.B) {
				var v uint16 = math.MaxUint16
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeUint16er); ok {
			b.Run(fmt.Sprintf("%s/uint16 via EncodeUint16()", data.Name), func(b *testing.B) {
				var v uint16 = math.MaxUint16
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeUint16(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeUint32(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/uint32 via Encode()", data.Name), func(b *testing.B) {
				var v uint32 = math.MaxUint32
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeUint32er); ok {
			b.Run(fmt.Sprintf("%s/uint32 via EncodeUint32()", data.Name), func(b *testing.B) {
				var v uint32 = math.MaxUint32
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeUint32(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeUint64(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/uint64 via Encode()", data.Name), func(b *testing.B) {
				var v uint64 = math.MaxUint64
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeUint64er); ok {
			b.Run(fmt.Sprintf("%s/uint64 via EncodeUint64()", data.Name), func(b *testing.B) {
				var v uint64 = math.MaxUint64
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeUint64(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeInt8(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/int8 via Encode()", data.Name), func(b *testing.B) {
				var v int8 = math.MaxInt8
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeInt8er); ok {
			b.Run(fmt.Sprintf("%s/int8 via EncodeInt8()", data.Name), func(b *testing.B) {
				var v int8 = math.MaxInt8
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeInt8(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeInt8FixNum(b *testing.B) {
	var v int8 = -31
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/int8 via Encode()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeInt8er); ok {
			b.Run(fmt.Sprintf("%s/int8 via EncodeInt8()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeInt8(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeInt16(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/int16 via Encode()", data.Name), func(b *testing.B) {
				var v int16 = math.MaxInt16
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeInt16er); ok {
			b.Run(fmt.Sprintf("%s/int16 via EncodeInt16()", data.Name), func(b *testing.B) {
				var v int16 = math.MaxInt16
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeInt16(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeInt32(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/int32 via Encode()", data.Name), func(b *testing.B) {
				var v int32 = math.MaxInt32
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeInt32er); ok {
			b.Run(fmt.Sprintf("%s/int32 via EncodeInt32()", data.Name), func(b *testing.B) {
				var v int32 = math.MaxInt32
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeInt32(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeInt64(b *testing.B) {
	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/int64 via Encode()", data.Name), func(b *testing.B) {
				var v int64 = math.MaxInt64
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(v); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeInt64er); ok {
			b.Run(fmt.Sprintf("%s/int64 via EncodeInt64()", data.Name), func(b *testing.B) {
				var v int64 = math.MaxInt64
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeInt64(v); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeString(b *testing.B) {
	for _, data := range encoders {
		for _, size := range []int{math.MaxUint8, math.MaxUint8 + 1, math.MaxUint16 + 1} {
			s := makeString(size)
			if enc, ok := data.Encoder.(Encoder); ok {
				b.Run(fmt.Sprintf("%s/string (%d bytes) via Encode()", data.Name, size), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if err := enc.Encode(s); err != nil {
							panic(err)
						}
					}
				})
			}
			if enc, ok := data.Encoder.(EncodeStringer); ok {
				b.Run(fmt.Sprintf("%s/string (%d bytes) via EncodeString()", data.Name, size), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if err := enc.EncodeString(s); err != nil {
							panic(err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkEncodeArray(b *testing.B) {
	var a []int
	for _, size := range []int{math.MaxUint8, math.MaxUint8, math.MaxUint16} {
		a = append(a, size)
	}

	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/array via Encode()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(a); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeArrayer); ok {
			b.Run(fmt.Sprintf("%s/array via EncodeArray()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeArray(a); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

func BenchmarkEncodeMap(b *testing.B) {
	var m = make(map[string]int)
	for _, size := range []int{math.MaxUint8, math.MaxUint8, math.MaxUint16} {
		m[fmt.Sprintf("%d", size)] = size
	}

	for _, data := range encoders {
		if enc, ok := data.Encoder.(Encoder); ok {
			b.Run(fmt.Sprintf("%s/map via Encode()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc.Encode(m); err != nil {
						panic(err)
					}
				}
			})
		}
		if enc, ok := data.Encoder.(EncodeMapper); ok {
			b.Run(fmt.Sprintf("%s/map via EncodeMap()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc.EncodeMap(m); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}

type PatternReader struct {
	pattern []byte
	pos     int
}

func NewPatternReader(b []byte) *PatternReader {
	return &PatternReader{pattern: b}
}

func (r *PatternReader) Read(b []byte) (int, error) {
	n := copy(b, r.pattern[r.pos:])
	if n < len(r.pattern) {
		r.pos = r.pos + n
	}

	if r.pos >= len(r.pattern) {
		r.pos = 0
	}
	return n, nil
}

func BenchmarkDecodeUint8(b *testing.B) {
	for _, data := range encoders {
		rdr := NewPatternReader([]byte{lestrrat.Uint8.Byte(), byte(math.MaxUint8)})
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(DecodeUint8er); ok {
			b.Run(fmt.Sprintf("%s/uint8 via DecodeUint8()", data.Name), func(b *testing.B) {
				var v uint8
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeUint8(&v); err != nil {
						panic(err)
					}
					if v != math.MaxUint8 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		} else if dec, ok := canary.(DecodeUint8Returner); ok {
			b.Run(fmt.Sprintf("%s/uint8 via DecodeUint8() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeUint8()
					if err != nil {
						panic(err)
					}
					if v != math.MaxUint8 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeUint16(b *testing.B) {
	for _, data := range encoders {
		serialized := make([]byte, 3)
		serialized[0] = lestrrat.Uint16.Byte()
		binary.BigEndian.PutUint16(serialized[1:], math.MaxUint16)
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(DecodeUint16er); ok {
			b.Run(fmt.Sprintf("%s/uint16 via DecodeUint16()", data.Name), func(b *testing.B) {
				var v uint16
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeUint16(&v); err != nil {
						panic(err)
					}
					if v != math.MaxUint16 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		} else if dec, ok := canary.(DecodeUint16Returner); ok {
			b.Run(fmt.Sprintf("%s/uint16 via DecodeUint16() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeUint16()
					if err != nil {
						panic(err)
					}
					if v != math.MaxUint16 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeUint32(b *testing.B) {
	for _, data := range encoders {
		canary := data.MakeDecoder(&bytes.Buffer{})
		serialized := make([]byte, 5)
		serialized[0] = lestrrat.Uint32.Byte()
		binary.BigEndian.PutUint32(serialized[1:], math.MaxUint32)
		rdr := NewPatternReader(serialized)
		if _, ok := canary.(DecodeUint32er); ok {
			b.Run(fmt.Sprintf("%s/uint32 via DecodeUint32()", data.Name), func(b *testing.B) {
				var v uint32
				dec := data.MakeDecoder(rdr).(DecodeUint32er)
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeUint32(&v); err != nil {
						panic(err)
					}
					if v != math.MaxUint32 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		} else if _, ok := canary.(DecodeUint32Returner); ok {
			b.Run(fmt.Sprintf("%s/uint32 via DecodeUint32() (return)", data.Name), func(b *testing.B) {
				dec := data.MakeDecoder(rdr).(DecodeUint32Returner)
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeUint32()
					if err != nil {
						panic(err)
					}
					if v != math.MaxUint32 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeUint64(b *testing.B) {
	for _, data := range encoders {
		serialized := make([]byte, 9)
		serialized[0] = lestrrat.Uint64.Byte()
		binary.BigEndian.PutUint64(serialized[1:], math.MaxUint64)
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)

		switch dec := canary.(type) {
		case DecodeUint64er:
			b.Run(fmt.Sprintf("%s/uint64 via DecodeUint64()", data.Name), func(b *testing.B) {
				var v uint64
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeUint64(&v); err != nil {
						panic(err)
					}
					if v != math.MaxUint64 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		case DecodeUint64Returner:
			b.Run(fmt.Sprintf("%s/uint64 via DecodeUint64() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeUint64()
					if err != nil {
						panic(err)
					}
					if v != math.MaxUint64 {
						panic("v should be math.MaxUint :/")
					}
				}
			})
		default:
			panic("couldn't figure out type")
		}
	}
}

func BenchmarkDecodeInt8FixNum(b *testing.B) {
	for _, data := range encoders {
		canary := data.MakeDecoder(&bytes.Buffer{})
		rdr := NewPatternReader([]byte{0x7f})
		if _, ok := canary.(DecodeInt8er); ok {
			b.Run(fmt.Sprintf("%s/int8 via DecodeInt8()", data.Name), func(b *testing.B) {
				var v int8
				dec := data.MakeDecoder(rdr).(DecodeInt8er)
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeInt8(&v); err != nil {
						panic(err)
					}
					if v != math.MaxInt8 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		} else if _, ok := canary.(DecodeInt8Returner); ok {
			b.Run(fmt.Sprintf("%s/int8 via DecodeInt8() (return)", data.Name), func(b *testing.B) {
				dec := data.MakeDecoder(rdr).(DecodeInt8Returner)
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeInt8()
					if err != nil {
						panic(err)
					}
					if v != math.MaxInt8 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeInt8(b *testing.B) {
	for _, data := range encoders {
		canary := data.MakeDecoder(&bytes.Buffer{})
		rdr := NewPatternReader([]byte{lestrrat.Int8.Byte(), byte(math.MaxInt8)})
		if _, ok := canary.(DecodeInt8er); ok {
			b.Run(fmt.Sprintf("%s/int8 via DecodeInt8()", data.Name), func(b *testing.B) {
				var v int8
				dec := data.MakeDecoder(rdr).(DecodeInt8er)
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeInt8(&v); err != nil {
						panic(err)
					}
					if v != math.MaxInt8 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		} else if _, ok := canary.(DecodeInt8Returner); ok {
			b.Run(fmt.Sprintf("%s/int8 via DecodeInt8() (return)", data.Name), func(b *testing.B) {
				dec := data.MakeDecoder(rdr).(DecodeInt8Returner)
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeInt8()
					if err != nil {
						panic(err)
					}
					if v != math.MaxInt8 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeInt16(b *testing.B) {
	for _, data := range encoders {
		canary := data.MakeDecoder(&bytes.Buffer{})
		serialized := make([]byte, 3)
		serialized[0] = lestrrat.Int16.Byte()
		binary.BigEndian.PutUint16(serialized[1:], math.MaxInt16)
		rdr := NewPatternReader(serialized)
		if _, ok := canary.(DecodeInt16er); ok {
			b.Run(fmt.Sprintf("%s/int16 via DecodeInt16()", data.Name), func(b *testing.B) {
				var v int16
				dec := data.MakeDecoder(rdr).(DecodeInt16er)
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeInt16(&v); err != nil {
						panic(err)
					}
					if v != math.MaxInt16 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		} else if _, ok := canary.(DecodeInt16Returner); ok {
			b.Run(fmt.Sprintf("%s/int16 via DecodeInt16() (return)", data.Name), func(b *testing.B) {
				dec := data.MakeDecoder(rdr).(DecodeInt16Returner)
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeInt16()
					if err != nil {
						panic(err)
					}
					if v != math.MaxInt16 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeInt32(b *testing.B) {
	for _, data := range encoders {
		canary := data.MakeDecoder(&bytes.Buffer{})
		serialized := make([]byte, 5)
		serialized[0] = lestrrat.Int32.Byte()
		binary.BigEndian.PutUint32(serialized[1:], math.MaxInt32)
		rdr := NewPatternReader(serialized)
		if _, ok := canary.(DecodeInt32er); ok {
			b.Run(fmt.Sprintf("%s/int32 via DecodeInt32()", data.Name), func(b *testing.B) {
				var v int32
				dec := data.MakeDecoder(rdr).(DecodeInt32er)
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeInt32(&v); err != nil {
						panic(err)
					}
					if v != math.MaxInt32 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		} else if _, ok := canary.(DecodeInt32Returner); ok {
			b.Run(fmt.Sprintf("%s/int32 via DecodeInt32() (return)", data.Name), func(b *testing.B) {
				dec := data.MakeDecoder(rdr).(DecodeInt32Returner)
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeInt32()
					if err != nil {
						panic(err)
					}
					if v != math.MaxInt32 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeInt64(b *testing.B) {
	for _, data := range encoders {
		serialized := make([]byte, 9)
		serialized[0] = lestrrat.Int64.Byte()
		binary.BigEndian.PutUint64(serialized[1:], math.MaxInt64)
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)

		switch dec := canary.(type) {
		case DecodeInt64er:
			b.Run(fmt.Sprintf("%s/int64 via DecodeInt64()", data.Name), func(b *testing.B) {
				var v int64
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeInt64(&v); err != nil {
						panic(err)
					}
					if v != math.MaxInt64 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		case DecodeInt64Returner:
			b.Run(fmt.Sprintf("%s/int64 via DecodeInt64() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeInt64()
					if err != nil {
						panic(err)
					}
					if v != math.MaxInt64 {
						panic("v should be math.MaxInt :/")
					}
				}
			})
		default:
			panic("couldn't figure out type")
		}
	}
}

func BenchmarkDecodeFloat32(b *testing.B) {
	for _, data := range encoders {
		var serialized = make([]byte, 5)
		serialized[0] = lestrrat.Float.Byte()
		binary.BigEndian.PutUint32(serialized[1:], math.Float32bits(math.MaxFloat32))
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(DecodeFloat32er); ok {
			b.Run(fmt.Sprintf("%s/float32 via DecodeFloat32()", data.Name), func(b *testing.B) {
				var v float32
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeFloat32(&v); err != nil {
						panic(err)
					}
					if v != math.MaxFloat32 {
						panic("v should be math.MaxFloat :/")
					}
				}
			})
		} else if dec, ok := canary.(DecodeFloat32Returner); ok {
			b.Run(fmt.Sprintf("%s/float32 via DecodeFloat32() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeFloat32()
					if err != nil {
						panic(err)
					}
					if v != math.MaxFloat32 {
						panic("v should be math.MaxFloat :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeFloat64(b *testing.B) {
	for _, data := range encoders {
		var serialized = make([]byte, 9)
		serialized[0] = lestrrat.Double.Byte()
		binary.BigEndian.PutUint64(serialized[1:], math.Float64bits(math.MaxFloat64))
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(DecodeFloat64er); ok {
			b.Run(fmt.Sprintf("%s/float64 via DecodeFloat64()", data.Name), func(b *testing.B) {
				var v float64
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeFloat64(&v); err != nil {
						panic(err)
					}
					if v != math.MaxFloat64 {
						panic("v should be math.MaxFloat :/")
					}
				}
			})
		} else if dec, ok := canary.(DecodeFloat64Returner); ok {
			b.Run(fmt.Sprintf("%s/float64 via DecodeFloat64() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeFloat64()
					if err != nil {
						panic(err)
					}
					if v != math.MaxFloat64 {
						panic("v should be math.MaxFloat :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeString(b *testing.B) {
	var s = makeString(255)
	for _, data := range encoders {
		serialized, _ := lestrrat.Marshal(s)
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(Decoder); ok {
			b.Run(fmt.Sprintf("%s/string via Decode()", data.Name), func(b *testing.B) {
				var v string
				for i := 0; i < b.N; i++ {
					if err := dec.Decode(&v); err != nil {
						panic(err)
					}
					if v != s {
						panic("v should be s :/")
					}
				}
			})
		}
		if dec, ok := canary.(DecodeStringer); ok {
			b.Run(fmt.Sprintf("%s/string via DecodeString()", data.Name), func(b *testing.B) {
				var v string
				for i := 0; i < b.N; i++ {
					if err := dec.DecodeString(&v); err != nil {
						panic(err)
					}
					if v != s {
						panic("v should be s :/")
					}
				}
			})
		} else if dec, ok := canary.(DecodeStringReturner); ok {
			b.Run(fmt.Sprintf("%s/string via DecodeString() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					v, err := dec.DecodeString()
					if err != nil {
						panic(err)
					}
					if v != s {
						panic("v should be s :/")
					}
				}
			})
		}
	}
}

func BenchmarkDecodeArray(b *testing.B) {
	builder := lestrrat.NewArrayBuilder()
	builder.Add(math.MaxUint8)
	builder.Add(math.MaxUint16)
	builder.Add(math.MaxUint32)
	serialized, _ := builder.Bytes()
	rdr := NewPatternReader(serialized)
	for _, data := range encoders {
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(Decoder); ok {
			b.Run(fmt.Sprintf("%s/array via Decode() (concrete)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var a []interface{}
					if err := dec.Decode(&a); err != nil {
						panic(err)
					}
				}
			})
			b.Run(fmt.Sprintf("%s/array via Decode() (interface{})", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var a interface{}
					if err := dec.Decode(&a); err != nil {
						panic(err)
					}
				}
			})
		}
		if dec, ok := canary.(DecodeArrayer); ok {
			b.Run(fmt.Sprintf("%s/array via DecodeArray()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var a []interface{}
					if err := dec.DecodeArray(&a); err != nil {
						panic(err)
					}
					_ = a
				}
			})
		}
		if dec, ok := canary.(DecodeArrayReturner); ok {
			b.Run(fmt.Sprintf("%s/array via DecodeArray() (return)", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					a, err := dec.DecodeArray()
					if err != nil {
						panic(err)
					}
					_ = a
				}
			})
		}
	}
}

func BenchmarkDecodeMap(b *testing.B) {
	builder := lestrrat.NewMapBuilder()
	builder.Add("uint8", math.MaxUint8)
	builder.Add("uint16", math.MaxUint16)
	builder.Add("uint32", math.MaxUint32)
	serialized, _ := builder.Bytes()
	for _, data := range encoders {
		rdr := NewPatternReader(serialized)
		canary := data.MakeDecoder(rdr)
		if dec, ok := canary.(Decoder); ok {
			b.Run(fmt.Sprintf("%s/map via Decode()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var m map[string]interface{}
					if err := dec.Decode(&m); err != nil {
						panic(err)
					}
				}
			})
		}
		if dec, ok := canary.(DecodeMapper); ok {
			b.Run(fmt.Sprintf("%s/map via DecodeMap()", data.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var m map[string]interface{}
					if err := dec.DecodeMap(&m); err != nil {
						panic(err)
					}
				}
			})
		}
	}
}
