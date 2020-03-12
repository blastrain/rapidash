package rapidash

import (
	"bytes"
	"time"

	"github.com/lestrrat-go/msgpack"
	"golang.org/x/xerrors"
)

type Decoder interface {
	Len() int
	At(int) Decoder
	Int(string) int
	Int8(string) int8
	Int16(string) int16
	Int32(string) int32
	Int64(string) int64
	Uint(string) uint
	Uint8(string) uint8
	Uint16(string) uint16
	Uint32(string) uint32
	Uint64(string) uint64
	Float32(string) float32
	Float64(string) float64
	Bool(string) bool
	String(string) string
	Bytes(string) []byte
	Time(string) time.Time
	Slice(string, Unmarshaler)
	Struct(string, Unmarshaler)
	IntPtr(string) *int
	Int8Ptr(string) *int8
	Int16Ptr(string) *int16
	Int32Ptr(string) *int32
	Int64Ptr(string) *int64
	UintPtr(string) *uint
	Uint8Ptr(string) *uint8
	Uint16Ptr(string) *uint16
	Uint32Ptr(string) *uint32
	Uint64Ptr(string) *uint64
	Float32Ptr(string) *float32
	Float64Ptr(string) *float64
	BoolPtr(string) *bool
	StringPtr(string) *string
	BytesPtr(string) *[]byte
	TimePtr(string) *time.Time
	Ints(string) []int
	Int8s(string) []int8
	Int16s(string) []int16
	Int32s(string) []int32
	Int64s(string) []int64
	Uints(string) []uint
	Uint8s(string) []uint8
	Uint16s(string) []uint16
	Uint32s(string) []uint32
	Uint64s(string) []uint64
	Float32s(string) []float32
	Float64s(string) []float64
	Bools(string) []bool
	Strings(string) []string
	Times(string) []time.Time
	Error() error
}

type PrimaryKeyDecoder struct {
	buf *bytes.Buffer
	dec *msgpack.Decoder
}

func (d *PrimaryKeyDecoder) SetBuffer(content []byte) {
	d.buf.Reset()
	d.buf.Write(content)
	d.dec.Reset(d.buf)
}

func (d *PrimaryKeyDecoder) Decode() (string, error) {
	var primaryKey string
	if err := d.dec.DecodeString(&primaryKey); err != nil {
		return "", xerrors.Errorf("failed to decode primary key: %w", err)
	}
	return primaryKey, nil
}

func NewPrimaryKeyDecoder(buf *bytes.Buffer) *PrimaryKeyDecoder {
	return &PrimaryKeyDecoder{
		buf: buf,
		dec: msgpack.NewDecoder(buf),
	}
}

type ValueDecoder struct {
	typ          *Struct
	columns      []string
	dec          *msgpack.Decoder
	buf          *bytes.Buffer
	valueFactory *ValueFactory
	decoderMap   map[TypeID]func(*StructField) (*Value, error)
}

func NewDecoder(s *Struct, buf *bytes.Buffer, valueFactory *ValueFactory) *ValueDecoder {
	dec := msgpack.NewDecoder(buf)
	d := &ValueDecoder{
		typ:          s,
		dec:          dec,
		columns:      s.Columns(),
		buf:          buf,
		valueFactory: valueFactory,
	}
	d.decoderMap = map[TypeID]func(*StructField) (*Value, error){
		IntType: func(*StructField) (*Value, error) {
			var v int
			if err := dec.DecodeInt(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode int: %w", err)
			}
			return valueFactory.CreateIntValue(v), nil
		},
		Int8Type: func(*StructField) (*Value, error) {
			var v int8
			if err := dec.DecodeInt8(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode int8: %w", err)
			}
			return valueFactory.CreateInt8Value(v), nil
		},
		Int16Type: func(*StructField) (*Value, error) {
			var v int16
			if err := dec.DecodeInt16(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode int16: %w", err)
			}
			return valueFactory.CreateInt16Value(v), nil
		},
		Int32Type: func(*StructField) (*Value, error) {
			var v int32
			if err := dec.DecodeInt32(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode int32: %w", err)
			}
			return valueFactory.CreateInt32Value(v), nil
		},
		Int64Type: func(*StructField) (*Value, error) {
			var v int64
			if err := dec.DecodeInt64(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode int64: %w", err)
			}
			return valueFactory.CreateInt64Value(v), nil
		},
		UintType: func(*StructField) (*Value, error) {
			var v uint
			if err := dec.DecodeUint(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode uint: %w", err)
			}
			return valueFactory.CreateUintValue(v), nil
		},
		Uint8Type: func(*StructField) (*Value, error) {
			var v uint8
			if err := dec.DecodeUint8(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode uint8: %w", err)
			}
			return valueFactory.CreateUint8Value(v), nil
		},
		Uint16Type: func(*StructField) (*Value, error) {
			var v uint16
			if err := dec.DecodeUint16(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode uint16: %w", err)
			}
			return valueFactory.CreateUint16Value(v), nil
		},
		Uint32Type: func(*StructField) (*Value, error) {
			var v uint32
			if err := dec.DecodeUint32(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode uint32: %w", err)
			}
			return valueFactory.CreateUint32Value(v), nil
		},
		Uint64Type: func(*StructField) (*Value, error) {
			var v uint64
			if err := dec.DecodeUint64(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode uint64: %w", err)
			}
			return valueFactory.CreateUint64Value(v), nil
		},
		Float32Type: func(*StructField) (*Value, error) {
			var v float32
			if err := dec.DecodeFloat32(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode float32: %w", err)
			}
			return valueFactory.CreateFloat32Value(v), nil
		},
		Float64Type: func(*StructField) (*Value, error) {
			var v float64
			if err := dec.DecodeFloat64(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode float64: %w", err)
			}
			return valueFactory.CreateFloat64Value(v), nil
		},
		BoolType: func(*StructField) (*Value, error) {
			var v bool
			if err := dec.DecodeBool(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode bool: %w", err)
			}
			return valueFactory.CreateBoolValue(v), nil
		},
		StringType: func(*StructField) (*Value, error) {
			var v string
			if err := dec.DecodeString(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode string: %w", err)
			}
			return valueFactory.CreateStringValue(v), nil
		},
		BytesType: func(*StructField) (*Value, error) {
			var v []byte
			if err := dec.DecodeBytes(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode []byte: %w", err)
			}
			return valueFactory.CreateBytesValue(v), nil
		},
		TimeType: func(*StructField) (*Value, error) {
			var v time.Time
			if err := dec.DecodeTime(&v); err != nil {
				return nil, xerrors.Errorf("failed to decode time.Time: %w", err)
			}
			return valueFactory.CreateTimeValue(v), nil
		},
		SliceType:  d.decodeSliceValue,
		StructType: d.decodeStructValue,
	}
	return d
}

func (d *ValueDecoder) SetBuffer(content []byte) {
	d.buf.Reset()
	d.buf.Write(content)
	d.dec.Reset(d.buf)
}

func (d *ValueDecoder) decodeStructValue(field *StructField) (*Value, error) {
	structType := field.subtypeStruct
	value := &StructValue{
		typ:    structType,
		fields: map[string]*Value{},
	}
	for _, column := range structType.Columns() {
		v, err := d.decodeValue(structType.fields[column])
		if err != nil {
			return nil, xerrors.Errorf("failed to decode value: %w", err)
		}
		value.fields[column] = v
	}
	return StructValueToValue(value), nil
}

func (d *ValueDecoder) decodeSliceValue(field *StructField) (*Value, error) {
	var len int
	if err := d.dec.DecodeArrayLength(&len); err != nil {
		return nil, xerrors.Errorf("failed to decode array length: %w", err)
	}
	values := []*Value{}
	subField := &StructField{
		typ:           field.subtype,
		column:        field.column,
		index:         field.index,
		subtypeStruct: field.subtypeStruct,
	}
	for i := 0; i < len; i++ {
		v, err := d.decodeValue(subField)
		if err != nil {
			return nil, xerrors.Errorf("failed to decode value: %w", err)
		}
		values = append(values, v)
	}
	return ValuesToValue(values), nil
}

func (d *ValueDecoder) decodeValue(field *StructField) (*Value, error) {
	dec := d.dec
	code, err := dec.PeekCode()
	if err != nil {
		return nil, xerrors.Errorf("failed to get peek code: %w", err)
	}
	if code == msgpack.Nil {
		var v interface{}
		if err := dec.DecodeNil(&v); err != nil {
			return nil, xerrors.Errorf("failed to decode nil: %w", err)
		}
		return nilValue, nil
	}
	value, err := d.decoderMap[field.typ](field)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode: %w", err)
	}
	return value, nil
}

func (d *ValueDecoder) Decode() (*StructValue, error) {
	value := &StructValue{
		typ:    d.typ,
		fields: make(map[string]*Value, len(d.columns)),
	}
	for _, column := range d.columns {
		v, err := d.decodeValue(d.typ.fields[column])
		if err != nil {
			return nil, xerrors.Errorf("failed to decode value: %w", err)
		}
		value.fields[column] = v
	}
	return value, nil
}

func (d *ValueDecoder) DecodeSlice() (*StructSliceValue, error) {
	values := NewStructSliceValue()
	var len int
	if err := d.dec.DecodeArrayLength(&len); err != nil {
		return nil, xerrors.Errorf("failed to decode array length: %w", err)
	}
	for i := 0; i < len; i++ {
		value := &StructValue{
			typ:    d.typ,
			fields: map[string]*Value{},
		}
		for _, column := range d.columns {
			v, err := d.decodeValue(d.typ.fields[column])
			if err != nil {
				return nil, xerrors.Errorf("failed to decode value: %w", err)
			}
			value.fields[column] = v
		}
		values.Append(value)
	}
	return values, nil
}
