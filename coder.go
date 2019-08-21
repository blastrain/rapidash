package rapidash

import (
	"bytes"
	"time"

	"github.com/lestrrat-go/msgpack"
	"golang.org/x/xerrors"
)

func Int(v int) *IntCoder             { return &IntCoder{v: &v} }
func Int8(v int8) *Int8Coder          { return &Int8Coder{v: &v} }
func Int16(v int16) *Int16Coder       { return &Int16Coder{v: &v} }
func Int32(v int32) *Int32Coder       { return &Int32Coder{v: &v} }
func Int64(v int64) *Int64Coder       { return &Int64Coder{v: &v} }
func Uint(v uint) *UintCoder          { return &UintCoder{v: &v} }
func Uint8(v uint8) *Uint8Coder       { return &Uint8Coder{v: &v} }
func Uint16(v uint16) *Uint16Coder    { return &Uint16Coder{v: &v} }
func Uint32(v uint32) *Uint32Coder    { return &Uint32Coder{v: &v} }
func Uint64(v uint64) *Uint64Coder    { return &Uint64Coder{v: &v} }
func Float32(v float32) *Float32Coder { return &Float32Coder{v: &v} }
func Float64(v float64) *Float64Coder { return &Float64Coder{v: &v} }
func String(v string) *StringCoder    { return &StringCoder{v: &v} }
func Bytes(v []byte) *BytesCoder      { return &BytesCoder{v: &v} }
func Bool(v bool) *BoolCoder          { return &BoolCoder{v: &v} }
func Time(v time.Time) *TimeCoder     { return &TimeCoder{v: &v} }

func IntPtr(v *int) *IntCoder             { return &IntCoder{v: v} }
func Int8Ptr(v *int8) *Int8Coder          { return &Int8Coder{v: v} }
func Int16Ptr(v *int16) *Int16Coder       { return &Int16Coder{v: v} }
func Int32Ptr(v *int32) *Int32Coder       { return &Int32Coder{v: v} }
func Int64Ptr(v *int64) *Int64Coder       { return &Int64Coder{v: v} }
func UintPtr(v *uint) *UintCoder          { return &UintCoder{v: v} }
func Uint8Ptr(v *uint8) *Uint8Coder       { return &Uint8Coder{v: v} }
func Uint16Ptr(v *uint16) *Uint16Coder    { return &Uint16Coder{v: v} }
func Uint32Ptr(v *uint32) *Uint32Coder    { return &Uint32Coder{v: v} }
func Uint64Ptr(v *uint64) *Uint64Coder    { return &Uint64Coder{v: v} }
func Float32Ptr(v *float32) *Float32Coder { return &Float32Coder{v: v} }
func Float64Ptr(v *float64) *Float64Coder { return &Float64Coder{v: v} }
func StringPtr(v *string) *StringCoder    { return &StringCoder{v: v} }
func BytesPtr(v *[]byte) *BytesCoder      { return &BytesCoder{v: v} }
func BoolPtr(v *bool) *BoolCoder          { return &BoolCoder{v: v} }
func TimePtr(v *time.Time) *TimeCoder     { return &TimeCoder{v: v} }

func Ints(v []int) *IntsCoder                    { return &IntsCoder{v: &v} }
func Int8s(v []int8) *Int8sCoder                 { return &Int8sCoder{v: &v} }
func Int16s(v []int16) *Int16sCoder              { return &Int16sCoder{v: &v} }
func Int32s(v []int32) *Int32sCoder              { return &Int32sCoder{v: &v} }
func Int64s(v []int64) *Int64sCoder              { return &Int64sCoder{v: &v} }
func Uints(v []uint) *UintsCoder                 { return &UintsCoder{v: &v} }
func Uint8s(v []uint8) *Uint8sCoder              { return &Uint8sCoder{v: &v} }
func Uint16s(v []uint16) *Uint16sCoder           { return &Uint16sCoder{v: &v} }
func Uint32s(v []uint32) *Uint32sCoder           { return &Uint32sCoder{v: &v} }
func Uint64s(v []uint64) *Uint64sCoder           { return &Uint64sCoder{v: &v} }
func Float32s(v []float32) *Float32sCoder        { return &Float32sCoder{v: &v} }
func Float64s(v []float64) *Float64sCoder        { return &Float64sCoder{v: &v} }
func Strings(v []string) *StringsCoder           { return &StringsCoder{v: &v} }
func Bools(v []bool) *BoolsCoder                 { return &BoolsCoder{v: &v} }
func Times(v []time.Time) *TimesCoder            { return &TimesCoder{v: &v} }
func Structs(v Coder, typ *Struct) *StructsCoder { return &StructsCoder{v: v, typ: typ} }

func IntsPtr(v *[]int) *IntsCoder                   { return &IntsCoder{v: v} }
func Int8sPtr(v *[]int8) *Int8sCoder                { return &Int8sCoder{v: v} }
func Int16sPtr(v *[]int16) *Int16sCoder             { return &Int16sCoder{v: v} }
func Int32sPtr(v *[]int32) *Int32sCoder             { return &Int32sCoder{v: v} }
func Int64sPtr(v *[]int64) *Int64sCoder             { return &Int64sCoder{v: v} }
func UintsPtr(v *[]uint) *UintsCoder                { return &UintsCoder{v: v} }
func Uint8sPtr(v *[]uint8) *Uint8sCoder             { return &Uint8sCoder{v: v} }
func Uint16sPtr(v *[]uint16) *Uint16sCoder          { return &Uint16sCoder{v: v} }
func Uint32sPtr(v *[]uint32) *Uint32sCoder          { return &Uint32sCoder{v: v} }
func Uint64sPtr(v *[]uint64) *Uint64sCoder          { return &Uint64sCoder{v: v} }
func Float32sPtr(v *[]float32) *Float32sCoder       { return &Float32sCoder{v: v} }
func Float64sPtr(v *[]float64) *Float64sCoder       { return &Float64sCoder{v: v} }
func StringsPtr(v *[]string) *StringsCoder          { return &StringsCoder{v: v} }
func BoolsPtr(v *[]bool) *BoolsCoder                { return &BoolsCoder{v: v} }
func TimesPtr(v *[]time.Time) *TimesCoder           { return &TimesCoder{v: v} }
func StructsPtr(v Coder, typ *Struct) *StructsCoder { return &StructsCoder{v: v, typ: typ} }

type IntCoder struct {
	v *int
}

func (c *IntCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeInt(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode int: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *IntCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeInt(c.v); err != nil {
		return xerrors.Errorf("failed to decode int: %w", err)
	}
	return nil
}

type Int8Coder struct {
	v *int8
}

func (c *Int8Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeInt8(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode int8: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Int8Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeInt8(c.v); err != nil {
		return xerrors.Errorf("failed to decode int8: %w", err)
	}
	return nil
}

type Int16Coder struct {
	v *int16
}

func (c *Int16Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeInt16(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode int16: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Int16Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeInt16(c.v); err != nil {
		return xerrors.Errorf("failed to decode int16: %w", err)
	}
	return nil
}

type Int32Coder struct {
	v *int32
}

func (c *Int32Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeInt32(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode int32: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Int32Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeInt32(c.v); err != nil {
		return xerrors.Errorf("failed to decode int32: %w", err)
	}
	return nil
}

type Int64Coder struct {
	v *int64
}

func (c *Int64Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeInt64(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode int64: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Int64Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeInt64(c.v); err != nil {
		return xerrors.Errorf("failed to decode int64: %w", err)
	}
	return nil
}

type UintCoder struct {
	v *uint
}

func (c *UintCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeUint(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode uint: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *UintCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeUint(c.v); err != nil {
		return xerrors.Errorf("failed to decode uint: %w", err)
	}
	return nil
}

type Uint8Coder struct {
	v *uint8
}

func (c *Uint8Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeUint8(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode uint8: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Uint8Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeUint8(c.v); err != nil {
		return xerrors.Errorf("failed to decode uint8: %w", err)
	}
	return nil
}

type Uint16Coder struct {
	v *uint16
}

func (c *Uint16Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeUint16(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode uint16: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Uint16Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeUint16(c.v); err != nil {
		return xerrors.Errorf("failed to decode uint16: %w", err)
	}
	return nil
}

type Uint32Coder struct {
	v *uint32
}

func (c *Uint32Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeUint32(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode uint32: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Uint32Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeUint32(c.v); err != nil {
		return xerrors.Errorf("failed to decode uint32: %w", err)
	}
	return nil
}

type Uint64Coder struct {
	v *uint64
}

func (c *Uint64Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeUint64(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode uint64: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Uint64Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeUint64(c.v); err != nil {
		return xerrors.Errorf("failed to decode uint64: %w", err)
	}
	return nil
}

type Float32Coder struct {
	v *float32
}

func (c *Float32Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeFloat32(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode float32: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Float32Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeFloat32(c.v); err != nil {
		return xerrors.Errorf("failed to decode float32: %w", err)
	}
	return nil
}

type Float64Coder struct {
	v *float64
}

func (c *Float64Coder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeFloat64(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode float64: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *Float64Coder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeFloat64(c.v); err != nil {
		return xerrors.Errorf("failed to decode float64: %w", err)
	}
	return nil
}

type StringCoder struct {
	v *string
}

func (c *StringCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeString(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode string: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *StringCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeString(c.v); err != nil {
		return xerrors.Errorf("failed to decode string: %w", err)
	}
	return nil
}

type BytesCoder struct {
	v *[]byte
}

func (c *BytesCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeBytes(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode []byte: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *BytesCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeBytes(c.v); err != nil {
		return xerrors.Errorf("failed to decode []byte: %w", err)
	}
	return nil
}

type BoolCoder struct {
	v *bool
}

func (c *BoolCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeBool(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode bool: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *BoolCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeBool(c.v); err != nil {
		return xerrors.Errorf("failed to decode bool: %w", err)
	}
	return nil
}

type TimeCoder struct {
	v *time.Time
}

func (c *TimeCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeTime(*c.v); err != nil {
		return nil, xerrors.Errorf("failed to encode time.Time: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *TimeCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	if err := dec.DecodeTime(c.v); err != nil {
		return xerrors.Errorf("failed to decode time.Time: %w", err)
	}
	return nil
}

type StructCoder struct {
	typ   *Struct
	value Coder
}

func (c *StructCoder) Encode() ([]byte, error) {
	enc := NewStructEncoder(c.typ, NewValueFactory())
	if err := c.value.EncodeRapidash(enc); err != nil {
		return nil, xerrors.Errorf("failed to encode struct: %w", err)
	}
	content, err := enc.Encode()
	if err != nil {
		return nil, xerrors.Errorf("failed to encode: %w", err)
	}
	return content, nil
}

func (c *StructCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := NewDecoder(c.typ, buf, NewValueFactory())
	value, err := dec.Decode()
	if err != nil {
		return xerrors.Errorf("failed to decode struct: %w", err)
	}
	if err := c.value.DecodeRapidash(value); err != nil {
		return xerrors.Errorf("failed to decode value: %w", err)
	}
	return nil
}

type IntsCoder struct {
	v *[]int
}

func (c *IntsCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []int: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeInt(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []int: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *IntsCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []int: %w", err)
	}
	*c.v = make([]int, len)
	for i := 0; i < len; i++ {
		var v int
		if err := dec.DecodeInt(&v); err != nil {
			return xerrors.Errorf("failed to decode []int: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Int8sCoder struct {
	v *[]int8
}

func (c *Int8sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []int8: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeInt8(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []int8: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Int8sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []int8: %w", err)
	}
	*c.v = make([]int8, len)
	for i := 0; i < len; i++ {
		var v int8
		if err := dec.DecodeInt8(&v); err != nil {
			return xerrors.Errorf("failed to decode []int8: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Int16sCoder struct {
	v *[]int16
}

func (c *Int16sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []int16: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeInt16(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []int16: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Int16sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []int16: %w", err)
	}
	*c.v = make([]int16, len)
	for i := 0; i < len; i++ {
		var v int16
		if err := dec.DecodeInt16(&v); err != nil {
			return xerrors.Errorf("failed to decode []int16: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Int32sCoder struct {
	v *[]int32
}

func (c *Int32sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []int32: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeInt32(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []int32: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Int32sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []int32: %w", err)
	}
	*c.v = make([]int32, len)
	for i := 0; i < len; i++ {
		var v int32
		if err := dec.DecodeInt32(&v); err != nil {
			return xerrors.Errorf("failed to decode []int32: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Int64sCoder struct {
	v *[]int64
}

func (c *Int64sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []int64: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeInt64(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []int64: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Int64sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []int64: %w", err)
	}
	*c.v = make([]int64, len)
	for i := 0; i < len; i++ {
		var v int64
		if err := dec.DecodeInt64(&v); err != nil {
			return xerrors.Errorf("failed to decode []int64: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type UintsCoder struct {
	v *[]uint
}

func (c *UintsCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []uint: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeUint(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []uint: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *UintsCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []uint: %w", err)
	}
	*c.v = make([]uint, len)
	for i := 0; i < len; i++ {
		var v uint
		if err := dec.DecodeUint(&v); err != nil {
			return xerrors.Errorf("failed to decode []uint: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Uint8sCoder struct {
	v *[]uint8
}

func (c *Uint8sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []uint8: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeUint8(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []uint8: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Uint8sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []uint8: %w", err)
	}
	*c.v = make([]uint8, len)
	for i := 0; i < len; i++ {
		var v uint8
		if err := dec.DecodeUint8(&v); err != nil {
			return xerrors.Errorf("failed to decode []uint8: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Uint16sCoder struct {
	v *[]uint16
}

func (c *Uint16sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []uint16: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeUint16(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []uint16: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Uint16sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []uint16: %w", err)
	}
	*c.v = make([]uint16, len)
	for i := 0; i < len; i++ {
		var v uint16
		if err := dec.DecodeUint16(&v); err != nil {
			return xerrors.Errorf("failed to decode []uint16: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Uint32sCoder struct {
	v *[]uint32
}

func (c *Uint32sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []uint32: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeUint32(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []uint32: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Uint32sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []uint32: %w", err)
	}
	*c.v = make([]uint32, len)
	for i := 0; i < len; i++ {
		var v uint32
		if err := dec.DecodeUint32(&v); err != nil {
			return xerrors.Errorf("failed to decode []uint32: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Uint64sCoder struct {
	v *[]uint64
}

func (c *Uint64sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []uint64: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeUint64(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []uint64: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Uint64sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []uint64: %w", err)
	}
	*c.v = make([]uint64, len)
	for i := 0; i < len; i++ {
		var v uint64
		if err := dec.DecodeUint64(&v); err != nil {
			return xerrors.Errorf("failed to decode []uint64: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Float32sCoder struct {
	v *[]float32
}

func (c *Float32sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []float32: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeFloat32(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []float32: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Float32sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []float32: %w", err)
	}
	*c.v = make([]float32, len)
	for i := 0; i < len; i++ {
		var v float32
		if err := dec.DecodeFloat32(&v); err != nil {
			return xerrors.Errorf("failed to decode []float32: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type Float64sCoder struct {
	v *[]float64
}

func (c *Float64sCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []float64: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeFloat64(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []float64: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *Float64sCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []float64: %w", err)
	}
	*c.v = make([]float64, len)
	for i := 0; i < len; i++ {
		var v float64
		if err := dec.DecodeFloat64(&v); err != nil {
			return xerrors.Errorf("failed to decode []float64: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type StringsCoder struct {
	v *[]string
}

func (c *StringsCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []string: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeString(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []string: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *StringsCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []string: %w", err)
	}
	*c.v = make([]string, len)
	for i := 0; i < len; i++ {
		var v string
		if err := dec.DecodeString(&v); err != nil {
			return xerrors.Errorf("failed to decode []string: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type BoolsCoder struct {
	v *[]bool
}

func (c *BoolsCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []bool: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeBool(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []bool: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *BoolsCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []bool: %w", err)
	}
	*c.v = make([]bool, len)
	for i := 0; i < len; i++ {
		var v bool
		if err := dec.DecodeBool(&v); err != nil {
			return xerrors.Errorf("failed to decode []bool: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type TimesCoder struct {
	v *[]time.Time
}

func (c *TimesCoder) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(*c.v)); err != nil {
		return nil, xerrors.Errorf("failed to encode length of []time.Time: %w", err)
	}
	for _, v := range *c.v {
		if err := enc.EncodeTime(v); err != nil {
			return nil, xerrors.Errorf("failed to encode []time.Time: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func (c *TimesCoder) Decode(content []byte) error {
	buf := bytes.NewBuffer(content)
	dec := msgpack.NewDecoder(buf)
	var len int
	if err := dec.DecodeArrayLength(&len); err != nil {
		return xerrors.Errorf("failed to decode length of []time.Time: %w", err)
	}
	*c.v = make([]time.Time, len)
	for i := 0; i < len; i++ {
		var v time.Time
		if err := dec.DecodeTime(&v); err != nil {
			return xerrors.Errorf("failed to decode []time.Time: %w", err)
		}
		(*c.v)[i] = v
	}
	return nil
}

type StructsCoder struct {
	v   Coder
	typ *Struct
}

func (c *StructsCoder) Encode() ([]byte, error) {
	content, err := NewStructSliceEncoder(c.typ, NewValueFactory(), c.v).Encode()
	if err != nil {
		return nil, xerrors.Errorf("failed to encode slice of struct: %w", err)
	}
	return content, nil
}

func (c *StructsCoder) Decode(content []byte) error {
	dec := NewDecoder(c.typ, &bytes.Buffer{}, NewValueFactory())
	dec.SetBuffer(content)
	v, err := dec.DecodeSlice()
	if err != nil {
		return xerrors.Errorf("failed to decode slice of struct: %w", err)
	}
	if err := c.v.DecodeRapidash(v); err != nil {
		return xerrors.Errorf("failed to decode value: %w", err)
	}
	return nil
}
