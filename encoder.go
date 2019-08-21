package rapidash

import (
	"bytes"
	"time"

	"github.com/lestrrat-go/msgpack"
	"golang.org/x/xerrors"
)

type Encoder interface {
	Error() error
	New() Encoder

	// raw types

	Int(string, int)
	Int8(string, int8)
	Int16(string, int16)
	Int32(string, int32)
	Int64(string, int64)
	Uint(string, uint)
	Uint8(string, uint8)
	Uint16(string, uint16)
	Uint32(string, uint32)
	Uint64(string, uint64)
	Float32(string, float32)
	Float64(string, float64)
	String(string, string)
	Bytes(string, []byte)
	Bool(string, bool)
	Time(string, time.Time)

	// pointer types

	IntPtr(string, *int)
	Int8Ptr(string, *int8)
	Int16Ptr(string, *int16)
	Int32Ptr(string, *int32)
	Int64Ptr(string, *int64)
	UintPtr(string, *uint)
	Uint8Ptr(string, *uint8)
	Uint16Ptr(string, *uint16)
	Uint32Ptr(string, *uint32)
	Uint64Ptr(string, *uint64)
	Float32Ptr(string, *float32)
	Float64Ptr(string, *float64)
	StringPtr(string, *string)
	BytesPtr(string, *[]byte)
	BoolPtr(string, *bool)
	TimePtr(string, *time.Time)
	Struct(string, Marshaler)

	// raw slice types

	Ints(string, []int)
	Int8s(string, []int8)
	Int16s(string, []int16)
	Int32s(string, []int32)
	Int64s(string, []int64)
	Uints(string, []uint)
	Uint8s(string, []uint8)
	Uint16s(string, []uint16)
	Uint32s(string, []uint32)
	Uint64s(string, []uint64)
	Float32s(string, []float32)
	Float64s(string, []float64)
	Strings(string, []string)
	Bools(string, []bool)
	Times(string, []time.Time)
	Structs(string, Marshaler)
}

type StructEncoder struct {
	typ          *Struct
	valueFactory *ValueFactory
	value        *StructValue
	slice        *StructSliceValue
	err          error
}

func NewStructEncoder(s *Struct, factory *ValueFactory) *StructEncoder {
	return &StructEncoder{
		typ:          s,
		valueFactory: factory,
		value: &StructValue{
			typ:    s,
			fields: map[string]*Value{},
		},
		slice: &StructSliceValue{values: []*StructValue{}},
	}
}

func (e *StructEncoder) New() Encoder {
	enc := NewStructEncoder(e.typ, e.valueFactory)
	e.slice.values = append(e.slice.values, enc.value)
	return enc
}

func (e *StructEncoder) Error() error {
	return e.err
}

func (e *StructEncoder) Int(column string, v int) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != IntType {
		e.err = xerrors.Errorf("%s.%s type is %s but required int: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateIntValue(v)
}

func (e *StructEncoder) Int8(column string, v int8) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int8Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required int8: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt8Value(v)
}

func (e *StructEncoder) Int16(column string, v int16) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int16Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required int16: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt16Value(v)
}

func (e *StructEncoder) Int32(column string, v int32) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int32Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required int32: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt32Value(v)
}

func (e *StructEncoder) Int64(column string, v int64) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int64Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required int64: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt64Value(v)
}

func (e *StructEncoder) Uint(column string, v uint) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != UintType {
		e.err = xerrors.Errorf("%s.%s type is %s but required uint: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUintValue(v)
}

func (e *StructEncoder) Uint8(column string, v uint8) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint8Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required uint8: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint8Value(v)
}

func (e *StructEncoder) Uint16(column string, v uint16) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint16Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required uint16: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint16Value(v)
}

func (e *StructEncoder) Uint32(column string, v uint32) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint32Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required uint32: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint32Value(v)
}

func (e *StructEncoder) Uint64(column string, v uint64) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint64Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required uint64: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint64Value(v)
}

func (e *StructEncoder) Float32(column string, v float32) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Float32Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required float32: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateFloat32Value(v)
}

func (e *StructEncoder) Float64(column string, v float64) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Float64Type {
		e.err = xerrors.Errorf("%s.%s type is %s but required float64: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateFloat64Value(v)
}

func (e *StructEncoder) String(column string, v string) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != StringType {
		e.err = xerrors.Errorf("%s.%s type is %s but required string: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateStringValue(v)
}

func (e *StructEncoder) Bytes(column string, v []byte) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != BytesType {
		e.err = xerrors.Errorf("%s.%s type is %s but required []byte: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateBytesValue(v)
}

func (e *StructEncoder) Bool(column string, v bool) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != BoolType {
		e.err = xerrors.Errorf("%s.%s type is %s but required bool: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateBoolValue(v)
}

func (e *StructEncoder) Time(column string, v time.Time) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != TimeType {
		e.err = xerrors.Errorf("%s.%s type is %s but required time.Time: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateTimeValue(v)
}

func (e *StructEncoder) IntPtr(column string, v *int) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != IntType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *int: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateIntPtrValue(v)
}

func (e *StructEncoder) Int8Ptr(column string, v *int8) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int8Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *int8: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt8PtrValue(v)
}

func (e *StructEncoder) Int16Ptr(column string, v *int16) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int16Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *int16: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt16PtrValue(v)
}

func (e *StructEncoder) Int32Ptr(column string, v *int32) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int32Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *int32: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt32PtrValue(v)
}

func (e *StructEncoder) Int64Ptr(column string, v *int64) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Int64Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *int64: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateInt64PtrValue(v)
}

func (e *StructEncoder) UintPtr(column string, v *uint) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != UintType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *uint: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUintPtrValue(v)
}

func (e *StructEncoder) Uint8Ptr(column string, v *uint8) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint8Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *uint8: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint8PtrValue(v)
}

func (e *StructEncoder) Uint16Ptr(column string, v *uint16) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint16Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *uint16: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint16PtrValue(v)
}

func (e *StructEncoder) Uint32Ptr(column string, v *uint32) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint32Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *uint32: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint32PtrValue(v)
}

func (e *StructEncoder) Uint64Ptr(column string, v *uint64) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Uint64Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *uint64: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateUint64PtrValue(v)
}

func (e *StructEncoder) Float32Ptr(column string, v *float32) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Float32Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *float32: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateFloat32PtrValue(v)
}

func (e *StructEncoder) Float64Ptr(column string, v *float64) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != Float64Type {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *float64: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateFloat64PtrValue(v)
}

func (e *StructEncoder) StringPtr(column string, v *string) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != StringType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *string: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateStringPtrValue(v)
}

func (e *StructEncoder) BytesPtr(column string, v *[]byte) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != BytesType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *[]byte: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateBytesPtrValue(v)
}

func (e *StructEncoder) BoolPtr(column string, v *bool) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != BoolType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *bool: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateBoolPtrValue(v)
}

func (e *StructEncoder) TimePtr(column string, v *time.Time) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != TimeType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required *time.Time: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	e.value.fields[column] = e.valueFactory.CreateTimePtrValue(v)
}

func (e *StructEncoder) Struct(column string, v Marshaler) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if field.typ != StructType {
		e.err = xerrors.Errorf("%s.%s type is *%s but required struct: %w",
			e.typ.tableName, column, field.typ, ErrInvalidEncodeType)
		return
	}
	enc := NewStructEncoder(field.subtypeStruct, e.valueFactory)
	if err := v.EncodeRapidash(enc); err != nil {
		e.err = xerrors.Errorf("failed to encode value: %w", err)
		return
	}
	if len(enc.value.fields) == 0 {
		e.value.fields[column] = StructValueToValue(nil)
		return
	}
	e.value.fields[column] = StructValueToValue(enc.value)
}

func (e *StructEncoder) Ints(column string, v []int) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateIntValue(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Int8s(column string, v []int8) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateInt8Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Int16s(column string, v []int16) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateInt16Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Int32s(column string, v []int32) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateInt32Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Int64s(column string, v []int64) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateInt64Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Uints(column string, v []uint) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateUintValue(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Uint8s(column string, v []uint8) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateUint8Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Uint16s(column string, v []uint16) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateUint16Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Uint32s(column string, v []uint32) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateUint32Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Uint64s(column string, v []uint64) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateUint64Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Float32s(column string, v []float32) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateFloat32Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Float64s(column string, v []float64) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateFloat64Value(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Strings(column string, v []string) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateStringValue(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Bools(column string, v []bool) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateBoolValue(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Times(column string, v []time.Time) {
	if e.err != nil {
		return
	}
	if _, exists := e.typ.fields[column]; !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	values := []*Value{}
	for _, value := range v {
		values = append(values, e.valueFactory.CreateTimeValue(value))
	}
	e.value.fields[column] = ValuesToValue(values)
}

func (e *StructEncoder) Structs(column string, v Marshaler) {
	if e.err != nil {
		return
	}
	field, exists := e.typ.fields[column]
	if !exists {
		e.err = xerrors.Errorf("%s.%s: %w", e.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	enc := NewStructEncoder(field.subtypeStruct, e.valueFactory)
	if err := v.EncodeRapidash(enc); err != nil {
		e.err = xerrors.Errorf("failed to encode slice of struct: %w", err)
		return
	}
	e.value.fields[column] = StructSliceValueToValue(enc.slice)
}

func (e *StructEncoder) Encode() ([]byte, error) {
	content, err := e.value.encodeValue()
	if err != nil {
		return nil, xerrors.Errorf("failed to encode struct: %w", err)
	}
	return content, nil
}

func (e *StructEncoder) EncodeSlice() ([][]byte, error) {
	contents := [][]byte{}
	for _, value := range e.slice.values {
		content, err := value.encodeValue()
		if err != nil {
			return nil, xerrors.Errorf("failed to encode value: %w", err)
		}
		contents = append(contents, content)
	}
	return contents, nil
}

type StructSliceEncoder struct {
	*StructEncoder
	coder Coder
}

func NewStructSliceEncoder(typ *Struct, valueFactory *ValueFactory, coder Coder) *StructSliceEncoder {
	return &StructSliceEncoder{
		StructEncoder: NewStructEncoder(typ, valueFactory),
		coder:         coder,
	}
}

func (e *StructSliceEncoder) Encode() ([]byte, error) {
	if err := e.coder.EncodeRapidash(e); err != nil {
		return nil, xerrors.Errorf("failed to encode value: %w", err)
	}
	columns := e.typ.Columns()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayHeader(len(e.slice.values)); err != nil {
		return nil, xerrors.Errorf("failed to encode array header: %w", err)
	}
	for _, value := range e.slice.values {
		for _, column := range columns {
			v, exists := value.fields[column]
			if exists {
				if err := v.encode(enc); err != nil {
					return nil, xerrors.Errorf("failed to encode: %w", err)
				}
			} else {
				if err := encodeDefaultValue(e.typ.fields[column].typ, enc); err != nil {
					return nil, xerrors.Errorf("failed to encode default value: %w", err)
				}
			}
		}
	}
	return buf.Bytes(), nil
}
