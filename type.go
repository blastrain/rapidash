package rapidash

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blastrain/msgpack"
	"golang.org/x/xerrors"
)

type Type interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

type TypeID int

func (t TypeID) String() string {
	switch t {
	case IntType:
		return "int"
	case Int8Type:
		return "int8"
	case Int16Type:
		return "int16"
	case Int32Type:
		return "int32"
	case Int64Type:
		return "int64"
	case UintType:
		return "uint"
	case Uint8Type:
		return "uint8"
	case Uint16Type:
		return "uint16"
	case Uint32Type:
		return "uint32"
	case Uint64Type:
		return "uint64"
	case Float32Type:
		return "float32"
	case Float64Type:
		return "float64"
	case BoolType:
		return "bool"
	case StringType:
		return "string"
	case BytesType:
		return "[]byte"
	case TimeType:
		return "time.Time"
	case SliceType:
		return "slice"
	case StructType:
		return "struct"
	}
	return "unknown"
}

const (
	IntType TypeID = iota
	Int8Type
	Int16Type
	Int32Type
	Int64Type
	UintType
	Uint8Type
	Uint16Type
	Uint32Type
	Uint64Type
	Float32Type
	Float64Type
	BoolType
	StringType
	BytesType
	TimeType
	SliceType
	StructType
)

type TypeKind int

func (t TypeKind) String() string {
	switch t {
	case IntKind:
		return "int"
	case FloatKind:
		return "float"
	case StringKind:
		return "string"
	case BytesKind:
		return "[]byte"
	case TimeKind:
		return "time.Time"
	}
	return "unknown"
}

const (
	IntKind TypeKind = iota
	FloatKind
	BoolKind
	StringKind
	BytesKind
	TimeKind
)

var (
	nilValue = NewNilValue()
	nilStr   = "nil"
)

type Struct struct {
	tableName string
	fields    map[string]*StructField
}

type StructField struct {
	typ           TypeID
	kind          TypeKind
	column        string
	index         int
	subtype       TypeID
	subtypeStruct *Struct
}

type ValueFactory struct {
	intValuePool           sync.Pool
	int8ValuePool          sync.Pool
	int16ValuePool         sync.Pool
	int32ValuePool         sync.Pool
	int64ValuePool         sync.Pool
	uintValuePool          sync.Pool
	uint8ValuePool         sync.Pool
	uint16ValuePool        sync.Pool
	uint32ValuePool        sync.Pool
	uint64ValuePool        sync.Pool
	float32ValuePool       sync.Pool
	float64ValuePool       sync.Pool
	boolValuePool          sync.Pool
	stringValuePool        sync.Pool
	bytesValuePool         sync.Pool
	timeValuePool          sync.Pool
	defaultValueCreatorMap map[TypeID]func() *Value
}

func NewValueFactory() *ValueFactory {
	var f *ValueFactory
	f = &ValueFactory{
		intValuePool: sync.Pool{
			New: func() interface{} {
				return NewIntValue(0)
			},
		},
		int8ValuePool: sync.Pool{
			New: func() interface{} {
				return NewInt8Value(0)
			},
		},
		int16ValuePool: sync.Pool{
			New: func() interface{} {
				return NewInt16Value(0)
			},
		},
		int32ValuePool: sync.Pool{
			New: func() interface{} {
				return NewInt32Value(0)
			},
		},
		int64ValuePool: sync.Pool{
			New: func() interface{} {
				return NewInt64Value(0)
			},
		},
		uintValuePool: sync.Pool{
			New: func() interface{} {
				return NewUintValue(0)
			},
		},
		uint8ValuePool: sync.Pool{
			New: func() interface{} {
				return NewUint8Value(0)
			},
		},
		uint16ValuePool: sync.Pool{
			New: func() interface{} {
				return NewUint16Value(0)
			},
		},
		uint32ValuePool: sync.Pool{
			New: func() interface{} {
				return NewUint32Value(0)
			},
		},
		uint64ValuePool: sync.Pool{
			New: func() interface{} {
				return NewUint64Value(0)
			},
		},
		float32ValuePool: sync.Pool{
			New: func() interface{} {
				return NewFloat32Value(0)
			},
		},
		float64ValuePool: sync.Pool{
			New: func() interface{} {
				return NewFloat64Value(0)
			},
		},
		boolValuePool: sync.Pool{
			New: func() interface{} {
				return NewBoolValue(false)
			},
		},
		stringValuePool: sync.Pool{
			New: func() interface{} {
				return NewStringValue("")
			},
		},
		bytesValuePool: sync.Pool{
			New: func() interface{} {
				return NewBytesValue([]byte{})
			},
		},
		timeValuePool: sync.Pool{
			New: func() interface{} {
				return NewTimeValue(time.Now())
			},
		},
		defaultValueCreatorMap: map[TypeID]func() *Value{
			IntType: func() *Value {
				return f.CreateIntValue(0)
			},
			Int8Type: func() *Value {
				return f.CreateInt8Value(0)
			},
			Int16Type: func() *Value {
				return f.CreateInt16Value(0)
			},
			Int32Type: func() *Value {
				return f.CreateInt32Value(0)
			},
			Int64Type: func() *Value {
				return f.CreateInt64Value(0)
			},
			UintType: func() *Value {
				return f.CreateUintValue(0)
			},
			Uint8Type: func() *Value {
				return f.CreateUint8Value(0)
			},
			Uint16Type: func() *Value {
				return f.CreateUint16Value(0)
			},
			Uint32Type: func() *Value {
				return f.CreateUint32Value(0)
			},
			Uint64Type: func() *Value {
				return f.CreateUint64Value(0)
			},
			Float32Type: func() *Value {
				return f.CreateFloat32Value(0)
			},
			Float64Type: func() *Value {
				return f.CreateFloat64Value(0)
			},
			BoolType: func() *Value {
				return f.CreateBoolValue(false)
			},
			StringType: func() *Value {
				return f.CreateStringValue("")
			},
			BytesType: func() *Value {
				return f.CreateBytesValue([]byte{})
			},
			TimeType: func() *Value {
				return f.CreateTimeValue(time.Time{})
			},
		},
	}
	return f
}

func (f *ValueFactory) CreateDefaultValue(typ TypeID) *Value {
	return f.defaultValueCreatorMap[typ]()
}

func (f *ValueFactory) CreateValueFromString(v string, typeID TypeID) (*Value, error) {
	if v == nilStr {
		return nilValue, nil
	}
	switch typeID {
	case IntType:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as int: %w", v, err)
		}
		return f.CreateIntValue(int(i)), nil
	case Int8Type:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as int: %w", v, err)
		}
		return f.CreateInt8Value(int8(i)), nil
	case Int16Type:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as int: %w", v, err)
		}
		return f.CreateInt16Value(int16(i)), nil
	case Int32Type:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as int: %w", v, err)
		}
		return f.CreateInt32Value(int32(i)), nil
	case Int64Type:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as int: %w", v, err)
		}
		return f.CreateInt64Value(i), nil
	case UintType:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as uint: %w", v, err)
		}
		return f.CreateUintValue(uint(u)), nil
	case Uint8Type:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as uint: %w", v, err)
		}
		return f.CreateUint8Value(uint8(u)), nil
	case Uint16Type:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as uint: %w", v, err)
		}
		return f.CreateUint16Value(uint16(u)), nil
	case Uint32Type:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as uint: %w", v, err)
		}
		return f.CreateUint32Value(uint32(u)), nil
	case Uint64Type:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as uint: %w", v, err)
		}
		return f.CreateUint64Value(u), nil
	case Float32Type:
		f32, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as float: %w", v, err)
		}
		return f.CreateFloat32Value(float32(f32)), nil
	case Float64Type:
		f64, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as float: %w", v, err)
		}
		return f.CreateFloat64Value(f64), nil
	case StringType:
		return f.CreateStringValue(v), nil
	case BytesType:
		return f.CreateBytesValue([]byte(v)), nil
	case BoolType:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as bool: %w", v, err)
		}
		return f.CreateBoolValue(b), nil
	case TimeType:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse %s as int: %w", v, err)
		}
		return f.CreateTimeValue(time.Unix(i, 0)), nil
	}
	return nil, ErrUnknownColumnType
}

func (f *ValueFactory) CreateValue(v interface{}) *Value {
	if v == nil {
		return nilValue
	}
	switch v := v.(type) {
	case int:
		return f.CreateIntValue(v)
	case int8:
		return f.CreateInt8Value(v)
	case int16:
		return f.CreateInt16Value(v)
	case int32:
		return f.CreateInt32Value(v)
	case int64:
		return f.CreateInt64Value(v)
	case uint:
		return f.CreateUintValue(v)
	case uint8:
		return f.CreateUint8Value(v)
	case uint16:
		return f.CreateUint16Value(v)
	case uint32:
		return f.CreateUint32Value(v)
	case uint64:
		return f.CreateUint64Value(v)
	case float32:
		return f.CreateFloat32Value(v)
	case float64:
		return f.CreateFloat64Value(v)
	case bool:
		return f.CreateBoolValue(v)
	case string:
		return f.CreateStringValue(v)
	case []byte:
		return f.CreateBytesValue(v)
	case time.Time:
		return f.CreateTimeValue(v)
	case *int:
		return f.CreateIntPtrValue(v)
	case *int8:
		return f.CreateInt8PtrValue(v)
	case *int16:
		return f.CreateInt16PtrValue(v)
	case *int32:
		return f.CreateInt32PtrValue(v)
	case *int64:
		return f.CreateInt64PtrValue(v)
	case *uint:
		return f.CreateUintPtrValue(v)
	case *uint8:
		return f.CreateUint8PtrValue(v)
	case *uint16:
		return f.CreateUint16PtrValue(v)
	case *uint32:
		return f.CreateUint32PtrValue(v)
	case *uint64:
		return f.CreateUint64PtrValue(v)
	case *float32:
		return f.CreateFloat32PtrValue(v)
	case *float64:
		return f.CreateFloat64PtrValue(v)
	case *bool:
		return f.CreateBoolPtrValue(v)
	case *string:
		return f.CreateStringPtrValue(v)
	case *[]byte:
		return f.CreateBytesPtrValue(v)
	case *time.Time:
		return f.CreateTimePtrValue(v)
	default:
	}
	return nil
}

func (f *ValueFactory) CreateUniqueValues(v interface{}) []*Value {
	switch slice := v.(type) {
	case []int:
		uniqueMap := map[int]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateIntValue(v))
		}
		return values
	case []int8:
		uniqueMap := map[int8]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateInt8Value(v))
		}
		return values
	case []int16:
		uniqueMap := map[int16]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateInt16Value(v))
		}
		return values
	case []int32:
		uniqueMap := map[int32]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateInt32Value(v))
		}
		return values
	case []int64:
		uniqueMap := map[int64]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateInt64Value(v))
		}
		return values
	case []uint:
		uniqueMap := map[uint]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateUintValue(v))
		}
		return values
	case []uint8:
		uniqueMap := map[uint8]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateUint8Value(v))
		}
		return values
	case []uint16:
		uniqueMap := map[uint16]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateUint16Value(v))
		}
		return values
	case []uint32:
		uniqueMap := map[uint32]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateUint32Value(v))
		}
		return values
	case []uint64:
		uniqueMap := map[uint64]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateUint64Value(v))
		}
		return values
	case []float32:
		uniqueMap := map[float32]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateFloat32Value(v))
		}
		return values
	case []float64:
		uniqueMap := map[float64]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateFloat64Value(v))
		}
		return values
	case []bool:
		uniqueMap := map[bool]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateBoolValue(v))
		}
		return values
	case []string:
		uniqueMap := map[string]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateStringValue(v))
		}
		return values
	case [][]byte:
		uniqueMap := map[string]struct{}{}
		for _, v := range slice {
			uniqueMap[hex.EncodeToString(v)] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			bytes, _ := hex.DecodeString(v)
			values = append(values, f.CreateBytesValue(bytes))
		}
		return values
	case []time.Time:
		uniqueMap := map[time.Time]struct{}{}
		for _, v := range slice {
			uniqueMap[v] = struct{}{}
		}
		values := make([]*Value, 0, len(uniqueMap))
		for v := range uniqueMap {
			values = append(values, f.CreateTimeValue(v))
		}
		return values
	default:
	}
	return nil
}

func (f *ValueFactory) CreateIntValue(v int) *Value {
	value := f.intValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.intValuePool
	return value
}

func (f *ValueFactory) CreateInt8Value(v int8) *Value {
	value := f.int8ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.int8ValuePool
	return value
}

func (f *ValueFactory) CreateInt16Value(v int16) *Value {
	value := f.int16ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.int16ValuePool
	return value
}

func (f *ValueFactory) CreateInt32Value(v int32) *Value {
	value := f.int32ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.int32ValuePool
	return value
}

func (f *ValueFactory) CreateInt64Value(v int64) *Value {
	value := f.int64ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.int64ValuePool
	return value
}

func (f *ValueFactory) CreateUintValue(v uint) *Value {
	value := f.uintValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.uintValuePool
	return value
}

func (f *ValueFactory) CreateUint8Value(v uint8) *Value {
	value := f.uint8ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.uint8ValuePool
	return value
}

func (f *ValueFactory) CreateUint16Value(v uint16) *Value {
	value := f.uint16ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.uint16ValuePool
	return value
}

func (f *ValueFactory) CreateUint32Value(v uint32) *Value {
	value := f.uint32ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.uint32ValuePool
	return value
}

func (f *ValueFactory) CreateUint64Value(v uint64) *Value {
	value := f.uint64ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.uint64ValuePool
	return value
}

func (f *ValueFactory) CreateFloat32Value(v float32) *Value {
	value := f.float32ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.float32ValuePool
	return value
}

func (f *ValueFactory) CreateFloat64Value(v float64) *Value {
	value := f.float64ValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.float64ValuePool
	return value
}

func (f *ValueFactory) CreateBoolValue(v bool) *Value {
	value := f.boolValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.boolValuePool
	return value
}

func (f *ValueFactory) CreateStringValue(v string) *Value {
	value := f.stringValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.stringValuePool
	return value
}

func (f *ValueFactory) CreateBytesValue(v []byte) *Value {
	value := f.bytesValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.bytesValuePool
	return value
}

func (f *ValueFactory) CreateTimeValue(v time.Time) *Value {
	value := f.timeValuePool.Get().(*Value)
	value.Set(v)
	value.IsNil = false
	value.valuePool = &f.timeValuePool
	return value
}

func (f *ValueFactory) CreateIntPtrValue(v *int) *Value {
	value := f.intValuePool.Get().(*Value)
	if v == nil {
		value.Set(0)
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.intValuePool
	return value
}

func (f *ValueFactory) CreateInt8PtrValue(v *int8) *Value {
	value := f.int8ValuePool.Get().(*Value)
	if v == nil {
		value.Set(int8(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.int8ValuePool
	return value
}

func (f *ValueFactory) CreateInt16PtrValue(v *int16) *Value {
	value := f.int16ValuePool.Get().(*Value)
	if v == nil {
		value.Set(int16(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.int16ValuePool
	return value
}

func (f *ValueFactory) CreateInt32PtrValue(v *int32) *Value {
	value := f.int32ValuePool.Get().(*Value)
	if v == nil {
		value.Set(int32(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.int32ValuePool
	return value
}

func (f *ValueFactory) CreateInt64PtrValue(v *int64) *Value {
	value := f.int64ValuePool.Get().(*Value)
	if v == nil {
		value.Set(int64(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.int64ValuePool
	return value
}

func (f *ValueFactory) CreateUintPtrValue(v *uint) *Value {
	value := f.uintValuePool.Get().(*Value)
	if v == nil {
		value.Set(uint(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.uintValuePool
	return value
}

func (f *ValueFactory) CreateUint8PtrValue(v *uint8) *Value {
	value := f.uint8ValuePool.Get().(*Value)
	if v == nil {
		value.Set(uint8(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.uint8ValuePool
	return value
}

func (f *ValueFactory) CreateUint16PtrValue(v *uint16) *Value {
	value := f.uint16ValuePool.Get().(*Value)
	if v == nil {
		value.Set(uint16(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.uint16ValuePool
	return value
}

func (f *ValueFactory) CreateUint32PtrValue(v *uint32) *Value {
	value := f.uint32ValuePool.Get().(*Value)
	if v == nil {
		value.Set(uint32(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.uint32ValuePool
	return value
}

func (f *ValueFactory) CreateUint64PtrValue(v *uint64) *Value {
	value := f.uint64ValuePool.Get().(*Value)
	if v == nil {
		value.Set(uint64(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.uint64ValuePool
	return value
}

func (f *ValueFactory) CreateFloat32PtrValue(v *float32) *Value {
	value := f.float32ValuePool.Get().(*Value)
	if v == nil {
		value.Set(float32(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.float32ValuePool
	return value
}

func (f *ValueFactory) CreateFloat64PtrValue(v *float64) *Value {
	value := f.float64ValuePool.Get().(*Value)
	if v == nil {
		value.Set(float64(0))
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.float64ValuePool
	return value
}

func (f *ValueFactory) CreateBoolPtrValue(v *bool) *Value {
	value := f.boolValuePool.Get().(*Value)
	if v == nil {
		value.Set(false)
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.boolValuePool
	return value
}

func (f *ValueFactory) CreateStringPtrValue(v *string) *Value {
	value := f.stringValuePool.Get().(*Value)
	if v == nil {
		value.Set("")
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.stringValuePool
	return value
}

func (f *ValueFactory) CreateBytesPtrValue(v *[]byte) *Value {
	value := f.bytesValuePool.Get().(*Value)
	if v == nil {
		value.Set([]byte{})
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.bytesValuePool
	return value
}

func (f *ValueFactory) CreateTimePtrValue(v *time.Time) *Value {
	value := f.timeValuePool.Get().(*Value)
	if v == nil {
		value.Set(time.Time{})
		value.IsNil = true
	} else {
		value.Set(*v)
		value.IsNil = false
	}
	value.valuePool = &f.timeValuePool
	return value
}

type Value struct {
	typ          TypeID
	kind         TypeKind
	intValue     int
	int8Value    int8
	int16Value   int16
	int32Value   int32
	int64Value   int64
	uintValue    uint
	uint8Value   uint8
	uint16Value  uint16
	uint32Value  uint32
	uint64Value  uint64
	float32Value float32
	float64Value float64
	boolValue    bool
	stringValue  string
	bytesValue   []byte
	timeValue    time.Time
	sliceValue   []*Value
	structValue  *StructValue
	IsNil        bool
	Set          func(interface{})
	EQ           func(*Value) bool
	NEQ          func(*Value) bool
	LT           func(*Value) bool
	LTE          func(*Value) bool
	GT           func(*Value) bool
	GTE          func(*Value) bool
	encode       func(*msgpack.Encoder) error
	String       func() string
	Hash         func() uint32
	RawValue     func() interface{}
	scan         func(interface{}) error
	valuePool    *sync.Pool
}

func (v *Value) Scan(src interface{}) error {
	if err := v.scan(src); err != nil {
		return xerrors.Errorf("cannot scan value %v: %w", src, err)
	}
	return nil
}

type Values []*Value

func (v Values) Len() int                 { return len(v) }
func (v Values) At(idx int) Decoder       { return v[idx].structValue }
func (Values) Int(string) int             { return 0 }
func (Values) Int8(string) int8           { return 0 }
func (Values) Int16(string) int16         { return 0 }
func (Values) Int32(string) int32         { return 0 }
func (Values) Int64(string) int64         { return 0 }
func (Values) Uint(string) uint           { return 0 }
func (Values) Uint8(string) uint8         { return 0 }
func (Values) Uint16(string) uint16       { return 0 }
func (Values) Uint32(string) uint32       { return 0 }
func (Values) Uint64(string) uint64       { return 0 }
func (Values) Float32(string) float32     { return 0 }
func (Values) Float64(string) float64     { return 0 }
func (Values) Bool(string) bool           { return false }
func (Values) String(string) string       { return "" }
func (Values) Bytes(string) []byte        { return []byte{} }
func (Values) Time(string) time.Time      { return time.Time{} }
func (Values) Slice(string, Unmarshaler)  {}
func (Values) Struct(string, Unmarshaler) {}
func (Values) IntPtr(string) *int         { return nil }
func (Values) Int8Ptr(string) *int8       { return nil }
func (Values) Int16Ptr(string) *int16     { return nil }
func (Values) Int32Ptr(string) *int32     { return nil }
func (Values) Int64Ptr(string) *int64     { return nil }
func (Values) UintPtr(string) *uint       { return nil }
func (Values) Uint8Ptr(string) *uint8     { return nil }
func (Values) Uint16Ptr(string) *uint16   { return nil }
func (Values) Uint32Ptr(string) *uint32   { return nil }
func (Values) Uint64Ptr(string) *uint64   { return nil }
func (Values) Float32Ptr(string) *float32 { return nil }
func (Values) Float64Ptr(string) *float64 { return nil }
func (Values) BoolPtr(string) *bool       { return nil }
func (Values) StringPtr(string) *string   { return nil }
func (Values) BytesPtr(string) *[]byte    { return nil }
func (Values) TimePtr(string) *time.Time  { return nil }
func (Values) Ints(string) []int          { return nil }
func (Values) Int8s(string) []int8        { return nil }
func (Values) Int16s(string) []int16      { return nil }
func (Values) Int32s(string) []int32      { return nil }
func (Values) Int64s(string) []int64      { return nil }
func (Values) Uints(string) []uint        { return nil }
func (Values) Uint8s(string) []uint8      { return nil }
func (Values) Uint16s(string) []uint16    { return nil }
func (Values) Uint32s(string) []uint32    { return nil }
func (Values) Uint64s(string) []uint64    { return nil }
func (Values) Float32s(string) []float32  { return nil }
func (Values) Float64s(string) []float64  { return nil }
func (Values) Bools(string) []bool        { return nil }
func (Values) Strings(string) []string    { return nil }
func (Values) Times(string) []time.Time   { return nil }
func (Values) Error() error               { return nil }

func (v *Value) Release() {
	if v.valuePool != nil {
		v.valuePool.Put(v)
	}
}

func NewNilValue() *Value {
	return &Value{
		IsNil: true,
		Set:   func(value interface{}) {},
		EQ: func(value *Value) bool {
			return value.IsNil
		},
		NEQ: func(value *Value) bool {
			return !value.IsNil
		},
		LT: func(value *Value) bool {
			return false
		},
		LTE: func(value *Value) bool {
			return false
		},
		GT: func(value *Value) bool {
			return false
		},
		GTE: func(value *Value) bool {
			return false
		},
		encode: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeNil(); err != nil {
				return xerrors.Errorf("failed to encode nil: %w", err)
			}
			return nil
		},
		String: func() string {
			return nilStr
		},
		Hash: func() uint32 {
			return 0
		},
		RawValue: func() interface{} {
			return nil
		},
		scan: func(src interface{}) error {
			return ErrScanToNilValue
		},
	}
}

func NewIntValue(v int) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:      IntType,
		kind:     IntKind,
		intValue: v,
		Set: func(value interface{}) {
			rvalue.intValue = value.(int)
		},
		EQ: func(value *Value) bool {
			return rvalue.intValue == value.intValue
		},
		NEQ: func(value *Value) bool {
			return rvalue.intValue != value.intValue
		},
		LT: func(value *Value) bool {
			return rvalue.intValue < value.intValue
		},
		LTE: func(value *Value) bool {
			return rvalue.intValue <= value.intValue
		},
		GT: func(value *Value) bool {
			return rvalue.intValue > value.intValue
		},
		GTE: func(value *Value) bool {
			return rvalue.intValue >= value.intValue
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeInt(rvalue.intValue); err != nil {
				return xerrors.Errorf("failed to encode int: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.intValue)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.intValue)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.intValue
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.intValue = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.intValue = v
			case int8:
				rvalue.intValue = int(v)
			case int16:
				rvalue.intValue = int(v)
			case int32:
				rvalue.intValue = int(v)
			case int64:
				rvalue.intValue = int(v)
			case uint:
				rvalue.intValue = int(v)
			case uint8:
				rvalue.intValue = int(v)
			case uint16:
				rvalue.intValue = int(v)
			case uint32:
				rvalue.intValue = int(v)
			case uint64:
				rvalue.intValue = int(v)
			case []byte:
				i, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", string(v), err)
				}
				rvalue.intValue = int(i)
			case string:
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", v, err)
				}
				rvalue.intValue = int(i)
			}
			return nil
		},
	}
	return rvalue
}

func NewInt8Value(v int8) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:       Int8Type,
		kind:      IntKind,
		int8Value: v,
		Set: func(value interface{}) {
			rvalue.int8Value = value.(int8)
		},
		EQ: func(value *Value) bool {
			return rvalue.int8Value == value.int8Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.int8Value != value.int8Value
		},
		LT: func(value *Value) bool {
			return rvalue.int8Value < value.int8Value
		},
		LTE: func(value *Value) bool {
			return rvalue.int8Value <= value.int8Value
		},
		GT: func(value *Value) bool {
			return rvalue.int8Value > value.int8Value
		},
		GTE: func(value *Value) bool {
			return rvalue.int8Value >= value.int8Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeInt8(rvalue.int8Value); err != nil {
				return xerrors.Errorf("failed to encode int8: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.int8Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.int8Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.int8Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.int8Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.int8Value = int8(v)
			case int8:
				rvalue.int8Value = v
			case int16:
				rvalue.int8Value = int8(v)
			case int32:
				rvalue.int8Value = int8(v)
			case int64:
				rvalue.int8Value = int8(v)
			case uint:
				rvalue.int8Value = int8(v)
			case uint8:
				rvalue.int8Value = int8(v)
			case uint16:
				rvalue.int8Value = int8(v)
			case uint32:
				rvalue.int8Value = int8(v)
			case uint64:
				rvalue.int8Value = int8(v)
			case []byte:
				i, err := strconv.ParseInt(string(v), 10, 8)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", string(v), err)
				}
				rvalue.int8Value = int8(i)
			case string:
				i, err := strconv.ParseInt(v, 10, 8)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", v, err)
				}
				rvalue.int8Value = int8(i)
			}
			return nil
		},
	}
	return rvalue
}

func NewInt16Value(v int16) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:        Int16Type,
		kind:       IntKind,
		int16Value: v,
		Set: func(value interface{}) {
			rvalue.int16Value = value.(int16)
		},
		EQ: func(value *Value) bool {
			return rvalue.int16Value == value.int16Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.int16Value != value.int16Value
		},
		LT: func(value *Value) bool {
			return rvalue.int16Value < value.int16Value
		},
		LTE: func(value *Value) bool {
			return rvalue.int16Value <= value.int16Value
		},
		GT: func(value *Value) bool {
			return rvalue.int16Value > value.int16Value
		},
		GTE: func(value *Value) bool {
			return rvalue.int16Value >= value.int16Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeInt16(rvalue.int16Value); err != nil {
				return xerrors.Errorf("failed to encode int16: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.int16Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.int16Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.int16Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.int16Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.int16Value = int16(v)
			case int8:
				rvalue.int16Value = int16(v)
			case int16:
				rvalue.int16Value = v
			case int32:
				rvalue.int16Value = int16(v)
			case int64:
				rvalue.int16Value = int16(v)
			case uint:
				rvalue.int16Value = int16(v)
			case uint8:
				rvalue.int16Value = int16(v)
			case uint16:
				rvalue.int16Value = int16(v)
			case uint32:
				rvalue.int16Value = int16(v)
			case uint64:
				rvalue.int16Value = int16(v)
			case []byte:
				i, err := strconv.ParseInt(string(v), 10, 16)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", string(v), err)
				}
				rvalue.int16Value = int16(i)
			case string:
				i, err := strconv.ParseInt(v, 10, 16)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", v, err)
				}
				rvalue.int16Value = int16(i)
			}
			return nil
		},
	}
	return rvalue
}

func NewInt32Value(v int32) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:        Int32Type,
		kind:       IntKind,
		int32Value: v,
		Set: func(value interface{}) {
			rvalue.int32Value = value.(int32)
		},
		EQ: func(value *Value) bool {
			return rvalue.int32Value == value.int32Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.int32Value != value.int32Value
		},
		LT: func(value *Value) bool {
			return rvalue.int32Value < value.int32Value
		},
		LTE: func(value *Value) bool {
			return rvalue.int32Value <= value.int32Value
		},
		GT: func(value *Value) bool {
			return rvalue.int32Value > value.int32Value
		},
		GTE: func(value *Value) bool {
			return rvalue.int32Value >= value.int32Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeInt32(rvalue.int32Value); err != nil {
				return xerrors.Errorf("failed to encode int32: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.int32Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.int32Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.int32Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.int32Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.int32Value = int32(v)
			case int8:
				rvalue.int32Value = int32(v)
			case int16:
				rvalue.int32Value = int32(v)
			case int32:
				rvalue.int32Value = v
			case int64:
				rvalue.int32Value = int32(v)
			case uint:
				rvalue.int32Value = int32(v)
			case uint8:
				rvalue.int32Value = int32(v)
			case uint16:
				rvalue.int32Value = int32(v)
			case uint32:
				rvalue.int32Value = int32(v)
			case uint64:
				rvalue.int32Value = int32(v)
			case []byte:
				i, err := strconv.ParseInt(string(v), 10, 32)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", string(v), err)
				}
				rvalue.int32Value = int32(i)
			case string:
				i, err := strconv.ParseInt(v, 10, 32)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", v, err)
				}
				rvalue.int32Value = int32(i)
			}
			return nil
		},
	}
	return rvalue
}

func NewInt64Value(v int64) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:        Int64Type,
		kind:       IntKind,
		int64Value: v,
		Set: func(value interface{}) {
			rvalue.int64Value = value.(int64)
		},
		EQ: func(value *Value) bool {
			return rvalue.int64Value == value.int64Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.int64Value != value.int64Value
		},
		LT: func(value *Value) bool {
			return rvalue.int64Value < value.int64Value
		},
		LTE: func(value *Value) bool {
			return rvalue.int64Value <= value.int64Value
		},
		GT: func(value *Value) bool {
			return rvalue.int64Value > value.int64Value
		},
		GTE: func(value *Value) bool {
			return rvalue.int64Value >= value.int64Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeInt64(rvalue.int64Value); err != nil {
				return xerrors.Errorf("failed to encode int64: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.int64Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.int64Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.int64Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.int64Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.int64Value = int64(v)
			case int8:
				rvalue.int64Value = int64(v)
			case int16:
				rvalue.int64Value = int64(v)
			case int32:
				rvalue.int64Value = int64(v)
			case int64:
				rvalue.int64Value = v
			case uint:
				rvalue.int64Value = int64(v)
			case uint8:
				rvalue.int64Value = int64(v)
			case uint16:
				rvalue.int64Value = int64(v)
			case uint32:
				rvalue.int64Value = int64(v)
			case uint64:
				rvalue.int64Value = int64(v)
			case []byte:
				i, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", string(v), err)
				}
				rvalue.int64Value = i
			case string:
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as int: %w", v, err)
				}
				rvalue.int64Value = i
			}
			return nil
		},
	}
	return rvalue
}

func NewUintValue(v uint) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:       UintType,
		kind:      IntKind,
		uintValue: v,
		Set: func(value interface{}) {
			rvalue.uintValue = value.(uint)
		},
		EQ: func(value *Value) bool {
			return rvalue.uintValue == value.uintValue
		},
		NEQ: func(value *Value) bool {
			return rvalue.uintValue != value.uintValue
		},
		LT: func(value *Value) bool {
			return rvalue.uintValue < value.uintValue
		},
		LTE: func(value *Value) bool {
			return rvalue.uintValue <= value.uintValue
		},
		GT: func(value *Value) bool {
			return rvalue.uintValue > value.uintValue
		},
		GTE: func(value *Value) bool {
			return rvalue.uintValue >= value.uintValue
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeUint(rvalue.uintValue); err != nil {
				return xerrors.Errorf("failed to encode uint: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.uintValue)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.uintValue)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.uintValue
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.uintValue = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.uintValue = uint(v)
			case int8:
				rvalue.uintValue = uint(v)
			case int16:
				rvalue.uintValue = uint(v)
			case int32:
				rvalue.uintValue = uint(v)
			case int64:
				rvalue.uintValue = uint(v)
			case uint:
				rvalue.uintValue = v
			case uint8:
				rvalue.uintValue = uint(v)
			case uint16:
				rvalue.uintValue = uint(v)
			case uint32:
				rvalue.uintValue = uint(v)
			case uint64:
				rvalue.uintValue = uint(v)
			case []byte:
				u, err := strconv.ParseUint(string(v), 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", string(v), err)
				}
				rvalue.uintValue = uint(u)
			}
			return nil
		},
	}
	return rvalue
}

func NewUint8Value(v uint8) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:        Uint8Type,
		kind:       IntKind,
		uint8Value: v,
		Set: func(value interface{}) {
			rvalue.uint8Value = value.(uint8)
		},
		EQ: func(value *Value) bool {
			return rvalue.uint8Value == value.uint8Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.uint8Value != value.uint8Value
		},
		LT: func(value *Value) bool {
			return rvalue.uint8Value < value.uint8Value
		},
		LTE: func(value *Value) bool {
			return rvalue.uint8Value <= value.uint8Value
		},
		GT: func(value *Value) bool {
			return rvalue.uint8Value > value.uint8Value
		},
		GTE: func(value *Value) bool {
			return rvalue.uint8Value >= value.uint8Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeUint8(rvalue.uint8Value); err != nil {
				return xerrors.Errorf("failed to encode uint8: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.uint8Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.uint8Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.uint8Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.uint8Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.uint8Value = uint8(v)
			case int8:
				rvalue.uint8Value = uint8(v)
			case int16:
				rvalue.uint8Value = uint8(v)
			case int32:
				rvalue.uint8Value = uint8(v)
			case int64:
				rvalue.uint8Value = uint8(v)
			case uint:
				rvalue.uint8Value = uint8(v)
			case uint8:
				rvalue.uint8Value = v
			case uint16:
				rvalue.uint8Value = uint8(v)
			case uint32:
				rvalue.uint8Value = uint8(v)
			case uint64:
				rvalue.uint8Value = uint8(v)
			case []byte:
				u, err := strconv.ParseUint(string(v), 10, 8)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", string(v), err)
				}
				rvalue.uint8Value = uint8(u)
			case string:
				u, err := strconv.ParseUint(v, 10, 8)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", v, err)
				}
				rvalue.uint8Value = uint8(u)
			}
			return nil
		},
	}
	return rvalue
}

func NewUint16Value(v uint16) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:         Uint16Type,
		kind:        IntKind,
		uint16Value: v,
		Set: func(value interface{}) {
			rvalue.uint16Value = value.(uint16)
		},
		EQ: func(value *Value) bool {
			return rvalue.uint16Value == value.uint16Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.uint16Value != value.uint16Value
		},
		LT: func(value *Value) bool {
			return rvalue.uint16Value < value.uint16Value
		},
		LTE: func(value *Value) bool {
			return rvalue.uint16Value <= value.uint16Value
		},
		GT: func(value *Value) bool {
			return rvalue.uint16Value > value.uint16Value
		},
		GTE: func(value *Value) bool {
			return rvalue.uint16Value >= value.uint16Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeUint16(rvalue.uint16Value); err != nil {
				return xerrors.Errorf("failed to encode uint16: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.uint16Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.uint16Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.uint16Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.uint16Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.uint16Value = uint16(v)
			case int8:
				rvalue.uint16Value = uint16(v)
			case int16:
				rvalue.uint16Value = uint16(v)
			case int32:
				rvalue.uint16Value = uint16(v)
			case int64:
				rvalue.uint16Value = uint16(v)
			case uint:
				rvalue.uint16Value = uint16(v)
			case uint8:
				rvalue.uint16Value = uint16(v)
			case uint16:
				rvalue.uint16Value = v
			case uint32:
				rvalue.uint16Value = uint16(v)
			case uint64:
				rvalue.uint16Value = uint16(v)
			case []byte:
				u, err := strconv.ParseUint(string(v), 10, 16)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", string(v), err)
				}
				rvalue.uint16Value = uint16(u)
			case string:
				u, err := strconv.ParseUint(v, 10, 16)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", v, err)
				}
				rvalue.uint16Value = uint16(u)
			}
			return nil
		},
	}
	return rvalue
}

func NewUint32Value(v uint32) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:         Uint32Type,
		kind:        IntKind,
		uint32Value: v,
		Set: func(value interface{}) {
			rvalue.uint32Value = value.(uint32)
		},
		EQ: func(value *Value) bool {
			return rvalue.uint32Value == value.uint32Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.uint32Value != value.uint32Value
		},
		LT: func(value *Value) bool {
			return rvalue.uint32Value < value.uint32Value
		},
		LTE: func(value *Value) bool {
			return rvalue.uint32Value <= value.uint32Value
		},
		GT: func(value *Value) bool {
			return rvalue.uint32Value > value.uint32Value
		},
		GTE: func(value *Value) bool {
			return rvalue.uint32Value >= value.uint32Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeUint32(rvalue.uint32Value); err != nil {
				return xerrors.Errorf("failed to encode uint32: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.uint32Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.uint32Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.uint32Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.uint32Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.uint32Value = uint32(v)
			case int8:
				rvalue.uint32Value = uint32(v)
			case int16:
				rvalue.uint32Value = uint32(v)
			case int32:
				rvalue.uint32Value = uint32(v)
			case int64:
				rvalue.uint32Value = uint32(v)
			case uint:
				rvalue.uint32Value = uint32(v)
			case uint8:
				rvalue.uint32Value = uint32(v)
			case uint16:
				rvalue.uint32Value = uint32(v)
			case uint32:
				rvalue.uint32Value = v
			case uint64:
				rvalue.uint32Value = uint32(v)
			case []byte:
				u, err := strconv.ParseUint(string(v), 10, 32)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", string(v), err)
				}
				rvalue.uint32Value = uint32(u)
			case string:
				u, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", v, err)
				}
				rvalue.uint32Value = uint32(u)
			}
			return nil
		},
	}
	return rvalue
}

func NewUint64Value(v uint64) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:         Uint64Type,
		kind:        IntKind,
		uint64Value: v,
		Set: func(value interface{}) {
			rvalue.uint64Value = value.(uint64)
		},
		EQ: func(value *Value) bool {
			return rvalue.uint64Value == value.uint64Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.uint64Value != value.uint64Value
		},
		LT: func(value *Value) bool {
			return rvalue.uint64Value < value.uint64Value
		},
		LTE: func(value *Value) bool {
			return rvalue.uint64Value <= value.uint64Value
		},
		GT: func(value *Value) bool {
			return rvalue.uint64Value > value.uint64Value
		},
		GTE: func(value *Value) bool {
			return rvalue.uint64Value >= value.uint64Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeUint64(rvalue.uint64Value); err != nil {
				return xerrors.Errorf("failed to encode uint64: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.uint64Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.uint64Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.uint64Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.uint64Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.uint64Value = uint64(v)
			case int8:
				rvalue.uint64Value = uint64(v)
			case int16:
				rvalue.uint64Value = uint64(v)
			case int32:
				rvalue.uint64Value = uint64(v)
			case int64:
				rvalue.uint64Value = uint64(v)
			case uint:
				rvalue.uint64Value = uint64(v)
			case uint8:
				rvalue.uint64Value = uint64(v)
			case uint16:
				rvalue.uint64Value = uint64(v)
			case uint32:
				rvalue.uint64Value = uint64(v)
			case uint64:
				rvalue.uint64Value = v
			case []byte:
				u, err := strconv.ParseUint(string(v), 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", string(v), err)
				}
				rvalue.uint64Value = u
			case string:
				u, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as uint: %w", v, err)
				}
				rvalue.uint64Value = u
			}
			return nil
		},
	}
	return rvalue
}

func NewFloat32Value(v float32) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:          Float32Type,
		kind:         FloatKind,
		float32Value: v,
		Set: func(value interface{}) {
			rvalue.float32Value = value.(float32)
		},
		EQ: func(value *Value) bool {
			return rvalue.float32Value == value.float32Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.float32Value != value.float32Value
		},
		LT: func(value *Value) bool {
			return rvalue.float32Value < value.float32Value
		},
		LTE: func(value *Value) bool {
			return rvalue.float32Value <= value.float32Value
		},
		GT: func(value *Value) bool {
			return rvalue.float32Value > value.float32Value
		},
		GTE: func(value *Value) bool {
			return rvalue.float32Value >= value.float32Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeFloat32(rvalue.float32Value); err != nil {
				return xerrors.Errorf("failed to encode float32: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.float32Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.float32Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.float32Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.float32Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case float32:
				rvalue.float32Value = v
			case float64:
				rvalue.float32Value = float32(v)
			case []byte:
				f, err := strconv.ParseFloat(string(v), 32)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as float: %w", string(v), err)
				}
				rvalue.float32Value = float32(f)
			}
			return nil
		},
	}
	return rvalue
}

func NewFloat64Value(v float64) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:          Float64Type,
		kind:         FloatKind,
		float64Value: v,
		Set: func(value interface{}) {
			rvalue.float64Value = value.(float64)
		},
		EQ: func(value *Value) bool {
			return rvalue.float64Value == value.float64Value
		},
		NEQ: func(value *Value) bool {
			return rvalue.float64Value != value.float64Value
		},
		LT: func(value *Value) bool {
			return rvalue.float64Value < value.float64Value
		},
		LTE: func(value *Value) bool {
			return rvalue.float64Value <= value.float64Value
		},
		GT: func(value *Value) bool {
			return rvalue.float64Value > value.float64Value
		},
		GTE: func(value *Value) bool {
			return rvalue.float64Value >= value.float64Value
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeFloat64(rvalue.float64Value); err != nil {
				return xerrors.Errorf("failed to encode float64: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.float64Value)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.float64Value)))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.float64Value
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.float64Value = 0
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case float32:
				rvalue.float64Value = float64(v)
			case float64:
				rvalue.float64Value = v
			case []byte:
				f, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					return xerrors.Errorf("failed to parse %s as float: %w", string(v), err)
				}
				rvalue.float64Value = f
			}
			return nil
		},
	}
	return rvalue
}

func NewBoolValue(v bool) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:       BoolType,
		kind:      BoolKind,
		boolValue: v,
		Set: func(value interface{}) {
			rvalue.boolValue = value.(bool)
		},
		EQ: func(value *Value) bool {
			return rvalue.boolValue == value.boolValue
		},
		NEQ: func(value *Value) bool {
			return rvalue.boolValue != value.boolValue
		},
		LT: func(value *Value) bool {
			return false
		},
		LTE: func(value *Value) bool {
			return false
		},
		GT: func(value *Value) bool {
			return false
		},
		GTE: func(value *Value) bool {
			return false
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeBool(rvalue.boolValue); err != nil {
				return xerrors.Errorf("failed to encode bool: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.boolValue)
		},
		Hash: func() uint32 {
			if rvalue.boolValue {
				return 1
			}
			return 0
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.boolValue
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.boolValue = false
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int:
				rvalue.boolValue = v == 1
			case int8:
				rvalue.boolValue = v == 1
			case int16:
				rvalue.boolValue = v == 1
			case int32:
				rvalue.boolValue = v == 1
			case int64:
				rvalue.boolValue = v == 1
			case uint:
				rvalue.boolValue = v == 1
			case uint8:
				rvalue.boolValue = v == 1
			case uint16:
				rvalue.boolValue = v == 1
			case uint32:
				rvalue.boolValue = v == 1
			case uint64:
				rvalue.boolValue = v == 1
			case bool:
				rvalue.boolValue = v
			case []byte:
				// string(v[0]) is "1", but v[0] is 49
				rvalue.boolValue = v[0] == 49
			}
			return nil
		},
	}
	return rvalue
}

func NewStringValue(v string) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:         StringType,
		kind:        StringKind,
		stringValue: v,
		Set: func(value interface{}) {
			rvalue.stringValue = value.(string)
		},
		EQ: func(value *Value) bool {
			return rvalue.stringValue == value.stringValue
		},
		NEQ: func(value *Value) bool {
			return rvalue.stringValue != value.stringValue
		},
		LT: func(value *Value) bool {
			return rvalue.stringValue < value.stringValue
		},
		LTE: func(value *Value) bool {
			return rvalue.stringValue <= value.stringValue
		},
		GT: func(value *Value) bool {
			return rvalue.stringValue > value.stringValue
		},
		GTE: func(value *Value) bool {
			return rvalue.stringValue >= value.stringValue
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeString(rvalue.stringValue); err != nil {
				return xerrors.Errorf("failed to encode string: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return strconv.Quote(rvalue.stringValue)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(rvalue.stringValue))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.stringValue
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.stringValue = ""
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int, int8, int16, int32, int64,
				uint, uint8, uint16, uint32, uint64,
				float32, float64, bool:
				rvalue.stringValue = fmt.Sprint(v)
			case []byte:
				rvalue.stringValue = string(v)
			case string:
				rvalue.stringValue = v
			}
			return nil
		},
	}
	return rvalue
}

func NewBytesValue(v []byte) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:        BytesType,
		kind:       BytesKind,
		bytesValue: v,
		Set: func(value interface{}) {
			rvalue.bytesValue = value.([]byte)
		},
		EQ: func(value *Value) bool {
			return string(rvalue.bytesValue) == string(value.bytesValue)
		},
		NEQ: func(value *Value) bool {
			return string(rvalue.bytesValue) != string(value.bytesValue)
		},
		LT: func(value *Value) bool {
			return string(rvalue.bytesValue) < string(value.bytesValue)
		},
		LTE: func(value *Value) bool {
			return string(rvalue.bytesValue) <= string(value.bytesValue)
		},
		GT: func(value *Value) bool {
			return string(rvalue.bytesValue) > string(value.bytesValue)
		},
		GTE: func(value *Value) bool {
			return string(rvalue.bytesValue) >= string(value.bytesValue)
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeBytes(rvalue.bytesValue); err != nil {
				return xerrors.Errorf("failed to encode []byte: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return string(rvalue.bytesValue)
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE(rvalue.bytesValue)
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.bytesValue
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.bytesValue = []byte{}
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			switch v := src.(type) {
			case int, int8, int16, int32, int64,
				uint, uint8, uint16, uint32, uint64,
				float32, float64, bool:
				rvalue.bytesValue = []byte(fmt.Sprint(v))
			case []byte:
				rvalue.bytesValue = make([]byte, len(v))
				copy(rvalue.bytesValue, v)
			case string:
				rvalue.bytesValue = []byte(v)
			}
			return nil
		},
	}
	return rvalue
}

func NewTimeValue(v time.Time) *Value {
	var rvalue *Value
	rvalue = &Value{
		typ:       TimeType,
		kind:      TimeKind,
		timeValue: v,
		Set: func(value interface{}) {
			rvalue.timeValue = value.(time.Time)
		},
		EQ: func(value *Value) bool {
			return rvalue.timeValue.Equal(value.timeValue)
		},
		NEQ: func(value *Value) bool {
			return !rvalue.timeValue.Equal(value.timeValue)
		},
		LT: func(value *Value) bool {
			return rvalue.timeValue.Before(value.timeValue)
		},
		LTE: func(value *Value) bool {
			return !rvalue.timeValue.After(value.timeValue)
		},
		GT: func(value *Value) bool {
			return rvalue.timeValue.After(value.timeValue)
		},
		GTE: func(value *Value) bool {
			return !rvalue.timeValue.Before(value.timeValue)
		},
		encode: func(enc *msgpack.Encoder) error {
			if rvalue.IsNil {
				if err := enc.EncodeNil(); err != nil {
					return xerrors.Errorf("failed to encode nil: %w", err)
				}
				return nil
			}
			if err := enc.EncodeTime(rvalue.timeValue); err != nil {
				return xerrors.Errorf("failed to encode time.Time: %w", err)
			}
			return nil
		},
		String: func() string {
			if rvalue.IsNil {
				return nilStr
			}
			return fmt.Sprint(rvalue.timeValue.Unix())
		},
		Hash: func() uint32 {
			return crc32.ChecksumIEEE([]byte(fmt.Sprint(rvalue.timeValue.Unix())))
		},
		RawValue: func() interface{} {
			if rvalue.IsNil {
				return nil
			}
			return rvalue.timeValue
		},
		scan: func(src interface{}) error {
			if src == nil {
				rvalue.timeValue = time.Time{}
				rvalue.IsNil = true
				return nil
			}
			rvalue.IsNil = false
			timeValue, ok := src.(time.Time)
			if ok {
				rvalue.timeValue = timeValue
			}
			return nil
		},
	}
	return rvalue
}

func ValuesToValue(v []*Value) *Value {
	var rvalue *Value
	rvalue = &Value{
		sliceValue: v,
		encode: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeArrayHeader(len(rvalue.sliceValue)); err != nil {
				return xerrors.Errorf("failed to encode array header: %w", err)
			}
			for _, v := range rvalue.sliceValue {
				if err := v.encode(enc); err != nil {
					return xerrors.Errorf("failed to encode value: %w", err)
				}
			}
			return nil
		},
	}
	return rvalue
}

func StructSliceValueToValue(v *StructSliceValue) *Value {
	values := []*Value{}
	for _, value := range v.values {
		values = append(values, &Value{
			structValue: value,
		})
	}
	var rvalue *Value
	rvalue = &Value{
		sliceValue: values,
		encode: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeArrayHeader(len(rvalue.sliceValue)); err != nil {
				return xerrors.Errorf("failed to encode array header: %w", err)
			}
			for _, v := range rvalue.sliceValue {
				if err := v.structValue.encode(enc); err != nil {
					return xerrors.Errorf("failed to encode value: %w", err)
				}
			}
			return nil
		},
	}
	return rvalue
}

func StructValueToValue(v *StructValue) *Value {
	var rvalue *Value
	rvalue = &Value{
		structValue: v,
		encode: func(enc *msgpack.Encoder) error {
			if err := rvalue.structValue.encode(enc); err != nil {
				return xerrors.Errorf("failed to encode struct: %w", err)
			}
			return nil
		},
	}
	return rvalue
}

type StructSliceValue struct {
	values []*StructValue
}

func NewStructSliceValue() *StructSliceValue {
	return &StructSliceValue{
		values: []*StructValue{},
	}
}

func (v *StructSliceValue) Release() {
	for _, value := range v.values {
		value.Release()
	}
}

func (v *StructSliceValue) EncodeLog() string {
	if v == nil {
		return "[]"
	}
	log := "["
	valueLogs := []string{}
	for _, value := range v.values {
		valueLogs = append(valueLogs, value.EncodeLog())
	}
	log += strings.Join(valueLogs, ",")
	return log + "]"
}

func (v *StructSliceValue) At(idx int) Decoder {
	return v.values[idx]
}

func (v *StructSliceValue) Len() int {
	return len(v.values)
}

func (v *StructSliceValue) Int(column string) int {
	return v.At(0).Int(column)
}

func (v *StructSliceValue) Int8(column string) int8 {
	return v.At(0).Int8(column)
}

func (v *StructSliceValue) Int16(column string) int16 {
	return v.At(0).Int16(column)
}

func (v *StructSliceValue) Int32(column string) int32 {
	return v.At(0).Int32(column)
}

func (v *StructSliceValue) Int64(column string) int64 {
	return v.At(0).Int64(column)
}

func (v *StructSliceValue) Uint(column string) uint {
	return v.At(0).Uint(column)
}

func (v *StructSliceValue) Uint8(column string) uint8 {
	return v.At(0).Uint8(column)
}

func (v *StructSliceValue) Uint16(column string) uint16 {
	return v.At(0).Uint16(column)
}

func (v *StructSliceValue) Uint32(column string) uint32 {
	return v.At(0).Uint32(column)
}

func (v *StructSliceValue) Uint64(column string) uint64 {
	return v.At(0).Uint64(column)
}

func (v *StructSliceValue) Float32(column string) float32 {
	return v.At(0).Float32(column)
}

func (v *StructSliceValue) Float64(column string) float64 {
	return v.At(0).Float64(column)
}

func (v *StructSliceValue) Bool(column string) bool {
	return v.At(0).Bool(column)
}

func (v *StructSliceValue) String(column string) string {
	return v.At(0).String(column)
}

func (v *StructSliceValue) Bytes(column string) []byte {
	return v.At(0).Bytes(column)
}

func (v *StructSliceValue) Time(column string) time.Time {
	return v.At(0).Time(column)
}

func (v *StructSliceValue) Slice(column string, unmarshaler Unmarshaler) {
	v.At(0).Slice(column, unmarshaler)
}

func (v *StructSliceValue) Struct(column string, unmarshaler Unmarshaler) {
	v.At(0).Struct(column, unmarshaler)
}

func (v *StructSliceValue) IntPtr(column string) *int {
	return v.At(0).IntPtr(column)
}

func (v *StructSliceValue) Int8Ptr(column string) *int8 {
	return v.At(0).Int8Ptr(column)
}

func (v *StructSliceValue) Int16Ptr(column string) *int16 {
	return v.At(0).Int16Ptr(column)
}

func (v *StructSliceValue) Int32Ptr(column string) *int32 {
	return v.At(0).Int32Ptr(column)
}

func (v *StructSliceValue) Int64Ptr(column string) *int64 {
	return v.At(0).Int64Ptr(column)
}

func (v *StructSliceValue) UintPtr(column string) *uint {
	return v.At(0).UintPtr(column)
}

func (v *StructSliceValue) Uint8Ptr(column string) *uint8 {
	return v.At(0).Uint8Ptr(column)
}

func (v *StructSliceValue) Uint16Ptr(column string) *uint16 {
	return v.At(0).Uint16Ptr(column)
}

func (v *StructSliceValue) Uint32Ptr(column string) *uint32 {
	return v.At(0).Uint32Ptr(column)
}

func (v *StructSliceValue) Uint64Ptr(column string) *uint64 {
	return v.At(0).Uint64Ptr(column)
}

func (v *StructSliceValue) Float32Ptr(column string) *float32 {
	return v.At(0).Float32Ptr(column)
}

func (v *StructSliceValue) Float64Ptr(column string) *float64 {
	return v.At(0).Float64Ptr(column)
}

func (v *StructSliceValue) BoolPtr(column string) *bool {
	return v.At(0).BoolPtr(column)
}

func (v *StructSliceValue) StringPtr(column string) *string {
	return v.At(0).StringPtr(column)
}

func (v *StructSliceValue) BytesPtr(column string) *[]byte {
	return v.At(0).BytesPtr(column)
}

func (v *StructSliceValue) TimePtr(column string) *time.Time {
	return v.At(0).TimePtr(column)
}

func (v *StructSliceValue) Ints(column string) []int {
	return v.At(0).Ints(column)
}

func (v *StructSliceValue) Int8s(column string) []int8 {
	return v.At(0).Int8s(column)
}

func (v *StructSliceValue) Int16s(column string) []int16 {
	return v.At(0).Int16s(column)
}

func (v *StructSliceValue) Int32s(column string) []int32 {
	return v.At(0).Int32s(column)
}

func (v *StructSliceValue) Int64s(column string) []int64 {
	return v.At(0).Int64s(column)
}

func (v *StructSliceValue) Uints(column string) []uint {
	return v.At(0).Uints(column)
}

func (v *StructSliceValue) Uint8s(column string) []uint8 {
	return v.At(0).Uint8s(column)
}

func (v *StructSliceValue) Uint16s(column string) []uint16 {
	return v.At(0).Uint16s(column)
}

func (v *StructSliceValue) Uint32s(column string) []uint32 {
	return v.At(0).Uint32s(column)
}

func (v *StructSliceValue) Uint64s(column string) []uint64 {
	return v.At(0).Uint64s(column)
}

func (v *StructSliceValue) Float32s(column string) []float32 {
	return v.At(0).Float32s(column)
}

func (v *StructSliceValue) Float64s(column string) []float64 {
	return v.At(0).Float64s(column)
}

func (v *StructSliceValue) Bools(column string) []bool {
	return v.At(0).Bools(column)
}

func (v *StructSliceValue) Strings(column string) []string {
	return v.At(0).Strings(column)
}

func (v *StructSliceValue) Times(column string) []time.Time {
	return v.At(0).Times(column)
}

func (v *StructSliceValue) Error() error {
	if err := v.At(0).Error(); err != nil {
		return xerrors.Errorf("error for struct slice value: %w", err)
	}
	return nil
}

func (v *StructSliceValue) Append(value *StructValue) {
	if value == nil {
		return
	}
	v.values = append(v.values, value)
}

func (v *StructSliceValue) AppendSlice(slice *StructSliceValue) {
	v.values = append(v.values, slice.values...)
}

func (v *StructSliceValue) Sort(orders []*OrderCondition) {
	for _, order := range orders {
		column := order.column
		isAsc := order.isAsc
		sort.SliceStable(v.values, func(i, j int) bool {
			if isAsc {
				return v.values[i].fields[column].LT(v.values[j].fields[column])
			}
			return v.values[i].fields[column].GT(v.values[j].fields[column])
		})
	}
}

func (v *StructSliceValue) Filter(condition Condition) *StructSliceValue {
	values := []*StructValue{}
	for _, value := range v.values {
		if condition.Compare(value.fields[condition.Column()]) {
			values = append(values, value)
		}
	}
	return &StructSliceValue{values: values}
}

type StructValue struct {
	typ       *Struct
	fields    map[string]*Value
	decodeErr error
}

var (
	defaultValueEncoderMap = map[TypeID]func(*msgpack.Encoder) error{
		IntType: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeInt(0); err != nil {
				return xerrors.Errorf("failed to encode default int value: %w", err)
			}
			return nil
		},
		Int8Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeInt8(0); err != nil {
				return xerrors.Errorf("failed to encode default int8 value: %w", err)
			}
			return nil
		},
		Int16Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeInt16(0); err != nil {
				return xerrors.Errorf("failed to encode default int16 value: %w", err)
			}
			return nil
		},
		Int32Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeInt32(0); err != nil {
				return xerrors.Errorf("failed to encode default int32 value: %w", err)
			}
			return nil
		},
		Int64Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeInt64(0); err != nil {
				return xerrors.Errorf("failed to encode default int64 value: %w", err)
			}
			return nil
		},
		UintType: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeUint(0); err != nil {
				return xerrors.Errorf("failed to encode default uint value: %w", err)
			}
			return nil
		},
		Uint8Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeUint8(0); err != nil {
				return xerrors.Errorf("failed to encode default uint8 value: %w", err)
			}
			return nil
		},
		Uint16Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeUint16(0); err != nil {
				return xerrors.Errorf("failed to encode default uint16 value: %w", err)
			}
			return nil
		},
		Uint32Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeUint32(0); err != nil {
				return xerrors.Errorf("failed to encode default uint32 value: %w", err)
			}
			return nil
		},
		Uint64Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeUint64(0); err != nil {
				return xerrors.Errorf("failed to encode default uint64 value: %w", err)
			}
			return nil
		},
		Float32Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeFloat32(0); err != nil {
				return xerrors.Errorf("failed to encode default float32 value: %w", err)
			}
			return nil
		},
		Float64Type: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeFloat64(0); err != nil {
				return xerrors.Errorf("failed to encode default float64 value: %w", err)
			}
			return nil
		},
		BoolType: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeBool(false); err != nil {
				return xerrors.Errorf("failed to encode default bool value: %w", err)
			}
			return nil
		},
		StringType: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeString(""); err != nil {
				return xerrors.Errorf("failed to encode default string value: %w", err)
			}
			return nil
		},
		BytesType: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeBytes([]byte{}); err != nil {
				return xerrors.Errorf("failed to encode default []byte value: %w", err)
			}
			return nil
		},
		TimeType: func(enc *msgpack.Encoder) error {
			if err := enc.EncodeNil(); err != nil {
				return xerrors.Errorf("failed to encode nil: %w", err)
			}
			return nil
		},
	}
)

func encodeDefaultValue(typ TypeID, enc *msgpack.Encoder) error {
	if err := defaultValueEncoderMap[typ](enc); err != nil {
		return xerrors.Errorf("failed to encode default value: %w", err)
	}
	return nil
}

func (v *StructValue) encode(enc *msgpack.Encoder) error {
	if v == nil {
		if err := enc.EncodeNil(); err != nil {
			return xerrors.Errorf("failed to encode nil: %w", err)
		}
		return nil
	}
	columns := v.typ.Columns()
	for _, column := range columns {
		value, exists := v.fields[column]
		if exists {
			if err := value.encode(enc); err != nil {
				return xerrors.Errorf("failed to encode: %w", err)
			}
		} else {
			if err := encodeDefaultValue(v.typ.fields[column].typ, enc); err != nil {
				return xerrors.Errorf("failed to encode default value: %w", err)
			}
		}
	}
	return nil
}

func (v *StructValue) encodeValue() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := v.encode(enc); err != nil {
		return nil, xerrors.Errorf("failed to encode value: %w", err)
	}
	return buf.Bytes(), nil
}

func (v *StructValue) Release() {
	if v == nil {
		return
	}
	for _, v := range v.fields {
		v.Release()
	}
}

func (v *StructValue) EncodeLog() string {
	if v == nil {
		return nilStr
	}
	log := "{"
	columnLogs := []string{}
	for _, column := range v.typ.Columns() {
		field := v.fields[column]
		value := nilStr
		if field != nil {
			value = v.fields[column].String()
		}
		columnLogs = append(columnLogs, fmt.Sprintf(`%s:%s`, column, value))
	}
	log += strings.Join(columnLogs, ",")
	return log + "}"
}

func (v *StructValue) Len() int {
	return 0
}

func (v *StructValue) At(int) Decoder {
	return v
}

func (v *StructValue) Int(column string) int {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != IntType {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required int: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.intValue
}

func (v *StructValue) Int8(column string) int8 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Int8Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required int8: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.int8Value
}

func (v *StructValue) Int16(column string) int16 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Int16Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required int16: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.int16Value
}

func (v *StructValue) Int32(column string) int32 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Int32Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required int32: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.int32Value
}

func (v *StructValue) Int64(column string) int64 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Int64Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required int64: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.int64Value
}

func (v *StructValue) Uint(column string) uint {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != UintType {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required uint: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.uintValue
}

func (v *StructValue) Uint8(column string) uint8 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Uint8Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required uint8: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.uint8Value
}

func (v *StructValue) Uint16(column string) uint16 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Uint16Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required uint16: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.uint16Value
}

func (v *StructValue) Uint32(column string) uint32 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Uint32Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required uint32: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.uint32Value
}

func (v *StructValue) Uint64(column string) uint64 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Uint64Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required uint64: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.uint64Value
}

func (v *StructValue) Float32(column string) float32 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Float32Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required float32: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.float32Value
}

func (v *StructValue) Float64(column string) float64 {
	if v.decodeErr != nil {
		return 0
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return 0
	}
	if value.typ != Float64Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required float64: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return 0
	}
	return value.float64Value
}

func (v *StructValue) Bool(column string) bool {
	if v.decodeErr != nil {
		return false
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return false
	}
	if value.typ != BoolType {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required bool: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return false
	}
	return value.boolValue
}

func (v *StructValue) String(column string) string {
	if v.decodeErr != nil {
		return ""
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return ""
	}
	if value.typ != StringType {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required string: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return ""
	}
	return value.stringValue
}

func (v *StructValue) Bytes(column string) []byte {
	if v.decodeErr != nil {
		return []byte{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []byte{}
	}
	if value.typ != BytesType {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required []byte: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return []byte{}
	}
	return value.bytesValue
}

func (v *StructValue) Time(column string) time.Time {
	if v.decodeErr != nil {
		return time.Time{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return time.Time{}
	}
	if value.typ != TimeType {
		v.decodeErr = xerrors.Errorf("%s.%s type is %s but required time.Time: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return time.Time{}
	}
	return value.timeValue
}

func (v *StructValue) Ints(column string) []int {
	if v.decodeErr != nil {
		return []int{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []int{}
	}
	s := value.sliceValue
	values := make([]int, len(s))
	for idx, value := range s {
		values[idx] = value.intValue
	}
	return values
}

func (v *StructValue) Int8s(column string) []int8 {
	if v.decodeErr != nil {
		return []int8{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []int8{}
	}
	s := value.sliceValue
	values := make([]int8, len(s))
	for idx, value := range s {
		values[idx] = value.int8Value
	}
	return values
}

func (v *StructValue) Int16s(column string) []int16 {
	if v.decodeErr != nil {
		return []int16{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []int16{}
	}
	s := value.sliceValue
	values := make([]int16, len(s))
	for idx, value := range s {
		values[idx] = value.int16Value
	}
	return values
}

func (v *StructValue) Int32s(column string) []int32 {
	if v.decodeErr != nil {
		return []int32{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []int32{}
	}
	s := value.sliceValue
	values := make([]int32, len(s))
	for idx, value := range s {
		values[idx] = value.int32Value
	}
	return values
}

func (v *StructValue) Int64s(column string) []int64 {
	if v.decodeErr != nil {
		return []int64{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []int64{}
	}
	s := value.sliceValue
	values := make([]int64, len(s))
	for idx, value := range s {
		values[idx] = value.int64Value
	}
	return values
}

func (v *StructValue) Uints(column string) []uint {
	if v.decodeErr != nil {
		return []uint{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []uint{}
	}
	s := value.sliceValue
	values := make([]uint, len(s))
	for idx, value := range s {
		values[idx] = value.uintValue
	}
	return values
}

func (v *StructValue) Uint8s(column string) []uint8 {
	if v.decodeErr != nil {
		return []uint8{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []uint8{}
	}
	s := value.sliceValue
	values := make([]uint8, len(s))
	for idx, value := range s {
		values[idx] = value.uint8Value
	}
	return values
}

func (v *StructValue) Uint16s(column string) []uint16 {
	if v.decodeErr != nil {
		return []uint16{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []uint16{}
	}
	s := value.sliceValue
	values := make([]uint16, len(s))
	for idx, value := range s {
		values[idx] = value.uint16Value
	}
	return values
}

func (v *StructValue) Uint32s(column string) []uint32 {
	if v.decodeErr != nil {
		return []uint32{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []uint32{}
	}
	s := value.sliceValue
	values := make([]uint32, len(s))
	for idx, value := range s {
		values[idx] = value.uint32Value
	}
	return values
}

func (v *StructValue) Uint64s(column string) []uint64 {
	if v.decodeErr != nil {
		return []uint64{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []uint64{}
	}
	s := value.sliceValue
	values := make([]uint64, len(s))
	for idx, value := range s {
		values[idx] = value.uint64Value
	}
	return values
}

func (v *StructValue) Float32s(column string) []float32 {
	if v.decodeErr != nil {
		return []float32{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []float32{}
	}
	s := value.sliceValue
	values := make([]float32, len(s))
	for idx, value := range s {
		values[idx] = value.float32Value
	}
	return values
}

func (v *StructValue) Float64s(column string) []float64 {
	if v.decodeErr != nil {
		return []float64{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []float64{}
	}
	s := value.sliceValue
	values := make([]float64, len(s))
	for idx, value := range s {
		values[idx] = value.float64Value
	}
	return values
}

func (v *StructValue) Bools(column string) []bool {
	if v.decodeErr != nil {
		return []bool{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []bool{}
	}
	s := value.sliceValue
	values := make([]bool, len(s))
	for idx, value := range s {
		values[idx] = value.boolValue
	}
	return values
}

func (v *StructValue) Strings(column string) []string {
	if v.decodeErr != nil {
		return []string{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []string{}
	}
	s := value.sliceValue
	values := make([]string, len(s))
	for idx, value := range s {
		values[idx] = value.stringValue
	}
	return values
}

func (v *StructValue) Times(column string) []time.Time {
	if v.decodeErr != nil {
		return []time.Time{}
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return []time.Time{}
	}
	s := value.sliceValue
	values := make([]time.Time, len(s))
	for idx, value := range s {
		values[idx] = value.timeValue
	}
	return values
}

func (v *StructValue) Slice(column string, unmarshaler Unmarshaler) {
	if v.decodeErr != nil {
		return
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if err := unmarshaler.DecodeRapidash(Values(value.sliceValue)); err != nil {
		v.decodeErr = xerrors.Errorf("failed to decode slice value: %w", err)
	}
}

func (v *StructValue) Struct(column string, unmarshaler Unmarshaler) {
	if v.decodeErr != nil {
		return
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return
	}
	if value.structValue == nil {
		return
	}
	if err := unmarshaler.DecodeRapidash(value.structValue); err != nil {
		v.decodeErr = xerrors.Errorf("failed to decode struct value: %w", err)
	}
}

func (v *StructValue) IntPtr(column string) *int {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != IntType {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *int: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	i := value.intValue
	return &i
}

func (v *StructValue) Int8Ptr(column string) *int8 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Int8Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *int8: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	i := value.int8Value
	return &i
}

func (v *StructValue) Int16Ptr(column string) *int16 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Int16Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *int16: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	i := value.int16Value
	return &i
}

func (v *StructValue) Int32Ptr(column string) *int32 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Int32Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *int32: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	i := value.int32Value
	return &i
}

func (v *StructValue) Int64Ptr(column string) *int64 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Int64Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *int64: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	i := value.int64Value
	return &i
}

func (v *StructValue) UintPtr(column string) *uint {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != UintType {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *uint: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	u := value.uintValue
	return &u
}

func (v *StructValue) Uint8Ptr(column string) *uint8 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Uint8Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *uint8",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	u := value.uint8Value
	return &u
}

func (v *StructValue) Uint16Ptr(column string) *uint16 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Uint16Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *uint16: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	u := value.uint16Value
	return &u
}

func (v *StructValue) Uint32Ptr(column string) *uint32 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Uint32Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *uint32: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	u := value.uint32Value
	return &u
}

func (v *StructValue) Uint64Ptr(column string) *uint64 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Uint64Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *uint64: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	u := value.uint64Value
	return &u
}

func (v *StructValue) Float32Ptr(column string) *float32 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Float32Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *float32: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	f := value.float32Value
	return &f
}

func (v *StructValue) Float64Ptr(column string) *float64 {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != Float64Type {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *float64: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	f := value.float64Value
	return &f
}

func (v *StructValue) BoolPtr(column string) *bool {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != BoolType {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *bool: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	b := value.boolValue
	return &b
}

func (v *StructValue) StringPtr(column string) *string {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != StringType {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *string: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	s := value.stringValue
	return &s
}

func (v *StructValue) BytesPtr(column string) *[]byte {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != BytesType {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *[]byte: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	b := value.bytesValue
	return &b
}

func (v *StructValue) TimePtr(column string) *time.Time {
	if v.decodeErr != nil {
		return nil
	}
	value, exists := v.fields[column]
	if !exists {
		v.decodeErr = xerrors.Errorf("%s.%s: %w", v.typ.tableName, column, ErrUnknownColumnName)
		return nil
	}
	if value.IsNil {
		return nil
	}
	if value.typ != TimeType {
		v.decodeErr = xerrors.Errorf("%s.%s type is *%s but required *time.Time: %w",
			v.typ.tableName, column, value.typ, ErrInvalidDecodeType)
		return nil
	}
	t := value.timeValue
	return &t
}

func (v *StructValue) Error() error {
	return v.decodeErr
}

func (v *StructValue) ValueByColumn(column string) *Value {
	return v.fields[column]
}

func (sf *StructField) ScanValue(factory *ValueFactory) *Value {
	return factory.CreateDefaultValue(sf.typ)
}

func NewStruct(tableName string) *Struct {
	return &Struct{
		tableName: tableName,
		fields:    map[string]*StructField{},
	}
}

func (s *Struct) sortedFields() []*StructField {
	fields := []*StructField{}
	for _, field := range s.fields {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].index < fields[j].index
	})
	return fields
}

func (s *Struct) addNewField(column string, typ TypeID, kind TypeKind) *Struct {
	field := &StructField{
		typ:    typ,
		kind:   kind,
		column: column,
		index:  len(s.fields),
	}
	s.fields[column] = field
	return s
}

func (s *Struct) FieldInt(column string) *Struct {
	return s.addNewField(column, IntType, IntKind)
}

func (s *Struct) FieldInt8(column string) *Struct {
	return s.addNewField(column, Int8Type, IntKind)
}

func (s *Struct) FieldInt16(column string) *Struct {
	return s.addNewField(column, Int16Type, IntKind)
}

func (s *Struct) FieldInt32(column string) *Struct {
	return s.addNewField(column, Int32Type, IntKind)
}

func (s *Struct) FieldInt64(column string) *Struct {
	return s.addNewField(column, Int64Type, IntKind)
}

func (s *Struct) FieldUint(column string) *Struct {
	return s.addNewField(column, UintType, IntKind)
}

func (s *Struct) FieldUint8(column string) *Struct {
	return s.addNewField(column, Uint8Type, IntKind)
}

func (s *Struct) FieldUint16(column string) *Struct {
	return s.addNewField(column, Uint16Type, IntKind)
}

func (s *Struct) FieldUint32(column string) *Struct {
	return s.addNewField(column, Uint32Type, IntKind)
}

func (s *Struct) FieldUint64(column string) *Struct {
	return s.addNewField(column, Uint64Type, IntKind)
}

func (s *Struct) FieldFloat32(column string) *Struct {
	return s.addNewField(column, Float32Type, FloatKind)
}

func (s *Struct) FieldFloat64(column string) *Struct {
	return s.addNewField(column, Float64Type, FloatKind)
}

func (s *Struct) FieldBool(column string) *Struct {
	return s.addNewField(column, BoolType, BoolKind)
}

func (s *Struct) FieldString(column string) *Struct {
	return s.addNewField(column, StringType, StringKind)
}

func (s *Struct) FieldBytes(column string) *Struct {
	return s.addNewField(column, BytesType, BytesKind)
}

func (s *Struct) FieldTime(column string) *Struct {
	return s.addNewField(column, TimeType, TimeKind)
}

func (s *Struct) FieldSlice(column string, typ TypeID) *Struct {
	field := &StructField{
		typ:     SliceType,
		column:  column,
		index:   len(s.fields),
		subtype: typ,
	}
	s.fields[column] = field
	return s
}

func (s *Struct) FieldStructSlice(column string, structType *Struct) *Struct {
	field := &StructField{
		typ:           SliceType,
		column:        column,
		index:         len(s.fields),
		subtype:       StructType,
		subtypeStruct: structType,
	}
	s.fields[column] = field
	return s
}

func (s *Struct) FieldSelfStructSlice(column string) *Struct {
	field := &StructField{
		typ:           SliceType,
		column:        column,
		index:         len(s.fields),
		subtype:       StructType,
		subtypeStruct: s,
	}
	s.fields[column] = field
	return s
}

func (s *Struct) FieldStruct(column string, structType *Struct) *Struct {
	field := &StructField{
		typ:           StructType,
		column:        column,
		index:         len(s.fields),
		subtype:       StructType,
		subtypeStruct: structType,
	}
	s.fields[column] = field
	return s
}

func (s *Struct) FieldSelfStruct(column string) *Struct {
	field := &StructField{
		typ:           StructType,
		column:        column,
		index:         len(s.fields),
		subtype:       StructType,
		subtypeStruct: s,
	}
	s.fields[column] = field
	return s
}

func (s *Struct) Cast(coder Coder) Type {
	return &StructCoder{typ: s, value: coder}
}

func (s *Struct) Columns() []string {
	fields := s.sortedFields()
	columns := make([]string, len(fields))
	for idx, field := range fields {
		columns[idx] = field.column
	}
	return columns
}

func (s *Struct) ScanValues(factory *ValueFactory) []interface{} {
	fields := s.sortedFields()
	values := make([]interface{}, len(fields))
	for idx, field := range fields {
		values[idx] = field.ScanValue(factory)
	}
	return values
}

func (s *Struct) StructValue(values []interface{}) *StructValue {
	fields := map[string]*Value{}
	for idx, field := range s.sortedFields() {
		fields[field.column] = values[idx].(*Value)
	}
	return &StructValue{
		typ:    s,
		fields: fields,
	}
}
