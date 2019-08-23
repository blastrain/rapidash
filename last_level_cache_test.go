package rapidash

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/xerrors"
)

type SimpleType struct {
	i int
}

type SimpleTypes []SimpleType

func (s *SimpleType) EncodeRapidash(enc Encoder) error {
	if s == nil {
		return nil
	}
	enc.Int("i", s.i)
	return enc.Error()
}

func (c *SimpleTypes) EncodeRapidash(enc Encoder) error {
	for _, v := range *c {
		if err := v.EncodeRapidash(enc.New()); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
	}
	return nil
}

func (c *SimpleTypes) DecodeRapidash(dec Decoder) error {
	len := dec.Len()
	*c = make(SimpleTypes, len)
	for i := 0; i < len; i++ {
		var v SimpleType
		if err := v.DecodeRapidash(dec.At(i)); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
		(*c)[i] = v
	}
	return nil
}

func (s *SimpleType) DecodeRapidash(dec Decoder) error {
	s.i = dec.Int("i")
	return dec.Error()
}

func simpleType() *Struct {
	return NewStruct("simple_type").
		FieldInt("i")
}

type ComplicatedType struct {
	intValue         int
	int8Value        int8
	int16Value       int16
	int32Value       int32
	int64Value       int64
	uintValue        uint
	uint8Value       uint8
	uint16Value      uint16
	uint32Value      uint32
	uint64Value      uint64
	float32Value     float32
	float64Value     float64
	stringValue      string
	bytesValue       []byte
	boolValue        bool
	timeValue        time.Time
	timePointerValue *time.Time
	sliceValue       []*ComplicatedType
	otherSliceValue  SimpleTypes
	intsValue        []int
	int8sValue       []int8
	int16sValue      []int16
	int32sValue      []int32
	int64sValue      []int64
	uintsValue       []uint
	uint8sValue      []uint8
	uint16sValue     []uint16
	uint32sValue     []uint32
	uint64sValue     []uint64
	float32sValue    []float32
	float64sValue    []float64
	stringsValue     []string
	boolsValue       []bool
	timesValue       []time.Time

	structValue      *ComplicatedType
	otherStructValue SimpleType
}

func complicatedType() *Struct {
	return NewStruct("complicated_type").
		FieldInt("int").
		FieldInt8("int8").
		FieldInt16("int16").
		FieldInt32("int32").
		FieldInt64("int64").
		FieldUint("uint").
		FieldUint8("uint8").
		FieldUint16("uint16").
		FieldUint32("uint32").
		FieldUint64("uint64").
		FieldFloat32("float32").
		FieldFloat64("float64").
		FieldString("string").
		FieldBytes("bytes").
		FieldBool("bool").
		FieldTime("time").
		FieldTime("timePointer").
		FieldSelfStructSlice("slice").
		FieldStructSlice("simples", simpleType()).
		FieldSlice("ints", IntType).
		FieldSlice("int8s", Int8Type).
		FieldSlice("int16s", Int16Type).
		FieldSlice("int32s", Int32Type).
		FieldSlice("int64s", Int64Type).
		FieldSlice("uints", UintType).
		FieldSlice("uint8s", Uint8Type).
		FieldSlice("uint16s", Uint16Type).
		FieldSlice("uint32s", Uint32Type).
		FieldSlice("uint64s", Uint64Type).
		FieldSlice("float32s", Float32Type).
		FieldSlice("float64s", Float64Type).
		FieldSlice("strings", StringType).
		FieldSlice("bools", BoolType).
		FieldSlice("times", TimeType).
		FieldSelfStruct("struct").
		FieldStruct("simple", simpleType())
}

type ComplicatedTypes []*ComplicatedType

func (c *ComplicatedTypes) EncodeRapidash(enc Encoder) error {
	for _, v := range *c {
		if err := v.EncodeRapidash(enc.New()); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
	}
	return nil
}

func (c *ComplicatedTypes) DecodeRapidash(dec Decoder) error {
	len := dec.Len()
	*c = make([]*ComplicatedType, len)
	for i := 0; i < len; i++ {
		var v ComplicatedType
		if err := v.DecodeRapidash(dec.At(i)); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
		(*c)[i] = &v
	}
	return nil
}

func (c *ComplicatedType) EncodeRapidash(enc Encoder) error {
	if c == nil {
		return nil
	}
	enc.Int("int", c.intValue)
	enc.Int8("int8", c.int8Value)
	enc.Int16("int16", c.int16Value)
	enc.Int32("int32", c.int32Value)
	enc.Int64("int64", c.int64Value)
	enc.Uint("uint", c.uintValue)
	enc.Uint8("uint8", c.uint8Value)
	enc.Uint16("uint16", c.uint16Value)
	enc.Uint32("uint32", c.uint32Value)
	enc.Uint64("uint64", c.uint64Value)
	enc.Float32("float32", c.float32Value)
	enc.Float64("float64", c.float64Value)
	enc.String("string", c.stringValue)
	enc.Bytes("bytes", c.bytesValue)
	enc.Bool("bool", c.boolValue)
	enc.Time("time", c.timeValue)
	enc.TimePtr("timePointer", c.timePointerValue)
	sliceValue := ComplicatedTypes(c.sliceValue)
	enc.Structs("slice", &sliceValue)
	enc.Structs("simples", &c.otherSliceValue)
	enc.Ints("ints", c.intsValue)
	enc.Int8s("int8s", c.int8sValue)
	enc.Int16s("int16s", c.int16sValue)
	enc.Int32s("int32s", c.int32sValue)
	enc.Int64s("int64s", c.int64sValue)
	enc.Uints("uints", c.uintsValue)
	enc.Uint8s("uint8s", c.uint8sValue)
	enc.Uint16s("uint16s", c.uint16sValue)
	enc.Uint32s("uint32s", c.uint32sValue)
	enc.Uint64s("uint64s", c.uint64sValue)
	enc.Float32s("float32s", c.float32sValue)
	enc.Float64s("float64s", c.float64sValue)
	enc.Strings("strings", c.stringsValue)
	enc.Bools("bools", c.boolsValue)
	enc.Times("times", c.timesValue)
	enc.Struct("struct", c.structValue)
	enc.Struct("simple", &c.otherStructValue)
	if err := enc.Error(); err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	return nil
}

func (c *ComplicatedType) DecodeRapidash(dec Decoder) error {
	c.intValue = dec.Int("int")
	c.int8Value = dec.Int8("int8")
	c.int16Value = dec.Int16("int16")
	c.int32Value = dec.Int32("int32")
	c.int64Value = dec.Int64("int64")
	c.uintValue = dec.Uint("uint")
	c.uint8Value = dec.Uint8("uint8")
	c.uint16Value = dec.Uint16("uint16")
	c.uint32Value = dec.Uint32("uint32")
	c.uint64Value = dec.Uint64("uint64")
	c.float32Value = dec.Float32("float32")
	c.float64Value = dec.Float64("float64")
	c.stringValue = dec.String("string")
	c.bytesValue = dec.Bytes("bytes")
	c.boolValue = dec.Bool("bool")
	c.timeValue = dec.Time("time")
	c.timePointerValue = dec.TimePtr("timePointer")
	c.intsValue = dec.Ints("ints")
	c.int8sValue = dec.Int8s("int8s")
	c.int16sValue = dec.Int16s("int16s")
	c.int32sValue = dec.Int32s("int32s")
	c.int64sValue = dec.Int64s("int64s")
	c.uintsValue = dec.Uints("uints")
	c.uint8sValue = dec.Uint8s("uint8s")
	c.uint16sValue = dec.Uint16s("uint16s")
	c.uint32sValue = dec.Uint32s("uint32s")
	c.uint64sValue = dec.Uint64s("uint64s")
	c.float32sValue = dec.Float32s("float32s")
	c.float64sValue = dec.Float64s("float64s")
	c.stringsValue = dec.Strings("strings")
	c.boolsValue = dec.Bools("bools")
	c.timesValue = dec.Times("times")

	var sliceValue ComplicatedTypes
	dec.Slice("slice", &sliceValue)
	c.sliceValue = sliceValue
	var otherSilceValue SimpleTypes
	dec.Slice("simples", &otherSilceValue)
	c.otherSliceValue = otherSilceValue
	var structValue ComplicatedType
	dec.Struct("struct", &structValue)
	c.structValue = &structValue
	var otherValue SimpleType
	dec.Struct("simple", &otherValue)
	c.otherStructValue = otherValue
	return dec.Error()
}

// nolint: gocyclo
func TestLLCStruct(t *testing.T) {
	now := time.Now()
	v := &ComplicatedType{
		intValue:         1,
		int8Value:        2,
		int16Value:       3,
		int32Value:       4,
		int64Value:       5,
		uintValue:        6,
		uint8Value:       7,
		uint16Value:      8,
		uint32Value:      9,
		uint64Value:      10,
		float32Value:     1.23,
		float64Value:     4.56,
		stringValue:      "hello",
		bytesValue:       []byte("world"),
		boolValue:        true,
		timeValue:        time.Now(),
		timePointerValue: &now,
		sliceValue: []*ComplicatedType{
			{
				intValue:   11,
				int8Value:  12,
				int16Value: 13,
				int32Value: 14,
				int64Value: 15,
			},
		},
		otherSliceValue: []SimpleType{
			{i: 1},
		},
		intsValue: []int{
			111,
			112,
			113,
			114,
			115,
		},
		int8sValue: []int8{
			111,
			112,
			113,
			114,
			115,
		},
		int16sValue: []int16{
			111,
			112,
			113,
			114,
			115,
		},
		int32sValue: []int32{
			111,
			112,
			113,
			114,
			115,
		},
		int64sValue: []int64{
			111,
			112,
			113,
			114,
			115,
		},
		uintsValue: []uint{
			111,
			112,
			113,
			114,
			115,
		},
		uint8sValue: []uint8{
			111,
			112,
			113,
			114,
			115,
		},
		uint16sValue: []uint16{
			111,
			112,
			113,
			114,
			115,
		},
		uint32sValue: []uint32{
			111,
			112,
			113,
			114,
			115,
		},
		uint64sValue: []uint64{
			111,
			112,
			113,
			114,
			115,
		},
		float32sValue: []float32{
			1.11,
			1.12,
			1.13,
			1.14,
			1.15,
		},
		float64sValue: []float64{
			1.11,
			1.12,
			1.13,
			1.14,
			1.15,
		},
		stringsValue: []string{
			"hello",
			"world",
		},
		boolsValue: []bool{
			true,
			false,
		},
		timesValue: []time.Time{
			now,
			now.Add(time.Minute),
		},
		structValue: &ComplicatedType{
			intValue:   21,
			int8Value:  22,
			int16Value: 23,
			int32Value: 24,
			int64Value: 25,
		},
		otherStructValue: SimpleType{
			i: 26,
		},
	}
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	t.Run("create", func(t *testing.T) {

		NoError(t, tx.Create("key1", complicatedType().Cast(v)))
		var newValue ComplicatedType
		NoError(t, tx.Find("key1", complicatedType().Cast(&newValue)))
		if v.intValue != newValue.intValue {
			t.Fatal("cannot set/get struct.intValue")
		}
		if v.int8Value != newValue.int8Value {
			t.Fatal("cannot set/get struct.int8Value")
		}
		if v.int16Value != newValue.int16Value {
			t.Fatal("cannot set/get struct.int16Value")
		}
		if v.int32Value != newValue.int32Value {
			t.Fatal("cannot set/get struct.int32Value")
		}
		if v.int64Value != newValue.int64Value {
			t.Fatal("cannot set/get struct.int64Value")
		}
		if v.uintValue != newValue.uintValue {
			t.Fatal("cannot set/get struct.uintValue")
		}
		if v.uint8Value != newValue.uint8Value {
			t.Fatal("cannot set/get struct.uint8Value")
		}
		if v.uint16Value != newValue.uint16Value {
			t.Fatal("cannot set/get struct.uint16Value")
		}
		if v.uint32Value != newValue.uint32Value {
			t.Fatal("cannot set/get struct.uint32Value")
		}
		if v.uint64Value != newValue.uint64Value {
			t.Fatal("cannot set/get struct.uint64Value")
		}
		if int(v.float32Value*100) != int(newValue.float32Value*100) {
			t.Fatal("cannot set/get struct.float32Value")
		}
		if int(v.float64Value*100) != int(newValue.float64Value*100) {
			t.Fatal("cannot set/get struct.float64Value")
		}
		if v.stringValue != newValue.stringValue {
			t.Fatal("cannot set/get struct.stringValue")
		}
		if string(v.bytesValue) != string(newValue.bytesValue) {
			t.Fatal("cannot set/get struct.bytesValue")
		}
		if v.boolValue != newValue.boolValue {
			t.Fatal("cannot set/get struct.boolValue")
		}
		if len(v.sliceValue) != len(newValue.sliceValue) {
			t.Fatal("cannot set/get struct.sliceValue")
		}
		if v.sliceValue[0].intValue != newValue.sliceValue[0].intValue {
			t.Fatal("cannot set/get struct.sliceValue")
		}
		if v.sliceValue[0].int8Value != newValue.sliceValue[0].int8Value {
			t.Fatal("cannot set/get struct.sliceValue")
		}
		if v.sliceValue[0].int16Value != newValue.sliceValue[0].int16Value {
			t.Fatal("cannot set/get struct.sliceValue")
		}
		if v.sliceValue[0].int32Value != newValue.sliceValue[0].int32Value {
			t.Fatal("cannot set/get struct.sliceValue")
		}
		if v.sliceValue[0].int64Value != newValue.sliceValue[0].int64Value {
			t.Fatal("cannot set/get struct.sliceValue")
		}
		if len(v.intsValue) != len(newValue.intsValue) {
			t.Fatal("cannot set/get struct.intsValue")
		}
		if v.intsValue[0] != newValue.intsValue[0] {
			t.Fatal("cannot set/get struct.intsValue")
		}
		if len(v.int8sValue) != len(newValue.int8sValue) {
			t.Fatal("cannot set/get struct.int8sValue")
		}
		if v.int8sValue[0] != newValue.int8sValue[0] {
			t.Fatal("cannot set/get struct.int8sValue")
		}
		if len(v.int16sValue) != len(newValue.int16sValue) {
			t.Fatal("cannot set/get struct.int16sValue")
		}
		if v.int16sValue[0] != newValue.int16sValue[0] {
			t.Fatal("cannot set/get struct.int16sValue")
		}
		if len(v.int32sValue) != len(newValue.int32sValue) {
			t.Fatal("cannot set/get struct.int32sValue")
		}
		if v.int32sValue[0] != newValue.int32sValue[0] {
			t.Fatal("cannot set/get struct.int32sValue")
		}
		if len(v.int64sValue) != len(newValue.int64sValue) {
			t.Fatal("cannot set/get struct.int64sValue")
		}
		if v.int64sValue[0] != newValue.int64sValue[0] {
			t.Fatal("cannot set/get struct.int64sValue")
		}
		if len(v.uintsValue) != len(newValue.uintsValue) {
			t.Fatal("cannot set/get struct.uintsValue")
		}
		if v.uintsValue[0] != newValue.uintsValue[0] {
			t.Fatal("cannot set/get struct.uintsValue")
		}
		if len(v.uint8sValue) != len(newValue.uint8sValue) {
			t.Fatal("cannot set/get struct.uint8sValue")
		}
		if v.uint8sValue[0] != newValue.uint8sValue[0] {
			t.Fatal("cannot set/get struct.uint8sValue")
		}
		if len(v.uint16sValue) != len(newValue.uint16sValue) {
			t.Fatal("cannot set/get struct.uint16sValue")
		}
		if v.uint16sValue[0] != newValue.uint16sValue[0] {
			t.Fatal("cannot set/get struct.uint16sValue")
		}
		if len(v.uint32sValue) != len(newValue.uint32sValue) {
			t.Fatal("cannot set/get struct.uint32sValue")
		}
		if v.uint32sValue[0] != newValue.uint32sValue[0] {
			t.Fatal("cannot set/get struct.uint32sValue")
		}
		if len(v.uint64sValue) != len(newValue.uint64sValue) {
			t.Fatal("cannot set/get struct.uint64sValue")
		}
		if v.uint64sValue[0] != newValue.uint64sValue[0] {
			t.Fatal("cannot set/get struct.uint64sValue")
		}
		if len(v.float32sValue) != len(newValue.float32sValue) {
			t.Fatal("cannot set/get struct.float32sValue")
		}
		if int(v.float32sValue[0]*100) != int(newValue.float32sValue[0]*100) {
			t.Fatal("cannot set/get struct.float32sValue")
		}
		if len(v.float64sValue) != len(newValue.float64sValue) {
			t.Fatal("cannot set/get struct.float64sValue")
		}
		if int(v.float64sValue[0]*100) != int(newValue.float64sValue[0]*100) {
			t.Fatal("cannot set/get struct.float64sValue")
		}
		if len(v.stringsValue) != len(newValue.stringsValue) {
			t.Fatal("cannot set/get struct.stringsValue")
		}
		if v.stringsValue[0] != newValue.stringsValue[0] {
			t.Fatal("cannot set/get struct.stringsValue")
		}
		if len(v.boolsValue) != len(newValue.boolsValue) {
			t.Fatal("cannot set/get struct.boolsValue")
		}
		if v.boolsValue[0] != newValue.boolsValue[0] {
			t.Fatal("cannot set/get struct.boolsValue")
		}
		if len(v.timesValue) != len(newValue.timesValue) {
			t.Fatal("cannot set/get struct.timesValue")
		}
		if !v.timesValue[0].Equal(newValue.timesValue[0]) {
			t.Fatal("cannot set/get struct.timesValue")
		}
		if v.structValue.intValue != newValue.structValue.intValue {
			t.Fatal("cannot set/get struct.structValue")
		}
		if v.structValue.int8Value != newValue.structValue.int8Value {
			t.Fatal("cannot set/get struct.structValue")
		}
		if v.structValue.int16Value != newValue.structValue.int16Value {
			t.Fatal("cannot set/get struct.structValue")
		}
		if v.structValue.int32Value != newValue.structValue.int32Value {
			t.Fatal("cannot set/get struct.structValue")
		}
		if v.structValue.int64Value != newValue.structValue.int64Value {
			t.Fatal("cannot set/get struct.structValue")
		}
		if v.otherStructValue.i != newValue.otherStructValue.i {
			t.Fatal("cannot set/get struct.otherStructValue")
		}

		t.Run("invalid struct column name", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)
			defer func() {
				NoError(t, tx.Rollback())
			}()
			typ := NewStruct("").FieldInt("int").FieldInt8("int8").FieldInt16("int16").FieldInt32("int32").
				FieldInt64("int64").FieldUint("uint").FieldUint8("uint8").FieldUint16("uint16").FieldUint32("uint32").
				FieldUint64("uint64").FieldFloat32("float32").FieldFloat64("float64").FieldString("string").FieldBytes("bytes").
				FieldBool("bool").FieldTime("time").FieldTime("timePointer").FieldSelfStructSlice("slice").FieldStructSlice("simples", simpleType()).FieldSlice("ints", IntType).
				FieldSlice("int8s", Int8Type).FieldSlice("int16s", Int16Type).FieldSlice("int32s", Int32Type).FieldSlice("int64s", Int64Type).
				FieldSlice("uints", UintType).FieldSlice("uint8s", Uint8Type).FieldSlice("uint16s", Uint16Type).FieldSlice("uint32s", Uint32Type).
				FieldSlice("uint64s", Uint64Type).FieldSlice("float32s", Float32Type).FieldSlice("float64s", Float64Type).FieldSlice("strings", StringType).
				FieldSlice("bools", BoolType).FieldSlice("times", TimeType).FieldSelfStruct("struct").FieldStruct("simple", simpleType())
			for column, field := range typ.fields {
				delete(typ.fields, column)
				typ.fields["rapidash"] = field

				if err := tx.Create("key2", Structs(v, typ)); !xerrors.Is(err, ErrUnknownColumnName) {
					t.Fatalf("%+v", err)
				}
				delete(typ.fields, "rapidash")
				typ.fields[column] = field
			}
		})
		t.Run("create by invalid struct type and decode error", func(t *testing.T) {
			typ := NewStruct("").FieldInt("int").FieldInt8("int8").FieldInt16("int16").FieldInt32("int32").
				FieldInt64("int64").FieldUint("uint").FieldUint8("uint8").FieldUint16("uint16").FieldUint32("uint32").
				FieldUint64("uint64").FieldFloat32("float32").FieldFloat64("float64").FieldString("string").FieldBytes("bytes").
				FieldBool("bool").FieldTime("time").FieldTime("timePointer").FieldSelfStructSlice("slice").FieldStructSlice("simples", simpleType()).FieldSlice("ints", IntType).
				FieldSlice("int8s", Int8Type).FieldSlice("int16s", Int16Type).FieldSlice("int32s", Int32Type).FieldSlice("int64s", Int64Type).
				FieldSlice("uints", UintType).FieldSlice("uint8s", Uint8Type).FieldSlice("uint16s", Uint16Type).FieldSlice("uint32s", Uint32Type).
				FieldSlice("uint64s", Uint64Type).FieldSlice("float32s", Float32Type).FieldSlice("float64s", Float64Type).FieldSlice("strings", StringType).
				FieldSlice("bools", BoolType).FieldSlice("times", TimeType).FieldSelfStruct("struct").FieldStruct("simple", simpleType())
			for _, field := range typ.fields {
				typeID := field.typ
				if typeID == SliceType {
					continue
				} else if typeID == StructType {
					field.typ = IntType
				} else {
					field.typ++
				}
				if err := tx.Create("key2", Structs(v, typ)); !xerrors.Is(err, ErrInvalidEncodeType) {
					t.Fatalf("%+v", err)
				}
				Error(t, tx.Find("key1", Structs(v, typ)))

				field.typ = typeID
			}
		})
	})
}

func TestLLCInt(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int", Int(1)))
	var v int
	NoError(t, tx.Find("int", IntPtr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get int value")
	}
}

func TestLLCInt8(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int8", Int8(1)))
	var v int8
	NoError(t, tx.Find("int8", Int8Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get int8 value")
	}
}

func TestLLCInt16(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int16", Int16(1)))
	var v int16
	NoError(t, tx.Find("int16", Int16Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get int16 value")
	}
}

func TestLLCInt32(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int32", Int32(1)))
	var v int32
	NoError(t, tx.Find("int32", Int32Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get int32 value")
	}
}

func TestLLCInt64(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int64", Int64(1)))
	var v int64
	NoError(t, tx.Find("int64", Int64Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get int64 value")
	}
}

func TestLLCUint(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint", Uint(1)))
	var v uint
	NoError(t, tx.Find("uint", UintPtr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get uint value")
	}
}

func TestLLCUint8(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint8", Uint8(1)))
	var v uint8
	NoError(t, tx.Find("uint8", Uint8Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get uint8 value")
	}
}

func TestLLCUint16(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint16", Uint16(1)))
	var v uint16
	NoError(t, tx.Find("uint16", Uint16Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get uint16 value")
	}
}

func TestLLCUint32(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint32", Uint32(1)))
	var v uint32
	NoError(t, tx.Find("uint32", Uint32Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get uint32 value")
	}
}

func TestLLCUint64(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint64", Uint64(1)))
	var v uint64
	NoError(t, tx.Find("uint64", Uint64Ptr(&v)))
	if v != 1 {
		t.Fatal("cannot set/get uint64 value")
	}
}

func TestLLCFloat32(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("float32", Float32(1.23)))
	var v float32
	NoError(t, tx.Find("float32", Float32Ptr(&v)))
	if fmt.Sprint(v) != fmt.Sprint(1.23) {
		t.Fatal("cannot set/get float32 value")
	}
}

func TestLLCFloat64(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("float64", Float64(1.23)))
	var v float64
	NoError(t, tx.Find("float64", Float64Ptr(&v)))
	if fmt.Sprint(v) != fmt.Sprint(1.23) {
		t.Fatal("cannot set/get float64 value")
	}
}

func TestLLCString(t *testing.T) {
	const hello = "hello"
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("string", String(hello)))
	var v string
	NoError(t, tx.Find("string", StringPtr(&v)))
	if v != hello {
		t.Fatal("cannot set/get string value")
	}
}

func TestLLCBytes(t *testing.T) {
	const hello = "hello"
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("bytes", Bytes([]byte(hello))))
	var v []byte
	NoError(t, tx.Find("bytes", BytesPtr(&v)))
	if string(v) != hello {
		t.Fatal("cannot set/get bytes value")
	}
}

func TestLLCBool(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("bool", Bool(true)))
	var v bool
	NoError(t, tx.Find("bool", BoolPtr(&v)))
	if !v {
		t.Fatal("cannot set/get bool value")
	}
}

func TestLLCTime(t *testing.T) {
	now := time.Now()
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("time", Time(now)))
	var v time.Time
	NoError(t, tx.Find("time", TimePtr(&v)))
	if !v.Equal(now) {
		t.Fatal("cannot set/get time.Time value")
	}
}

func TestLLCIntSlice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int_slice", Ints([]int{1, 2, 3})))
	var v []int
	NoError(t, tx.Find("int_slice", IntsPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []int value")
	}
}

func TestLLCInt8Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int8_slice", Int8s([]int8{1, 2, 3})))
	var v []int8
	NoError(t, tx.Find("int8_slice", Int8sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []int8 value")
	}
}

func TestLLCInt16Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int16_slice", Int16s([]int16{1, 2, 3})))
	var v []int16
	NoError(t, tx.Find("int16_slice", Int16sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []int16 value")
	}
}

func TestLLCInt32Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int32_slice", Int32s([]int32{1, 2, 3})))
	var v []int32
	NoError(t, tx.Find("int32_slice", Int32sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []int32 value")
	}
}

func TestLLCInt64Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("int64_slice", Int64s([]int64{1, 2, 3})))
	var v []int64
	NoError(t, tx.Find("int64_slice", Int64sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []int64 value")
	}
}

func TestLLCUintSlice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint_slice", Uints([]uint{1, 2, 3})))
	var v []uint
	NoError(t, tx.Find("uint_slice", UintsPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []uint value")
	}
}

func TestLLCUint8Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint8_slice", Uint8s([]uint8{1, 2, 3})))
	var v []uint8
	NoError(t, tx.Find("uint8_slice", Uint8sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []uint8 value")
	}
}

func TestLLCUint16Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint16_slice", Uint16s([]uint16{1, 2, 3})))
	var v []uint16
	NoError(t, tx.Find("uint16_slice", Uint16sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []uint16 value")
	}
}

func TestLLCUint32Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint32_slice", Uint32s([]uint32{1, 2, 3})))
	var v []uint32
	NoError(t, tx.Find("uint32_slice", Uint32sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []uint32 value")
	}
}

func TestLLCUint64Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("uint64_slice", Uint64s([]uint64{1, 2, 3})))
	var v []uint64
	NoError(t, tx.Find("uint64_slice", Uint64sPtr(&v)))
	if !(v[0] == 1 && v[1] == 2 && v[2] == 3) {
		t.Fatal("cannot set/get []uint64 value")
	}
}

func TestLLCFloat32Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("float32_slice", Float32s([]float32{1.23, 4.56, 7.89})))
	var v []float32
	NoError(t, tx.Find("float32_slice", Float32sPtr(&v)))
	if !(fmt.Sprint(v[0]) == fmt.Sprint(1.23) && fmt.Sprint(v[1]) == fmt.Sprint(4.56) && fmt.Sprint(v[2]) == fmt.Sprint(7.89)) {
		t.Fatal("cannot set/get []float32 value")
	}
}

func TestLLCFloat64Slice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("float64_slice", Float64s([]float64{1.23, 4.56, 7.89})))
	var v []float64
	NoError(t, tx.Find("float64_slice", Float64sPtr(&v)))
	if !(fmt.Sprint(v[0]) == fmt.Sprint(1.23) && fmt.Sprint(v[1]) == fmt.Sprint(4.56) && fmt.Sprint(v[2]) == fmt.Sprint(7.89)) {
		t.Fatal("cannot set/get []float64 value")
	}
}

func TestLLCStringSlice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("string_slice", Strings([]string{"hello", "world", "rapidash"})))
	var v []string
	NoError(t, tx.Find("string_slice", StringsPtr(&v)))
	if !(v[0] == "hello" && v[1] == "world" && v[2] == "rapidash") {
		t.Fatal("cannot set/get []string value")
	}
}

func TestLLCBoolSlice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("bool_slice", Bools([]bool{true, false, true})))
	var v []bool
	NoError(t, tx.Find("bool_slice", BoolsPtr(&v)))
	if !(v[0] && !v[1] && v[2]) {
		t.Fatal("cannot set/get []bool value")
	}
}

func TestLLCTimeSlice(t *testing.T) {
	now := time.Now()
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	NoError(t, tx.Create("time_slice", Times([]time.Time{now, now.Add(time.Minute), now.Add(time.Hour)})))
	var v []time.Time
	NoError(t, tx.Find("time_slice", TimesPtr(&v)))
	if !(v[0].Equal(now) && v[1].Equal(now.Add(time.Minute)) && v[2].Equal(now.Add(time.Hour))) {
		t.Fatal("cannot set/get []time.Time value")
	}
}

func TestCacheMiss(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() { NoError(t, tx.Rollback()) }()
	var v int
	if err := tx.Find("int", IntPtr(&v)); !IsCacheMiss(err) {
		t.Fatalf("%+v", err)
	}
}

func TestLLCDecodeError(t *testing.T) {
	now := time.Now()
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() { NoError(t, tx.Rollback()) }()

	NoError(t, tx.Create("time", Time(now)))
	NoError(t, tx.Create("string", String("string")))

	{
		var v int
		Error(t, tx.Find("time", IntPtr(&v)))
	}
	{
		var v int8
		Error(t, tx.Find("time", Int8Ptr(&v)))
	}
	{
		var v int16
		Error(t, tx.Find("time", Int16Ptr(&v)))
	}
	{
		var v int32
		Error(t, tx.Find("time", Int32Ptr(&v)))
	}
	{
		var v int64
		Error(t, tx.Find("time", Int64Ptr(&v)))
	}
	{
		var v uint
		Error(t, tx.Find("time", UintPtr(&v)))
	}
	{
		var v uint8
		Error(t, tx.Find("time", Uint8Ptr(&v)))
	}
	{
		var v uint16
		Error(t, tx.Find("time", Uint16Ptr(&v)))
	}
	{
		var v uint32
		Error(t, tx.Find("time", Uint32Ptr(&v)))
	}
	{
		var v uint64
		Error(t, tx.Find("time", Uint64Ptr(&v)))
	}
	{
		var v float32
		Error(t, tx.Find("time", Float32Ptr(&v)))
	}
	{
		var v float64
		Error(t, tx.Find("time", Float64Ptr(&v)))
	}
	{
		var v []byte
		Error(t, tx.Find("time", BytesPtr(&v)))
	}
	{
		var v string
		Error(t, tx.Find("time", StringPtr(&v)))
	}
	{
		var v bool
		Error(t, tx.Find("time", BoolPtr(&v)))
	}
	{
		var v time.Time
		Error(t, tx.Find("string", TimePtr(&v)))
	}
}

type T struct {
	intValue    int
	boolValue   bool
	stringValue string
}

func (t *T) EncodeRapidash(enc Encoder) error {
	enc.Int("i", t.intValue)
	enc.Bool("b", t.boolValue)
	enc.String("s", t.stringValue)
	if err := enc.Error(); err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	return nil
}

func (t *T) DecodeRapidash(dec Decoder) error {
	t.intValue = dec.Int("i")
	t.boolValue = dec.Bool("b")
	t.stringValue = dec.String("s")
	return nil
}

type MultiT []*T

func (t *MultiT) EncodeRapidash(enc Encoder) error {
	for _, v := range *t {
		if err := v.EncodeRapidash(enc.New()); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
	}
	return nil
}

func (t *MultiT) DecodeRapidash(dec Decoder) error {
	len := dec.Len()
	*t = make([]*T, len)
	for i := 0; i < len; i++ {
		var v T
		if err := v.DecodeRapidash(dec.At(i)); err != nil {
			return xerrors.Errorf("failed to decode: %w", err)
		}
		(*t)[i] = &v
	}
	return nil
}

func TestLLCStructSlice(t *testing.T) {
	tx, err := cache.Begin()
	NoError(t, err)
	defer func() {
		NoError(t, tx.Rollback())
	}()
	v := MultiT([]*T{
		{
			intValue:    1,
			boolValue:   true,
			stringValue: "hello",
		},
		{
			intValue:    2,
			boolValue:   false,
			stringValue: "world",
		},
	})

	t.Run("create by valid struct type", func(t *testing.T) {
		typ := NewStruct("").FieldInt("i").FieldBool("b").FieldString("s")
		NoError(t, tx.Create("struct_slice", Structs(&v, typ)))
		var slice MultiT
		NoError(t, tx.Find("struct_slice", StructsPtr(&slice, typ)))
		if len(v) != len(slice) {
			t.Fatal("cannot set/get struct slice value")
		}
		for i := 0; i < len(v); i++ {
			if v[i].intValue != slice[i].intValue {
				t.Fatal("cannot set/get struct slice value")
			}
			if v[i].boolValue != slice[i].boolValue {
				t.Fatal("cannot set/get struct slice value")
			}
			if v[i].stringValue != slice[i].stringValue {
				t.Fatal("cannot set/get struct slice value")
			}
		}
	})
}

func TestLLCCRUD(t *testing.T) {
	NoError(t, cache.Flush())
	key := "crud"
	t.Run("Create/Find", func(t *testing.T) {
		t.Run("Create", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)
			defer func() {
				NoError(t, tx.RollbackUnlessCommitted())
			}()
			NoError(t, tx.Create(key, String("hello")))

			NoError(t, tx.Commit())
		})
		t.Run("Find", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)
			var s string
			NoError(t, tx.Find(key, StringPtr(&s)))
			if s != "hello" {
				t.Fatal("cannot get value by LLC")
			}
		})
	})
	t.Run("Update/Find", func(t *testing.T) {
		t.Run("Update", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)
			defer func() {
				NoError(t, tx.RollbackUnlessCommitted())

			}()

			NoError(t, tx.Update(key, String("world")))

			NoError(t, tx.Commit())
		})
		t.Run("Find", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)

			var s string
			NoError(t, tx.Find(key, StringPtr(&s)))
			if s != "world" {
				t.Fatal("cannot get value by LLC")
			}
		})
	})
	t.Run("Delete/Find", func(t *testing.T) {
		t.Run("Delete", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)
			defer func() {
				NoError(t, tx.RollbackUnlessCommitted())
			}()
			NoError(t, tx.Delete(key))

			NoError(t, tx.Commit())
		})
		t.Run("Find", func(t *testing.T) {
			tx, err := cache.Begin()
			NoError(t, err)

			var s string
			Error(t, tx.Find(key, StringPtr(&s)))
		})
	})
}
