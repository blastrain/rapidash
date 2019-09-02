package rapidash

import (
	"testing"
	"time"
)

func TestRemoveDuplidateCondition(t *testing.T) {
	t.Run("remove condition int", func(t *testing.T) {
		values := []*Value{{typ: IntType, intValue: 1}, {typ: IntType, intValue: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition int8", func(t *testing.T) {
		values := []*Value{{typ: Int8Type, int8Value: 1}, {typ: Int8Type, int8Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition int16", func(t *testing.T) {
		values := []*Value{{typ: Int16Type, int16Value: 1}, {typ: Int16Type, int16Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition int32", func(t *testing.T) {
		values := []*Value{{typ: Int32Type, int32Value: 1}, {typ: Int32Type, int32Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition int64", func(t *testing.T) {
		values := []*Value{{typ: Int64Type, int64Value: 1}, {typ: Int64Type, int64Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition uint", func(t *testing.T) {
		values := []*Value{{typ: UintType, uintValue: 1}, {typ: UintType, uintValue: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition uint8", func(t *testing.T) {
		values := []*Value{{typ: Uint8Type, uint8Value: 1}, {typ: Uint8Type, uint8Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition uint16", func(t *testing.T) {
		values := []*Value{{typ: Uint16Type, uint16Value: 1}, {typ: Uint16Type, uint16Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition uint32", func(t *testing.T) {
		values := []*Value{{typ: Uint32Type, uint32Value: 1}, {typ: Uint32Type, uint32Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition uint64", func(t *testing.T) {
		values := []*Value{{typ: Uint64Type, uint64Value: 1}, {typ: Uint64Type, uint64Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition float32", func(t *testing.T) {
		values := []*Value{{typ: Float32Type, float32Value: 1}, {typ: Float32Type, float32Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition float64", func(t *testing.T) {
		values := []*Value{{typ: Float64Type, float64Value: 1}, {typ: Float64Type, float64Value: 1}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition bool", func(t *testing.T) {
		values := []*Value{{typ: BoolType, boolValue: true}, {typ: BoolType, boolValue: true}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition string", func(t *testing.T) {
		values := []*Value{{typ: StringType, stringValue: "abc"}, {typ: StringType, stringValue: "abc"}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition []byte", func(t *testing.T) {
		values := []*Value{{typ: BytesType, bytesValue: []byte{1, 2, 3}}, {typ: BytesType, bytesValue: []byte{1, 2, 3}}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition time", func(t *testing.T) {
		values := []*Value{{typ: TimeType, timeValue: time.Unix(1567393200, 0)}, {typ: TimeType, timeValue: time.Unix(1567393200, 0)}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition slice", func(t *testing.T) {
		values := []*Value{{typ: SliceType, sliceValue: []*Value{{typ: Uint64Type, uint64Value: 1}, {typ: Uint64Type, uint64Value: 1}}}, {typ: SliceType, sliceValue: []*Value{{typ: Uint64Type, uint64Value: 1}, {typ: Uint64Type, uint64Value: 1}}}}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
	t.Run("remove condition struct", func(t *testing.T) {
		values := []*Value{
			{typ: StructType, structValue: &StructValue{typ: &Struct{tableName: "users"}, fields: map[string]*Value{"id": {typ: Uint64Type, uint64Value: 1}}}},
			{typ: StructType, structValue: &StructValue{typ: &Struct{tableName: "users"}, fields: map[string]*Value{"id": {typ: Uint64Type, uint64Value: 1}}}},
		}
		condition := &INCondition{column: "id", values: values}
		condition.removeDuplidateConditionValue()
		Equal(t, 1, len(condition.values))
	})
}
