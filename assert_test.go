package rapidash

import (
	"fmt"
	"reflect"
	"testing"
)

func Error(t *testing.T, err error) {
	if err == nil {
		t.Fatalf("required error is not nil")
	}
}

func Errorf(t *testing.T, err error, msg string, args ...interface{}) {
	if err == nil {
		t.Fatalf("%s", fmt.Sprintf(msg, args...))
	}
}

func NoError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("required error of not nil %+v", err)
	}
}

func NoErrorf(t *testing.T, err error, msg string, args ...interface{}) {
	if err != nil {
		t.Fatalf("required error of not nil. %s\n", fmt.Sprintf(msg, args...))
	}
}

func Equal(t *testing.T, src interface{}, dst interface{}) {
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("not equal %v and %v", src, dst)
	}
}

func Equalf(t *testing.T, src interface{}, dst interface{}, msg string, args ...interface{}) {
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("not equal %v and %v. %s", src, dst, fmt.Sprintf(msg, args...))
	}
}

func NotEqualf(t *testing.T, src interface{}, dst interface{}, msg string, args ...interface{}) {
	if reflect.DeepEqual(src, dst) {
		t.Fatalf("equal %v and %v. %s", src, dst, fmt.Sprintf(msg, args...))
	}
}

func IsNil(t *testing.T, src interface{}) {
	IsNilf(t, src, "")
}

// reference: https://github.com/stretchr/testify/blob/v1.5.1/assert/assertions.go#L507
func IsNilf(t *testing.T, src interface{}, msg string, args ...interface{}) {
	if src == nil {
		return
	}
	value := reflect.ValueOf(src)
	if value.IsNil() {
		return
	}

	kind := value.Kind()
	kinds := []reflect.Kind{
		reflect.Chan, reflect.Func,
		reflect.Interface, reflect.Map,
		reflect.Ptr, reflect.Slice,
	}

	for i := 0; i < len(kinds); i++ {
		if kind == kinds[i] {
			return
		}
	}

	t.Fatalf("not equal to nil %v. %s", src, fmt.Sprintf(msg, args...))
}
