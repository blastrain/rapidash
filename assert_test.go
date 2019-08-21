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
