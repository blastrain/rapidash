package server

import (
	"reflect"
	"testing"
)

func Equal(t *testing.T, src interface{}, dst interface{}) {
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("not equal %v and %v", src, dst)
	}
}
