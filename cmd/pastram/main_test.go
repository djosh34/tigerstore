package main

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

func TestExpectedValues(t *testing.T) {
	w := &workload{}
	for i, b := range k8ssize.Buckets {
		for range b.Count {
			w.rows = append(w.rows, i)
		}
	}
	w.objects = len(w.rows)
	var size int
	for i := range w.objects {
		v := w.value(i)
		size += len(v)
		if err := w.check([]byte(w.key(i)), v); err != nil {
			t.Fatal(err)
		}
		if i > 0 && len(w.value(i-1)) == len(v) {
			if err := w.check([]byte(w.key(i)), w.value(i-1)); err == nil {
				t.Fatalf("another key's value accepted for %s", w.key(i))
			}
		}
	}
	if w.objects != 6345 || size != 9253583 {
		t.Fatalf("histogram changed: %d objects, %d bytes", w.objects, size)
	}
}
