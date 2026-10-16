package main

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

func TestExpectedValues(t *testing.T) {
	w := &workload{changed: map[int]int64{}}
	for i, b := range k8ssize.Buckets {
		for range b.Count {
			w.rows = append(w.rows, i)
		}
	}
	w.objects = len(w.rows)
	var size int
	for i := range w.objects {
		v := w.value(i, 0)
		size += len(v)
		if err := w.check([]byte(w.key(i)), v); err != nil {
			t.Fatal(err)
		}
		w.changed[i] = 1
		if err := w.check([]byte(w.key(i)), v); err == nil {
			t.Fatalf("old value accepted for %s", w.key(i))
		}
		if err := w.check([]byte(w.key(i)), w.value(i, 1)); err != nil {
			t.Fatal(err)
		}
		w.changed[i] = -1
		if err := w.check([]byte(w.key(i)), w.value(i, 1)); err == nil {
			t.Fatalf("deleted value accepted for %s", w.key(i))
		}
	}
	if w.objects != 6345 || size != 9253583 {
		t.Fatalf("histogram changed: %d objects, %d bytes", w.objects, size)
	}
}
