// Copyright 2024 The Go Authors. All rights reserved.  Use of this source code
// is governed by a BSD-style license that can be found at
// https://go.googlesource.com/go/+/refs/heads/master/LICENSE.

// Based on https://go.googlesource.com/go/+/refs/heads/master/src/reflect/iter.go.

package reflect

import "iter"

// TypeAssert is semantically equivalent to v.Interface().(T).
func TypeAssert[T any](v Value) (T, bool) {
	x, ok := v.Interface().(T)
	return x, ok
}

// Seq returns an iter.Seq[Value] that loops over the elements of v, like
// reflect.Value.Seq.
func (v Value) Seq() iter.Seq[Value] {
	if v.kind == wrappedMap {
		return func(yield func(Value) bool) {
			for it := v.MapRange(); it.Next(); {
				if !yield(it.Key()) {
					return
				}
			}
		}
	}
	return func(yield func(Value) bool) {
		for x := range v.inner.Seq() {
			if !yield(wrapValue(x)) {
				return
			}
		}
	}
}

// Seq2 returns an iter.Seq2[Value, Value] that loops over the elements of v,
// like reflect.Value.Seq2.
func (v Value) Seq2() iter.Seq2[Value, Value] {
	if v.kind == wrappedMap {
		return func(yield func(Value, Value) bool) {
			for it := v.MapRange(); it.Next(); {
				if !yield(it.Key(), it.Value()) {
					return
				}
			}
		}
	}
	return func(yield func(Value, Value) bool) {
		for x, y := range v.inner.Seq2() {
			if !yield(wrapValue(x), wrapValue(y)) {
				return
			}
		}
	}
}

func (t *typeImpl) CanSeq() bool {
	return t.kind == wrappedMap || t.inner.CanSeq()
}

func (t *typeImpl) CanSeq2() bool {
	return t.kind == wrappedMap || t.inner.CanSeq2()
}
