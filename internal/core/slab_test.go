package core

import (
	"bytes"
	"testing"
)

func TestClassOf(t *testing.T) {
	for n := 0; n <= 1<<maxClassBits; n++ {
		c := classOf(n)
		if c == bigClass || classSize[c] < n || (c > 0 && classSize[c-1] >= n) {
			t.Fatalf("classOf(%d) = %d", n, c)
		}
	}
	if classOf(1<<maxClassBits+1) != bigClass {
		t.Fatal("big")
	}
}

func TestSlabs(t *testing.T) {
	var sl slabs
	var slots []slot
	var vals [][]byte
	for i := range 3000 {
		v := bytes.Repeat([]byte{byte(i)}, (i*997)%(300000))
		slots = append(slots, sl.put(v))
		vals = append(vals, v)
	}
	for i := 0; i < len(slots); i += 2 {
		sl.free(slots[i])
	}
	for i := 0; i < len(slots); i += 2 {
		slots[i] = sl.put(vals[i])
	}
	for i := range slots {
		if !bytes.Equal(sl.bytes(slots[i], len(vals[i])), vals[i]) {
			t.Fatalf("value %d", i)
		}
	}
}
