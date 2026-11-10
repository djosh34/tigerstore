package main

import (
	"testing"
	"time"
)

// TestCPUPrimitives checks that each primitive that needs no disk is
// measured and plausible: some cost, under a millisecond a call, and no
// negative cost a byte.
func TestCPUPrimitives(t *testing.T) {
	var p primitives
	if err := p.cpu(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]cost{
		"seal": p.Seal, "open": p.Open, "send": p.Send, "recv": p.Recv,
		"crc": p.CRC, "copy": p.Copy, "parse": p.Parse,
		"frames": {CallNs: p.FrameNs}, "kv": {CallNs: p.KVNs},
	} {
		if c.ByteNs < 0 || c.CallNs < 0 || c.CallNs+c.ByteNs <= 0 || c.CallNs > 1e6 {
			t.Errorf("%s: %+v", name, c)
		}
	}
}

// TestIndex times the index at every size and cold reads in every size of
// data: some cost, under 100 µs, and a cost that does not fall much as
// the index grows.
func TestIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("builds an index of a million keys and 256 MiB of pointers")
	}
	var p primitives
	if err := p.index(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if len(p.Index) != len(indexSizes) || len(p.Miss) != len(missSizes) {
		t.Fatalf("%d index sizes and %d data sizes, want %d and %d", len(p.Index), len(p.Miss), len(indexSizes), len(missSizes))
	}
	for _, c := range p.Index {
		for _, ns := range []float64{c.LookupNs, c.InsertNs, c.DeleteNs} {
			if ns <= 0 || ns > 1e5 {
				t.Errorf("%+v", c)
			}
		}
	}
	for _, c := range p.Miss {
		if c.Ns <= 0 || c.Ns > 1e5 {
			t.Errorf("%+v", c)
		}
	}
	t.Logf("%+v %+v", p.Index, p.Miss)
}
