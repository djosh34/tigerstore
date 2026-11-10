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
