package main

import (
	"testing"
	"time"
)

// TestCPURates checks that every per-byte rate is measured and plausible:
// above 0 and under 100 ns a byte on any machine this runs on.
func TestCPURates(t *testing.T) {
	seal, err := encrypt()
	if err != nil {
		t.Fatal(err)
	}
	sendNs, err := send(50 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for name, ns := range map[string]float64{
		"encode":  perByte(50*time.Millisecond, encode()),
		"encrypt": perByte(50*time.Millisecond, seal),
		"send":    sendNs,
	} {
		if !(ns > 0 && ns < 100) {
			t.Errorf("%s: %v ns a byte", name, ns)
		}
	}
}
