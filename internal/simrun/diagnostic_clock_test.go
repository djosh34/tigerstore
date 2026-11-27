package simrun

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/wallclock"
)

func TestDiagnosticWallClock(t *testing.T) {
	virtual := time.Now()
	started := wallclock.Now()
	for wallclock.Now()-started < int64(time.Millisecond) {
	}
	if InGosim && !time.Now().Equal(virtual) {
		t.Fatal("diagnostic clock advanced the simulation")
	}
}
