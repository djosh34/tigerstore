//go:build !(sim || simio)

package wl

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestTurnWatchSamplesASleepingTurn(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	w := newTurnWatch()
	now := time.Now()
	w.begin(now)
	time.Sleep(100 * time.Millisecond)
	w.end()
	r := w.report(now)
	t.Log(r)
	if !strings.Contains(r, "S/") {
		t.Fatalf("no sleeping sample: %s", r)
	}
}
