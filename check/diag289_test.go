package check

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// The exact -sim.out history from the failing race-sim job of #289.
func TestDiag289History(t *testing.T) {
	if os.Getenv("DIAG289") == "" {
		t.Skip("diagnosis-only replay")
	}
	f, err := os.Open("testdata/diag289-seed7004.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	var h history.History
	if err := json.NewDecoder(z).Decode(&h); err != nil {
		t.Fatal(err)
	}
	limit := time.Minute
	if v := os.Getenv("DIAG289_TIMEOUT"); v != "" {
		limit, err = time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range bothModes {
		if v := os.Getenv("DIAG289_MODE"); v != "" && v != mode.String() {
			continue
		}
		t.Run(mode.String(), func(t *testing.T) {
			start := time.Now()
			err := Witnessed(&h, mode, limit)
			t.Logf("mode=%s ops=%d limit=%s elapsed=%s verdict=%v", mode, len(h.Ops), limit, time.Since(start), err)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// #284's reduced legal order and its stale variant, with exact verdicts.
func TestDiag289Reduced284(t *testing.T) {
	for _, early := range []bool{false, true} {
		for _, mode := range bothModes {
			err := Witnessed(grantReadCompact(t, early), mode, time.Second)
			t.Logf("stale=%v mode=%s verdict=%v", early, mode, err)
			if (err != nil) != early {
				t.Errorf("stale=%v mode=%s: %v", early, mode, err)
			}
		}
	}
}
