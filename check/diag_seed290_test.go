package check

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestDiagSavedHistory measures the checker's search on an unchanged saved
// history. DIAG_HISTORY can select another file; no simulator runs here.
func TestDiagSavedHistory(t *testing.T) {
	path := os.Getenv("DIAG_HISTORY")
	if path == "" {
		path = "testdata/diag626-seed290.json.gz"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var h history.History
	if bytes.HasPrefix(data, []byte{0x1f, 0x8b}) {
		z, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewDecoder(z).Decode(&h)
		closeErr := z.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	} else if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	for _, mode := range bothModes {
		start := time.Now()
		w := newWitness(&h, start.Add(time.Minute))
		w.maxSteps = 50_000_000
		if mode == FromLog {
			err = w.fromLog()
			if err == nil {
				err = w.observe()
			}
			if err == nil {
				err = w.verify()
			}
			for i, e := range w.events {
				if w.states[i+1].rev >= 121 && w.states[i+1].rev <= 123 {
					t.Logf("log event %d rev %d op %d req %s", i+1, w.states[i+1].rev, e.op, describeRequest(*e.req))
				}
			}
		} else {
			err = w.fromRevisions()
		}
		t.Logf("%s ops=%d seconds=%.6f steps=%d error=%v; %s", mode, len(h.Ops), time.Since(start).Seconds(), w.steps, err, w.stoppedAt())
		if err != nil {
			t.Error(err)
		}
	}
}

// TestDiagEarlyGap is seed 290's search trap in fourteen operations. The
// revoke can write revision 3 in the model, but was called after an answer
// already saw that revision. Searching the eight later gaps cannot fix it.
func TestDiagEarlyGap(t *testing.T) {
	b := newBuilder(t, "a", "b", "c")
	b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 10}})
	b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
	deleted := b.del(1, "a")
	b.del(0, "b")
	b.do(0, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
	var ambiguous []int
	for i := range 8 {
		ambiguous = append(ambiguous, b.put(1, "b", strconv.Itoa(i)))
	}
	b.put(0, "c", "last")
	h := b.done()
	for _, i := range append(ambiguous, deleted) {
		h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
	}
	for _, mode := range bothModes {
		start := time.Now()
		err := Witnessed(h, mode, time.Minute, MaxSteps(10_000))
		t.Logf("%s ops=%d seconds=%.6f: %v", mode, len(h.Ops), time.Since(start).Seconds(), err)
		mustPass(t, mode.String(), err)
	}
}
