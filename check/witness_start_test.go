package check

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessStartLeaseRevoke keeps one of the image's leases alive for
// an acked revoke after eight gaps. Its expiry is called at the fork's
// start, as are the other leases', and comes first among them: taking it
// in a gap leaves every order of the other expiries to try before the
// acked revoke fails (#867).
func TestWitnessStartLeaseRevoke(t *testing.T) {
	st := &history.Start{Rev: 100}
	for k := range 9 {
		st.Leases = append(st.Leases, int64(k+1))
		st.KVs = append(st.KVs, history.KeyValue{Key: string(rune('a' + k)), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: int64(k + 1)})
	}
	b := newBuilderFrom(t, st)
	for k := range 8 {
		b.do(k, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: int64(k + 2)}})
	}
	b.do(8, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 1}})
	h := b.done()
	// Lease 1 is revoked last, after the other eight expire. The clients
	// of the expiries got no answer, and the fork calls all of them at 0.
	for i := range 8 {
		ambiguous(h, i)
		h.Ops[i].Call, h.Ops[i].Return = 0, 0
	}
	// The image also admits an expiry of lease 1, even though this fork
	// revokes it explicitly. Put that expiry first in the history.
	h.Ops = append([]history.Op{{Client: 9, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 1}}, Err: &history.Error{Ambiguous: true}}}, h.Ops...)
	mustPass(t, "Witnessed from log", Witnessed(h, FromLog, time.Minute))
	h.Log = nil
	mustPass(t, "Validate", h.Validate())
	mustPass(t, "Witnessed", Witnessed(h, FromRevisions, time.Minute, MaxSteps(10_000)))
	w := newWitness(h, time.Now().Add(time.Minute))
	w.maxSteps = 10_000
	mustPass(t, "revision order", w.fromRevisions())
	if w.search.backs != 0 {
		t.Fatalf("went back %d times, want none", w.search.backs)
	}
	t.Logf("%d steps, %d backtracks", w.steps, w.search.backs)
}

// TestWitnessStartLeaseRegrant allows an expiry before an acked revoke
// when the image's lease ID is granted again. The revoke need only find
// the new grant alive, not the image's (#867).
func TestWitnessStartLeaseRegrant(t *testing.T) {
	for _, unacked := range []bool{false, true} {
		b := newBuilderFrom(t, startOf())
		expiry := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
		grant := b.do(2, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 5, TTL: 5}})
		b.do(3, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
		h := b.done()
		ambiguous(h, expiry)
		h.Ops[expiry].Call, h.Ops[expiry].Return = 0, 0
		if unacked {
			ambiguous(h, grant)
		}
		h.Log = nil
		mustPass(t, "Witnessed", Witnessed(h, FromRevisions, time.Minute, MaxSteps(1_000)))
	}
}

// TestWitnessStartLeaseRevokeConvicts keeps both successful revokes of
// the image's lease, with no grant between them. Protecting its lifetime
// must not make an order pass without checking both answers.
func TestWitnessStartLeaseRevokeConvicts(t *testing.T) {
	b := newBuilderFrom(t, startOf())
	i := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
	j := b.do(2, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 5}})
	h := b.done()
	h.Ops[j].Err = nil
	h.Ops[j].Resp = h.Ops[i].Resp
	err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(1_000))
	if errors.Is(err, ErrWitnessTimeout) {
		t.Fatal(err)
	}
	mustFail(t, "Witnessed", err, "")
}

// TestWitnessStartRecordedVerdicts compares every saved history with the
// revision witness's verdict on eae285b8 (before #867), without a log or
// effects. The planted history also keeps its full failure explanation.
func TestWitnessStartRecordedVerdicts(t *testing.T) {
	for _, c := range []struct{ file, want string }{
		{file: "seed-5058843.json"},
		{file: "seed-5068434.json"},
		{file: "seed-59113215.json"},
		{file: "seed-77148.json"},
		{file: "seed-888737.json"},
		{file: "witness-425.json.gz"},
		{file: "witness-control-80-201.json.gz"},
		{file: "witness-control-320-1529.json.gz"},
		{file: "planted-B01a-1.json", want: "witness check (revisions) of 257 operations: op 89 (client 0, 21337863118 to 21338251670), a range, with answer revision 1, 0 keys of 0 has no state that real time allows: it answers revision 1, but an op that returned before it was called answered 5"},
	} {
		t.Run(c.file, func(t *testing.T) {
			f, err := os.Open("testdata/" + c.file)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close() //nolint:errcheck // read only
			decoder := json.NewDecoder(f)
			if strings.HasSuffix(c.file, ".gz") {
				z, err := gzip.NewReader(f)
				if err != nil {
					t.Fatal(err)
				}
				defer z.Close() //nolint:errcheck // read only
				decoder = json.NewDecoder(z)
			}
			var h history.History
			if err := decoder.Decode(&h); err != nil {
				t.Fatal(err)
			}
			h.Log = nil
			for i := range h.Ops {
				h.Ops[i].Effect = nil
			}
			got := ""
			if err := Witnessed(&h, FromRevisions, time.Minute); err != nil {
				got = err.Error()
			}
			if got != c.want {
				t.Fatalf("verdict = %q, recorded %q", got, c.want)
			}
		})
	}
}
