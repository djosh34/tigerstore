//go:build difftest

package difftest

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

var recordFlag = flag.Bool("record", false, "write the endpoint's answers to "+goldenFile+" instead of comparing")

const goldenFile = "testdata/etcd-v3.7.2.json"

// cases run in this order, because compaction is global: each case only
// compacts above the revisions of the cases before it.
var cases = []struct {
	name string
	run  func(r *run)
}{
	{"compact-below-current", compactBelowCurrent},
	{"compact-at-current", compactAtCurrent},
	{"compact-above-current", compactAboveCurrent},
	{"compact-physical", compactPhysical},
	{"kubernetes-shapes", kubernetesShapes},
	{"error-codes", errorCodes},
	{"leases", leases},
	{"max-request-size", maxRequestSize},
}

// stricter lists the steps where tigerstore must not copy etcd v3.7.2. After
// a compaction at C, etcd sends the update at C without prev_kv, because the
// version before C is compacted. kube-apiserver rejects a non-create event
// without its previous value, so tigerstore must send the previous value or
// cancel the watch as compacted at C. These answers are the only ones allowed
// from tigerstore. etcd's own answer is allowed only when
// TIGERSTORE_DIFF_SERVER is etcd.
var stricter = map[string][]string{
	"compact-below-current/watch a from 2 with prev_kv": {
		`{"created":true,"events":[{"type":"PUT","kv":{"key":"a","value":"2","create":1,"mod":2,"version":2},"prevKV":{"key":"a","value":"1","create":1,"mod":1,"version":1}},{"type":"PUT","kv":{"key":"a","value":"3","create":1,"mod":3,"version":3},"prevKV":{"key":"a","value":"2","create":1,"mod":2,"version":2}}]}`,
		`{"created":true,"canceled":true,"compactRevision":2}`,
	},
	"compact-physical/watch a from 2 with prev_kv": {
		`{"created":true,"events":[{"type":"PUT","kv":{"key":"a","value":"2","create":1,"mod":2,"version":2},"prevKV":{"key":"a","value":"1","create":1,"mod":1,"version":1}},{"type":"PUT","kv":{"key":"a","value":"3","create":1,"mod":3,"version":3},"prevKV":{"key":"a","value":"2","create":1,"mod":2,"version":2}}]}`,
		`{"created":true,"canceled":true,"compactRevision":2}`,
	},
	"compact-at-current/watch a from 3 with prev_kv": {
		`{"created":true,"events":[{"type":"PUT","kv":{"key":"a","value":"3","create":1,"mod":3,"version":3},"prevKV":{"key":"a","value":"2","create":1,"mod":2,"version":2}}]}`,
		`{"created":true,"canceled":true,"compactRevision":3}`,
	},
}

const (
	issueCompaction = "Compact is not built yet, https://github.com/djosh34/tigerstore/issues/47"
	issueLeases     = "leases are not built yet, https://github.com/djosh34/tigerstore/issues/47"
	issueSize       = "the request size limit is looser, https://github.com/djosh34/tigerstore/issues/62"
)

// expectedFail lists the steps where tigerstore differs from etcd v3.7.2
// today, by case, with the reason. They still run. A listed step that
// matches etcd fails the test, so the list shrinks as the core grows.
var expectedFail = map[string]struct {
	reason string
	steps  []string
}{
	"compact-below-current": {issueCompaction, []string{"compact at R-1", "range a at 1", "watch a from 1 with prev_kv", "compact at R-1 again", "compact at R-2"}},
	"compact-at-current":    {issueCompaction, []string{"compact at R", "range a at 1", "range a at 2", "watch a from 1 with prev_kv", "watch a from 2 with prev_kv", "compact at R again"}},
	"compact-above-current": {issueCompaction, []string{"compact at R+1"}},
	"compact-physical":      {issueCompaction, []string{"physical compact at R-1", "range a at 1", "watch a from 1 with prev_kv"}},
	"error-codes":           {issueCompaction, []string{"txn with a historical range at a compacted revision", "compact at revision 0", "compact at revision -1"}},
	"leases": {issueLeases, []string{
		"grant with an explicit ID", "grant the same ID again", "grant with a TTL of 1 s", "grant with a TTL of 9000000001 s",
		"grant with a TTL of 0", "grant with a negative ID", "put k with the lease", "put j with the lease", "put j without a lease",
		"put ignore_lease on k", "range keys", "revoke the lease", "range keys after the revoke", "revoke the lease again",
		"grant a lease for no keys", "revoke a lease without keys", "revoke lease 0",
	}},
	"max-request-size": {issueSize, []string{"largest put value for a key of 39 bytes", "largest txn put value for a key of 39 bytes"}},
}

// server returns TIGERSTORE_DIFF_SERVER, which says what the endpoint is.
func server(t *testing.T) string {
	t.Helper()
	s := os.Getenv("TIGERSTORE_DIFF_SERVER")
	if s != "etcd" && s != "tigerstore" {
		t.Fatalf("TIGERSTORE_DIFF_SERVER is %q, want etcd or tigerstore", s)
	}
	return s
}

// allowed reports whether got is a right answer for the step that etcd
// v3.7.2 answered with want.
func allowed(srv, name string, want, got step) bool {
	alts, ok := stricter[name+"/"+want.Do]
	if !ok || srv == "etcd" && sameStep(want, got) {
		return sameStep(want, got)
	}
	for _, alt := range alts {
		if sameStep(step{Do: want.Do, Code: "OK", Result: json.RawMessage(alt)}, got) {
			return true
		}
	}
	return false
}

func TestBoundaries(t *testing.T) {
	srv := server(t)
	c := dial(t)
	if *recordFlag {
		st, err := c.maint.Status(t.Context(), &pb.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if st.Version != "3.7.2" {
			t.Fatalf("recording needs etcd 3.7.2, the endpoint is %s", st.Version)
		}
	}
	start, err := c.kv.Range(t.Context(), &pb.RangeRequest{Key: []byte("/diff/")})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]step{}
	var order []string
	for _, cs := range cases {
		r := newRun(t, c, start.Header.Revision, cs.name)
		cs.run(r)
		got[cs.name] = r.steps
		order = append(order, cs.name)
	}
	if *recordFlag {
		var buf bytes.Buffer
		buf.WriteString("{\n")
		for i, name := range order {
			b, err := json.MarshalIndent(got[name], "  ", "  ")
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&buf, "  %q: %s", name, b)
			if i < len(order)-1 {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString("}\n")
		if err := os.WriteFile(goldenFile, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]step{}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(cases) {
		t.Errorf("%s has %d cases, the test has %d", goldenFile, len(want), len(cases))
	}
	for name, known := range expectedFail {
		for _, do := range known.steps {
			if !slices.ContainsFunc(want[name], func(s step) bool { return s.Do == do }) {
				t.Errorf("expectedFail names %s step %q, which %s does not have", name, do, goldenFile)
			}
		}
	}
	for k := range stricter {
		name, do, _ := strings.Cut(k, "/")
		if !slices.ContainsFunc(want[name], func(s step) bool { return s.Do == do }) {
			t.Errorf("stricter names %q, which %s does not have", k, goldenFile)
		}
	}
	for _, name := range order {
		w, g := want[name], got[name]
		if len(w) != len(g) {
			t.Errorf("%s: %d steps, etcd v3.7.2 had %d", name, len(g), len(w))
		}
		known := expectedFail[name]
		for i := range min(len(w), len(g)) {
			ok := allowed(srv, name, w[i], g[i])
			if srv == "tigerstore" && slices.Contains(known.steps, w[i].Do) {
				if ok {
					t.Errorf("%s step %d %q now matches etcd v3.7.2; remove it from expectedFail", name, i, w[i].Do)
				} else {
					t.Logf("expected failure, %s: %s step %d %q:\n got  %s\n want %s", known.reason, name, i, g[i].Do, show(g[i]), show(w[i]))
				}
				continue
			}
			if !ok {
				t.Errorf("%s step %d %q:\n got  %s\n want %s", name, i, g[i].Do, show(g[i]), show(w[i]))
			}
		}
	}
}

func sameStep(a, b step) bool {
	var ra, rb bytes.Buffer
	if len(a.Result) > 0 {
		if err := json.Compact(&ra, a.Result); err != nil {
			return false
		}
	}
	if len(b.Result) > 0 {
		if err := json.Compact(&rb, b.Result); err != nil {
			return false
		}
	}
	return a.Do == b.Do && a.Code == b.Code && a.Message == b.Message && bytes.Equal(ra.Bytes(), rb.Bytes())
}

func show(s step) string {
	b, err := json.Marshal(s)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// threeVersions writes key a three times, at relative revisions 1, 2 and 3.
func threeVersions(r *run) {
	for i := 1; i <= 3; i++ {
		r.put(fmt.Sprintf("put a=%d", i), &pb.PutRequest{Key: r.key("a"), Value: fmt.Appendf(nil, "%d", i)})
	}
}

// readsAround reads key a at relative revisions 1 to 4 and watches it from
// 1, 2 and 3, with previous values.
func readsAround(r *run) {
	for rev := int64(1); rev <= 4; rev++ {
		r.get(fmt.Sprintf("range a at %d", rev), &pb.RangeRequest{Key: r.key("a"), Revision: r.abs(rev)})
	}
	for rev := int64(1); rev <= 3; rev++ {
		r.watch(fmt.Sprintf("watch a from %d with prev_kv", rev), &pb.WatchCreateRequest{Key: r.key("a"), StartRevision: r.abs(rev), PrevKv: true})
	}
}

// compactBelowCurrent compacts at R-1, R being the current revision.
func compactBelowCurrent(r *run) {
	threeVersions(r)
	r.compact("compact at R-1", r.abs(2), false)
	readsAround(r)
	r.compact("compact at R-1 again", r.abs(2), false)
	r.compact("compact at R-2", r.abs(1), false)
}

// compactAtCurrent compacts at R.
func compactAtCurrent(r *run) {
	threeVersions(r)
	r.compact("compact at R", r.abs(3), false)
	readsAround(r)
	r.compact("compact at R again", r.abs(3), false)
}

// compactAboveCurrent compacts at R+1, which fails and compacts nothing.
func compactAboveCurrent(r *run) {
	threeVersions(r)
	r.compact("compact at R+1", r.abs(4), false)
	readsAround(r)
}

func compactPhysical(r *run) {
	threeVersions(r)
	r.compact("physical compact at R-1", r.abs(2), true)
	readsAround(r)
}

// prefixEnd is the end of the range of keys that start with prefix, as
// clientv3.GetPrefixRangeEnd computes it.
func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}

func modEquals(key []byte, rev int64) *pb.Compare {
	return &pb.Compare{Key: key, Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}
}

func putOp(key []byte, value string) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: []byte(value)}}}
}

func getOp(key []byte) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}
}

func deleteOp(key []byte) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: key}}}
}

// kubernetesShapes are the requests kube-apiserver sends.
func kubernetesShapes(r *run) {
	a, b, c := r.key("pods/a"), r.key("pods/b"), r.key("pods/c")
	pods := r.key("pods/")
	r.txn("create a: mod(a) = 0", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(a, 0)}, Success: []*pb.RequestOp{putOp(a, "a1")}, Failure: []*pb.RequestOp{getOp(a)}})
	r.txn("create a again", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(a, 0)}, Success: []*pb.RequestOp{putOp(a, "a2")}, Failure: []*pb.RequestOp{getOp(a)}})
	r.txn("update a: mod(a) = 1", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(a, r.abs(1))}, Success: []*pb.RequestOp{putOp(a, "a2")}, Failure: []*pb.RequestOp{getOp(a)}})
	r.txn("stale update a: mod(a) = 1", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(a, r.abs(1))}, Success: []*pb.RequestOp{putOp(a, "a3")}, Failure: []*pb.RequestOp{getOp(a)}})
	r.txn("create b", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(b, 0)}, Success: []*pb.RequestOp{putOp(b, "b1")}, Failure: []*pb.RequestOp{getOp(b)}})
	r.txn("create c", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(c, 0)}, Success: []*pb.RequestOp{putOp(c, "c1")}, Failure: []*pb.RequestOp{getOp(c)}})
	r.txn("delete b: mod(b) = 3", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(b, r.abs(3))}, Success: []*pb.RequestOp{deleteOp(b)}, Failure: []*pb.RequestOp{getOp(b)}})
	r.txn("delete b again", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(b, r.abs(3))}, Success: []*pb.RequestOp{deleteOp(b)}, Failure: []*pb.RequestOp{getOp(b)}})
	r.del("delete missing key", &pb.DeleteRangeRequest{Key: r.key("missing")})
	r.get("list pods", &pb.RangeRequest{Key: pods, RangeEnd: prefixEnd(pods)})
	r.get("list pods, limit 1", &pb.RangeRequest{Key: pods, RangeEnd: prefixEnd(pods), Limit: 1})
	r.get("list pods at 3, limit 1", &pb.RangeRequest{Key: pods, RangeEnd: prefixEnd(pods), Limit: 1, Revision: r.abs(3)})
	r.get("list pods at 3 from b", &pb.RangeRequest{Key: b, RangeEnd: prefixEnd(pods), Revision: r.abs(3)})
	r.get("count pods", &pb.RangeRequest{Key: pods, RangeEnd: prefixEnd(pods), CountOnly: true})
	r.get("count pods, limit 1", &pb.RangeRequest{Key: pods, RangeEnd: prefixEnd(pods), CountOnly: true, Limit: 1})
	r.get("list pod keys", &pb.RangeRequest{Key: pods, RangeEnd: prefixEnd(pods), KeysOnly: true})
	r.get("serializable get a", &pb.RangeRequest{Key: a, Serializable: true})
	r.watch("watch pods from 1 with prev_kv", &pb.WatchCreateRequest{Key: pods, RangeEnd: prefixEnd(pods), StartRevision: r.abs(1), PrevKv: true})
	r.watch("watch pods from 1 without prev_kv", &pb.WatchCreateRequest{Key: pods, RangeEnd: prefixEnd(pods), StartRevision: r.abs(1)})
	r.watch("watch pods from a future revision", &pb.WatchCreateRequest{Key: pods, RangeEnd: prefixEnd(pods), StartRevision: r.abs(100)})
	// The compactor's lock: put compact_rev_key if its version is still
	// the one it read.
	lock := r.key("compact_rev_key")
	version := func(v int64) *pb.Compare {
		return &pb.Compare{Key: lock, Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: v}}
	}
	r.txn("compactor: version(lock) = 0", &pb.TxnRequest{Compare: []*pb.Compare{version(0)}, Success: []*pb.RequestOp{putOp(lock, "5")}, Failure: []*pb.RequestOp{getOp(lock)}})
	r.txn("compactor: version(lock) = 0 again", &pb.TxnRequest{Compare: []*pb.Compare{version(0)}, Success: []*pb.RequestOp{putOp(lock, "6")}, Failure: []*pb.RequestOp{getOp(lock)}})
	r.txn("compactor: version(lock) = 1", &pb.TxnRequest{Compare: []*pb.Compare{version(1)}, Success: []*pb.RequestOp{putOp(lock, "6")}, Failure: []*pb.RequestOp{getOp(lock)}})
	r.txn("read-only txn", &pb.TxnRequest{Success: []*pb.RequestOp{getOp(a), getOp(c)}})
	r.txn("txn writing two keys", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(a, "a4"), putOp(c, "c2"), getOp(a)}})
	r.put("put a with prev_kv", &pb.PutRequest{Key: a, Value: []byte("a5"), PrevKv: true})
	r.del("delete pods with prev_kv", &pb.DeleteRangeRequest{Key: pods, RangeEnd: prefixEnd(pods), PrevKv: true})
}

// errorCodes are requests etcd rejects, and their neighbours it accepts.
func errorCodes(r *run) {
	k := r.key("k")
	r.put("put k", &pb.PutRequest{Key: k, Value: []byte("v")})
	r.put("put empty key", &pb.PutRequest{Value: []byte("v")})
	r.get("range empty key", &pb.RangeRequest{})
	r.del("delete empty key", &pb.DeleteRangeRequest{})
	r.get("range at a future revision", &pb.RangeRequest{Key: k, Revision: r.abs(100)})
	r.get("range at revision -1", &pb.RangeRequest{Key: k, Revision: -1})
	r.get("range with limit -1", &pb.RangeRequest{Key: k, Limit: -1})
	r.get("range with an end before the key", &pb.RangeRequest{Key: r.key("z"), RangeEnd: r.key("a")})
	r.put("put with a missing lease", &pb.PutRequest{Key: k, Value: []byte("v"), Lease: 0x7a11})
	r.put("put ignore_value on a missing key", &pb.PutRequest{Key: r.key("missing"), IgnoreValue: true})
	r.put("put ignore_lease on a missing key", &pb.PutRequest{Key: r.key("missing"), IgnoreLease: true})
	r.put("put ignore_value with a value", &pb.PutRequest{Key: k, Value: []byte("v"), IgnoreValue: true})
	r.put("put ignore_value on k", &pb.PutRequest{Key: k, IgnoreValue: true})
	r.txn("txn putting one key twice", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(k, "1"), putOp(k, "2")}})
	r.txn("txn putting and deleting one key", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(k, "1"), deleteOp(k)}})
	r.txn("txn reading one key twice", &pb.TxnRequest{Success: []*pb.RequestOp{getOp(k), getOp(k)}})
	many := func(n int) []*pb.RequestOp {
		ops := make([]*pb.RequestOp, n)
		for i := range ops {
			ops[i] = putOp(r.key(fmt.Sprintf("many/%03d", i)), "x")
		}
		return ops
	}
	r.txn("txn with 128 operations", &pb.TxnRequest{Success: many(128)})
	r.txn("txn with 129 operations", &pb.TxnRequest{Success: many(129)})
	r.txn("txn with 129 operations in the branch not taken", &pb.TxnRequest{Compare: []*pb.Compare{modEquals(k, 0)}, Success: many(129)})
	r.txn("txn with a historical range at a compacted revision", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: k, Revision: 1}}}}})
	r.compact("compact at revision 0", 0, false)
	r.compact("compact at revision -1", -1, false)
}

func leases(r *run) {
	// Explicit lease IDs must be new on every run of the endpoint.
	id := r.base*1000 + 1
	r.grant("grant with an explicit ID", id, 60)
	r.grant("grant the same ID again", id, 60)
	r.grant("grant with a TTL of 1 s", 0, 1)
	r.grant("grant with a TTL of 9000000001 s", 0, 9000000001)
	r.grant("grant with a TTL of 0", 0, 0)
	r.grant("grant with a negative ID", -id, 60)
	r.put("put k with the lease", &pb.PutRequest{Key: r.key("k"), Value: []byte("v"), Lease: id})
	r.put("put j with the lease", &pb.PutRequest{Key: r.key("j"), Value: []byte("v"), Lease: id})
	r.put("put j without a lease", &pb.PutRequest{Key: r.key("j"), Value: []byte("w")})
	r.put("put ignore_lease on k", &pb.PutRequest{Key: r.key("k"), Value: []byte("w"), IgnoreLease: true})
	r.get("range keys", &pb.RangeRequest{Key: r.key(""), RangeEnd: prefixEnd(r.key(""))})
	r.revoke("revoke the lease", id)
	r.get("range keys after the revoke", &pb.RangeRequest{Key: r.key(""), RangeEnd: prefixEnd(r.key(""))})
	r.revoke("revoke the lease again", id)
	empty := r.grant("grant a lease for no keys", 0, 60)
	r.revoke("revoke a lease without keys", empty)
	r.revoke("revoke lease 0", 0)
}

// maxRequestSize finds the largest value a put and a txn put accept for one
// key, and records the requests one byte below, at and above it.
func maxRequestSize(r *run) {
	key := r.key("big")
	accepts := func(n int, txn bool) bool {
		value := bytes.Repeat([]byte("x"), n)
		var err error
		if txn {
			_, err = r.c.kv.Txn(r.ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: value}}}}})
		} else {
			_, err = r.c.kv.Put(r.ctx, &pb.PutRequest{Key: key, Value: value})
		}
		if err != nil && !strings.Contains(err.Error(), "too large") && !strings.Contains(err.Error(), "larger than max") {
			r.t.Fatalf("put of %d bytes: %v", n, err)
		}
		return err == nil
	}
	largest := func(txn bool) int {
		lo, hi := 0, 4<<20
		if !accepts(lo, txn) || accepts(hi, txn) {
			r.t.Fatalf("the limit is not between %d and %d bytes", lo, hi)
		}
		for hi-lo > 1 {
			mid := (lo + hi) / 2
			if accepts(mid, txn) {
				lo = mid
			} else {
				hi = mid
			}
		}
		return lo
	}
	n := largest(false)
	// The probes wrote revisions; count from here.
	r.rebase()
	r.record(fmt.Sprintf("largest put value for a key of %d bytes", len(key)), n, nil)
	for _, size := range []int{n - 1, n, n + 1} {
		r.put(fmt.Sprintf("put %s of the largest size", relSize(size-n)), &pb.PutRequest{Key: key, Value: bytes.Repeat([]byte("x"), size)})
	}
	t := largest(true)
	r.rebase()
	r.record(fmt.Sprintf("largest txn put value for a key of %d bytes", len(key)), t, nil)
	for _, size := range []int{t - 1, t, t + 1} {
		r.txn(fmt.Sprintf("txn put %s of the largest size", relSize(size-t)), &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: bytes.Repeat([]byte("x"), size)}}}}})
	}
}

func relSize(d int) string {
	switch {
	case d < 0:
		return "one byte below"
	case d > 0:
		return "one byte above"
	}
	return "exactly"
}
