//go:build difftest

package difftest

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

var recordFlag = flag.Bool("record", false, "write etcd's answers to the golden file of each test, or tigerstore's answers to its expected failures to the known file, instead of comparing")

// testCase is one named sequence of steps.
type testCase struct {
	name string
	run  func(r *run)
}

// cases run in this order, because compaction is global: each case only
// compacts above the revisions of the cases before it.
var cases = []testCase{
	{"compact-below-current", compactBelowCurrent},
	{"compact-at-current", compactAtCurrent},
	{"compact-above-current", compactAboveCurrent},
	{"compact-physical", compactPhysical},
	{"kubernetes-shapes", kubernetesShapes},
	{"error-codes", errorCodes},
	{"leases", leases},
	{"max-request-size", maxRequestSize},
	{"range-stream", rangeStream},
	{"lock", lockCase},
	{"election", electionCase},
	{"health", health},
	{"hashes", hashes},
	{"alarms", alarms},
	{"snapshot", snapshot},
	{"members", members},
	{"defragment", defragment},
	{"compression", compression},
	{"fragments", fragments},
	{"gateway", gatewayCase},
	{"metrics", metricsCase},
	// Last: after a cancel, the cluster version may stay a minor
	// version lower for up to 4 s.
	{"downgrade", downgradeCase},
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
		`{"created":true,"createdRevision":3,"events":[{"type":"PUT","kv":{"key":"a","value":"2","create":1,"mod":2,"version":2},"prevKV":{"key":"a","value":"1","create":1,"mod":1,"version":1}},{"type":"PUT","kv":{"key":"a","value":"3","create":1,"mod":3,"version":3},"prevKV":{"key":"a","value":"2","create":1,"mod":2,"version":2}}]}`,
		`{"created":true,"createdRevision":3,"canceled":true,"compactRevision":2}`,
	},
	"compact-physical/watch a from 2 with prev_kv": {
		`{"created":true,"createdRevision":3,"events":[{"type":"PUT","kv":{"key":"a","value":"2","create":1,"mod":2,"version":2},"prevKV":{"key":"a","value":"1","create":1,"mod":1,"version":1}},{"type":"PUT","kv":{"key":"a","value":"3","create":1,"mod":3,"version":3},"prevKV":{"key":"a","value":"2","create":1,"mod":2,"version":2}}]}`,
		`{"created":true,"createdRevision":3,"canceled":true,"compactRevision":2}`,
	},
	"compact-at-current/watch a from 3 with prev_kv": {
		`{"created":true,"createdRevision":3,"events":[{"type":"PUT","kv":{"key":"a","value":"3","create":1,"mod":3,"version":3},"prevKV":{"key":"a","value":"2","create":1,"mod":2,"version":2}}]}`,
		`{"created":true,"createdRevision":3,"canceled":true,"compactRevision":3}`,
	},
}

// expectedFail lists the steps where tigerstore differs from etcd v3.7.2
// today, by case, with the reason. They still run, and each must give the
// answer recorded for it in knownFile. A listed step that matches etcd fails
// the test, so the list shrinks as the core grows.
var expectedFail = map[string]struct {
	reason string
	steps  []string
}{}

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
	if !ok || srv == "etcd" {
		return sameStep(want, got)
	}
	for _, alt := range alts {
		if sameStep(step{Do: want.Do, Code: "OK", Result: json.RawMessage(alt)}, got) {
			return true
		}
	}
	return false
}

func TestBoundaries(t *testing.T) { runCases(t, "etcd-v3.7.2.json", cases) }

// runCases runs cases in order against the endpoint and compares the
// answers with etcd's in testdata/<golden>. tigerstore's answers to its
// expected failures are in testdata/<golden> with "etcd-v3.7.2" replaced
// by "tigerstore-known".
func runCases(t *testing.T, golden string, cases []testCase) {
	runCasesOn(t, golden, dial(t), cases)
}

// runCasesOn is runCases on the client c.
func runCasesOn(t *testing.T, golden string, c *client, cases []testCase) {
	srv := server(t)
	goldenFile := "testdata/" + golden
	knownFile := "testdata/" + strings.Replace(golden, "etcd-v3.7.2", "tigerstore-known", 1)
	if *recordFlag && srv == "etcd" {
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
	for _, cs := range cases {
		r := newRun(t, c, start.Header.Revision, cs.name)
		cs.run(r)
		got[cs.name] = r.steps
	}
	if *recordFlag {
		if srv == "etcd" {
			writeJSON(t, goldenFile, got)
			return
		}
		// tigerstore's answers to the steps it does not match yet.
		known := map[string]step{}
		for name, steps := range got {
			for _, s := range steps {
				if slices.Contains(expectedFail[name].steps, s.Do) {
					known[name+"/"+s.Do] = s
				}
			}
		}
		writeJSON(t, knownFile, known)
		return
	}
	want := map[string][]step{}
	readJSON(t, goldenFile, &want)
	if len(want) != len(cases) {
		t.Errorf("%s has %d cases, the test has %d", goldenFile, len(want), len(cases))
	}
	known := map[string]step{}
	if _, err := os.Stat(knownFile); err == nil {
		readJSON(t, knownFile, &known)
	}
	ours := map[string]bool{}
	for _, cs := range cases {
		ours[cs.name] = true
	}
	listed := 0
	for name, ef := range expectedFail {
		if !ours[name] {
			continue
		}
		for _, do := range ef.steps {
			listed++
			if !slices.ContainsFunc(want[name], func(s step) bool { return s.Do == do }) {
				t.Errorf("expectedFail names %s step %q, which %s does not have", name, do, goldenFile)
			}
			if _, ok := known[name+"/"+do]; !ok {
				t.Errorf("expectedFail names %s step %q, which %s does not have", name, do, knownFile)
			}
		}
	}
	if len(known) != listed {
		t.Errorf("%s has %d steps, expectedFail lists %d", knownFile, len(known), listed)
	}
	for k := range stricter {
		name, do, _ := strings.Cut(k, "/")
		if !ours[name] {
			continue
		}
		if !slices.ContainsFunc(want[name], func(s step) bool { return s.Do == do }) {
			t.Errorf("stricter names %q, which %s does not have", k, goldenFile)
		}
	}
	for _, cs := range cases {
		name := cs.name
		w, g := want[name], got[name]
		if len(w) != len(g) {
			t.Errorf("%s: %d steps, etcd v3.7.2 had %d", name, len(g), len(w))
		}
		ef := expectedFail[name]
		for i := range min(len(w), len(g)) {
			ok := allowed(srv, name, w[i], g[i])
			if srv == "tigerstore" && slices.Contains(ef.steps, w[i].Do) {
				// The step must still give the answer recorded for it,
				// so an unrelated change cannot hide behind the list.
				k := known[name+"/"+w[i].Do]
				switch {
				case ok:
					t.Errorf("%s step %d %q now matches etcd v3.7.2; remove it from expectedFail and %s", name, i, w[i].Do, knownFile)
				case !sameStep(k, g[i]):
					t.Errorf("%s step %d %q is an expected failure (%s), but its answer changed:\n got      %s\n recorded %s\n etcd     %s", name, i, g[i].Do, ef.reason, show(g[i]), show(k), show(w[i]))
				default:
					t.Logf("expected failure, %s: %s step %d %q:\n got  %s\n want %s", ef.reason, name, i, g[i].Do, show(g[i]), show(w[i]))
				}
				continue
			}
			if !ok {
				t.Errorf("%s step %d %q:\n got  %s\n want %s", name, i, g[i].Do, show(g[i]), show(w[i]))
			}
		}
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("%s: %v", path, err)
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
	r.compact("compact at 2", r.abs(2), false)
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
	r.txn("txn with a historical range at a compacted revision", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: k, Revision: r.abs(1)}}}}})
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

// maxRequestSize finds the largest value a put and a put inside a
// transaction accept, for keys of several lengths, and records the requests
// one byte below, at and above it. etcd limits the marshaled request with
// its header, so the key's length and the client's certificate identity
// (#99) both count.
func maxRequestSize(r *run) {
	for _, name := range []string{"b", "big", strings.Repeat("k", 100), strings.Repeat("k", 3000)} {
		key := r.key(name)
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
			r.put(fmt.Sprintf("put %s of the largest size, key of %d bytes", relSize(size-n), len(key)), &pb.PutRequest{Key: key, Value: bytes.Repeat([]byte("x"), size)})
		}
		t := largest(true)
		r.rebase()
		r.record(fmt.Sprintf("largest txn put value for a key of %d bytes", len(key)), t, nil)
		for _, size := range []int{t - 1, t, t + 1} {
			r.txn(fmt.Sprintf("txn put %s of the largest size, key of %d bytes", relSize(size-t), len(key)), &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: bytes.Repeat([]byte("x"), size)}}}}})
		}
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

// rangeStream checks KV.RangeStream's chunks against etcd: their sizes
// for small values, for 400 KiB values, and with the first chunk just
// below and at half of the 1.5 MiB target, where etcd doubles the next
// limit or keeps it; the header, count and more of the last chunk with a
// limit, at a past revision, count only and empty; and the errors.
func rangeStream(r *run) {
	r.rebase()
	for i := range 25 {
		r.put(fmt.Sprintf("put small key %d", i), &pb.PutRequest{Key: r.key(fmt.Sprintf("s/%02d", i)), Value: []byte("v")})
	}
	small := func(req *pb.RangeRequest) *pb.RangeRequest {
		req.Key, req.RangeEnd = r.key("s/"), r.key("s0")
		return req
	}
	r.rangeStream("stream 25 small keys", small(&pb.RangeRequest{}))
	r.rangeStream("stream with limit 12", small(&pb.RangeRequest{Limit: 12}))
	r.rangeStream("stream at a past revision", small(&pb.RangeRequest{Revision: r.abs(20)}))
	r.rangeStream("stream count only", small(&pb.RangeRequest{CountOnly: true}))
	r.rangeStream("stream serializable", small(&pb.RangeRequest{Serializable: true}))
	r.rangeStream("stream an empty range", &pb.RangeRequest{Key: r.key("x/"), RangeEnd: r.key("x0")})
	r.rangeStream("stream keys only", small(&pb.RangeRequest{KeysOnly: true}))

	big := bytes.Repeat([]byte("b"), 400<<10)
	for i := range 25 {
		r.put(fmt.Sprintf("put 400 KiB key %d", i), &pb.PutRequest{Key: r.key(fmt.Sprintf("b/%02d", i)), Value: big})
	}
	r.rangeStream("stream 25 values of 400 KiB", &pb.RangeRequest{Key: r.key("b/"), RangeEnd: r.key("b0")})

	// The next limit doubles when a chunk, as etcd sizes it inside the
	// server, is under half of 1.5 MiB (786,432 bytes).
	for _, target := range []int{786431, 786432} {
		prefix := fmt.Sprintf("t%d/", target)
		r.rebase()
		first := r.base + 1
		sizes := boundarySizes(r, prefix, first, target)
		for i, n := range sizes {
			r.put(fmt.Sprintf("put boundary key %d for %d bytes", i, target), &pb.PutRequest{Key: r.key(fmt.Sprintf("%s%02d", prefix, i)), Value: bytes.Repeat([]byte("v"), n)})
		}
		r.rangeStream(fmt.Sprintf("stream with a first chunk of %d bytes", target), &pb.RangeRequest{Key: r.key(prefix), RangeEnd: prefixEnd(r.key(prefix))})
	}

	r.rangeStream("stream sorted by key descending", small(&pb.RangeRequest{SortTarget: pb.RangeRequest_KEY, SortOrder: pb.RangeRequest_DESCEND}))
	r.rangeStream("stream sorted by value", small(&pb.RangeRequest{SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_ASCEND}))
	r.rangeStream("stream with a revision filter", small(&pb.RangeRequest{MinModRevision: 2}))
	r.rangeStream("stream with an empty key", &pb.RangeRequest{})
	r.rangeStream("stream at a future revision", small(&pb.RangeRequest{Revision: r.abs(1 << 20)}))
	r.rebase()
	r.compact("compact for streams", r.abs(0), false)
	r.rangeStream("stream below the compaction", small(&pb.RangeRequest{Revision: r.abs(-1)}))
	r.rangeStream("stream at the compaction", small(&pb.RangeRequest{Revision: r.abs(0)}))
}

// boundarySizes returns the value lengths of 30 keys under prefix, written
// from revision first on, so that the first chunk of a stream over them,
// as etcd's range answers it inside the server (the store revision as its
// only header field, 10 keys, more, and a count of the 11 keys it saw),
// has exactly target bytes.
func boundarySizes(r *run, prefix string, first int64, target int) []int {
	sizes := make([]int, 30)
	for i := range sizes {
		sizes[i] = target / 10
	}
	chunk := func() int {
		resp := &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: first + 29}, More: true, Count: 11}
		for i := range 10 {
			resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{
				Key: r.key(fmt.Sprintf("%s%02d", prefix, i)), Value: make([]byte, sizes[i]),
				CreateRevision: first + int64(i), ModRevision: first + int64(i), Version: 1,
			})
		}
		return proto.Size(resp)
	}
	for range 10 {
		d := target - chunk()
		if d == 0 {
			return sizes
		}
		sizes[9] += d
	}
	r.t.Fatalf("no value sizes give a first chunk of %d bytes", target)
	return nil
}

// health checks the gRPC health service, /readyz and /livez of a healthy
// server.
func health(r *run) {
	r.healthCheck("check the server", "")
	r.healthCheck("check a service", "etcdserverpb.KV")
	r.healthList("list")
	r.healthWatch("watch the server", "")
	r.healthWatch("watch a service", "etcdserverpb.KV")
	r.healthWatchRaw("watch with no request", 0, true)
	r.healthWatchRaw("watch with two requests", 2, true)
	r.healthWatchRaw("watch before the client ends its side", 1, false)
	r.invoke("call an unknown health method", "/grpc.health.v1.Health/Nothing")
	for _, path := range []string{
		"/readyz",
		"/readyz?verbose",
		"/readyz?exclude=linearizable_read&exclude=nothing&verbose",
		"/readyz?exclude=data_corruption&exclude=serializable_read&exclude=linearizable_read&exclude=non_learner",
		"/readyz/data_corruption?verbose",
		"/readyz/serializable_read?verbose",
		"/readyz/linearizable_read?exclude=linearizable_read&verbose",
		"/readyz/non_learner",
		"/readyz/",
		"/readyz/nothing",
		"/livez",
		"/livez?verbose",
		"/livez?exclude=serializable_read&verbose",
		"/livez/serializable_read?verbose",
		"/livez/linearizable_read",
	} {
		r.http("GET "+path, r.c.http2, http.MethodGet, path)
	}
	r.http("HEAD /readyz", r.c.http2, http.MethodHead, "/readyz")
	r.http("POST /livez", r.c.http2, http.MethodPost, "/livez")
	r.http("GET /readyz?verbose over HTTP/1.1", r.c.http1, http.MethodGet, "/readyz?verbose")
	r.http("POST /livez over HTTP/1.1", r.c.http1, http.MethodPost, "/livez")
}
