package wl

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

type putTimingLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *putTimingLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *putTimingLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestLargePutTimingsIncludeAdmissionAndRaftStages(t *testing.T) {
	var out putTimingLog
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) {
			c.TraceLargePuts = true
			c.Logger = log.New(&out, "", 0)
		}
	})
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	value := strings.Repeat("v", putTimingBytes)
	if _, err := c.Put(ctx, "timed-put", value); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Txn(ctx).Then(clientv3.OpPut("timed-txn", value)).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := pb.NewKVClient(c.ActiveConnection()).Put(ctx, &pb.PutRequest{Key: []byte("refused-put"), Value: make([]byte, maxRequestBytes)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("large put refusal: %v", err)
	}
	if _, err := pb.NewKVClient(c.ActiveConnection()).Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("refused-txn"), Value: make([]byte, maxRequestBytes)}}}}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("large txn refusal: %v", err)
	}
	// The gRPC size refusal is before the body is taken in. Its declared
	// size must still get an ID and reply, without inventing Raft stages.
	if _, err := pb.NewKVClient(c.ActiveConnection()).Put(ctx, &pb.PutRequest{Key: []byte("grpc-refused"), Value: make([]byte, 3<<20)}, grpc.MaxCallSendMsgSize(4<<20)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("gRPC size refusal: %v", err)
	}
	if _, err := pb.NewKVClient(c.ActiveConnection()).Put(ctx, &pb.PutRequest{Key: []byte("compressed-put"), Value: []byte(value)}, grpc.UseCompressor(grpcgzip.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := pb.NewKVClient(c.ActiveConnection()).Put(ctx, &pb.PutRequest{Key: []byte("compressed-refused"), Value: make([]byte, 3<<20)}, grpc.UseCompressor(grpcgzip.Name), grpc.MaxCallSendMsgSize(4<<20)); status.Code(err) != codes.Internal {
		t.Fatalf("decompression size refusal: %v", err)
	}
	groups := timingGroups(t, out.text())
	if len(groups) != 7 {
		t.Fatalf("%d timed requests, want 7:\n%s", len(groups), out.text())
	}
	accepted, refused, compressed := 0, 0, 0
	for _, events := range groups {
		stages := make([]string, 0, len(events))
		var previous int64
		for _, event := range events {
			stages = append(stages, event["stage"])
			ns, err := strconv.ParseInt(event["elapsed_ns"], 10, 64)
			if err != nil || ns < previous {
				t.Fatalf("elapsed times not monotonic: %v", events)
			}
			previous = ns
			if !strings.HasPrefix(event["request"], "1/") {
				t.Fatalf("request logged on a replica other than its receiver: %v", event)
			}
		}
		last := events[len(events)-1]
		wire, _ := strconv.Atoi(last["bytes"])
		decoded, _ := strconv.Atoi(last["decoded_bytes"])
		if wire < putTimingBytes {
			compressed++
			if decoded < putTimingBytes || (last["code"] == "0") != (last["decoded_complete"] == "true") {
				t.Fatalf("compressed request size/completeness not recorded: %v", last)
			}
		}
		if last["stage"] != "reply_queued" {
			t.Fatalf("request did not end with its reply: %v", stages)
		}
		if last["code"] == "0" {
			accepted++
			want := "receive,received,batched,propose_begin,proposed,commit,apply_begin,apply,reply_queued"
			if strings.Join(stages, ",") != want || last["batch"] == "0" || last["index"] == "0" {
				t.Fatalf("accepted request stages %v, want %s with batch/index", stages, want)
			}
		} else {
			refused++
			if strings.Contains(strings.Join(stages, ","), "commit") || last["batch"] != "0" || last["index"] != "0" {
				t.Fatalf("refused request invented a Raft stage: %v", events)
			}
		}
	}
	if accepted != 3 || refused != 4 || compressed != 2 {
		t.Fatalf("accepted/refused/compressed %d/%d/%d, want 3/4/2", accepted, refused, compressed)
	}
	if strings.Contains(out.text(), "timed-put") || strings.Contains(out.text(), "timed-txn") {
		t.Fatal("diagnostics logged a key")
	}
}

func timingGroups(t *testing.T, text string) map[string][]map[string]string {
	t.Helper()
	groups := map[string][]map[string]string{}
	for line := range strings.SplitSeq(text, "\n") {
		if !strings.HasPrefix(line, "large-put ") {
			continue
		}
		event := map[string]string{}
		for _, f := range strings.Fields(line)[1:] {
			k, v, ok := strings.Cut(f, "=")
			if !ok {
				t.Fatalf("malformed timing field %q", f)
			}
			event[k] = v
		}
		groups[event["request"]] = append(groups[event["request"]], event)
	}
	return groups
}

// Admission is independent of the clock: small calls and the default
// production configuration allocate no trace. A lost request's timing
// survives its closed stream so a late commit/apply remains attributable.
func TestLargePutTimingGateAndLateApply(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	s.kind = kPut
	if allocs := testing.AllocsPerRun(1, func() { n.tracePutStart(s, putTimingBytes) }); allocs != 0 {
		t.Fatalf("disabled diagnostics allocated %g times", allocs)
	}
	if s.putTiming != nil {
		t.Fatal("production default traced a request")
	}
	n.putTimings = &putTimings{pending: map[uint64][]*putTiming{}}
	if allocs := testing.AllocsPerRun(1, func() { n.tracePutStart(s, putTimingBytes-1) }); allocs != 0 {
		t.Fatalf("small-request diagnostics allocated %g times", allocs)
	}
	if s.putTiming != nil {
		t.Fatal("small request traced")
	}
	n.tracePutStart(s, putTimingBytes)
	trace := s.putTiming
	if trace == nil {
		t.Fatal("large request not traced")
	}
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	seq := uint64(7)
	n.tracePutAdd(mkHandle(c, s.id), seq, 0)
	c.endStream(s)
	if s.putTiming != nil || n.putTimings.pending[seq][0] != trace {
		t.Fatal("closed stream kept its pointer or lost its batch diagnostics")
	}
	data := make([]byte, batchHdr)
	copy(data[17:], magic)
	binary.LittleEndian.PutUint32(data, uint32(n.id))
	binary.LittleEndian.PutUint64(data[4:], seq)
	n.tracePutBatch(data, "commit", 23)
	n.tracePutApply(n.putBatchTimings(data), 0)
	n.tracePutApplied()
	if !trace.committed || !trace.applied || trace.index != 23 {
		t.Fatalf("late stages missing: %+v", trace)
	}
	n.forgetPutTimings(seq)
	if len(n.putTimings.pending) != 0 {
		t.Fatal("completed diagnostics retained")
	}
	// Uncommitted requests are bounded by forward-slot reuse, too.
	n.putTimings.pending[seq] = []*putTiming{trace}
	n.forgetPutTimings(seq)
	if len(n.putTimings.pending) != 0 {
		t.Fatal("overwritten forward slot retained diagnostics")
	}
}
