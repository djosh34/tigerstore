//go:build difftest

package difftest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	epb "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// client is a raw etcd v3 gRPC client.
type client struct {
	kv    pb.KVClient
	watch pb.WatchClient
	lease pb.LeaseClient
	maint pb.MaintenanceClient
	// lock and election are etcd's Lock and Election services.
	lock     v3lockpb.LockClient
	election epb.ElectionClient
}

// dial connects to TIGERSTORE_DIFF_ENDPOINT. A missing endpoint fails the
// test: these tests only build with the difftest tag, so they never skip.
func dial(t *testing.T) *client {
	t.Helper()
	endpoint := os.Getenv("TIGERSTORE_DIFF_ENDPOINT")
	if endpoint == "" {
		t.Fatal("TIGERSTORE_DIFF_ENDPOINT is not set")
	}
	creds := insecure.NewCredentials()
	if ca := os.Getenv("TIGERSTORE_DIFF_CA"); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			t.Fatal(err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			t.Fatalf("no certificates in %s", ca)
		}
		cert, err := tls.LoadX509KeyPair(os.Getenv("TIGERSTORE_DIFF_CERT"), os.Getenv("TIGERSTORE_DIFF_KEY"))
		if err != nil {
			t.Fatal(err)
		}
		creds = credentials.NewTLS(&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint,
		grpc.WithTransportCredentials(creds),
		// Let requests above the server's limit reach the server.
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64<<20), grpc.MaxCallRecvMsgSize(64<<20)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &client{
		kv:       pb.NewKVClient(conn),
		watch:    pb.NewWatchClient(conn),
		lease:    pb.NewLeaseClient(conn),
		maint:    pb.NewMaintenanceClient(conn),
		lock:     v3lockpb.NewLockClient(conn),
		election: epb.NewElectionClient(conn),
	}
	// A fresh server may still be electing a leader.
	var err2 error
	for range 60 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err2 = c.kv.Range(ctx, &pb.RangeRequest{Key: []byte("/diff/")})
		cancel()
		if err2 == nil {
			return c
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s is not ready after 60 attempts: %v", endpoint, err2)
	return nil
}

// step is one recorded request and what came back. Revisions are relative to
// the revision at the start of the case, and lease IDs are numbered in the
// order the case granted them, so the record does not depend on what ran
// before.
type step struct {
	Do      string          `json:"do"`
	Code    string          `json:"code"`
	Message string          `json:"message,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

// run records the steps of one case.
type run struct {
	t      *testing.T
	c      *client
	ctx    context.Context
	prefix string
	base   int64
	leases map[int64]int64
	steps  []step
}

// newRun starts a case. Its keys live under a prefix of their own, made from
// tag, so the tests can run again against the same endpoint. The prefix has
// a fixed length, because the request size limit counts key bytes.
func newRun(t *testing.T, c *client, tag int64, name string) *run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	r := &run{t: t, c: c, ctx: ctx, prefix: fmt.Sprintf("/diff/%012d/%s/", tag, name), leases: map[int64]int64{}}
	resp, err := c.kv.Range(ctx, &pb.RangeRequest{Key: []byte(r.prefix)})
	if err != nil {
		t.Fatal(err)
	}
	r.base = resp.Header.Revision
	return r
}

// rebase makes the current revision the new base of relative revisions.
func (r *run) rebase() {
	r.t.Helper()
	resp, err := r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: []byte(r.prefix)})
	if err != nil {
		r.t.Fatal(err)
	}
	r.base = resp.Header.Revision
}

// key returns the full name of a key of this case.
func (r *run) key(k string) []byte { return []byte(r.prefix + k) }

// abs turns a relative revision into an absolute one.
func (r *run) abs(rel int64) int64 { return r.base + rel }

// rel turns an absolute revision into a relative one. Zero stays zero.
func (r *run) rel(rev int64) int64 {
	if rev == 0 {
		return 0
	}
	return rev - r.base
}

// lease returns the case-local number of a lease ID.
func (r *run) lease(id int64) int64 {
	if id == 0 {
		return 0
	}
	n, ok := r.leases[id]
	if !ok {
		n = int64(len(r.leases) + 1)
		r.leases[id] = n
	}
	return n
}

// record adds a step. A nil error records code OK and the result.
func (r *run) record(do string, result any, err error) {
	r.t.Helper()
	s := step{Do: do, Code: codes.OK.String()}
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			r.t.Fatalf("%s: not a gRPC status: %v", do, err)
		}
		s.Code, s.Message = st.Code().String(), st.Message()
		if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Unavailable {
			r.t.Fatalf("%s: the endpoint did not answer: %v", do, err)
		}
	} else if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			r.t.Fatal(err)
		}
		s.Result = b
	}
	r.steps = append(r.steps, s)
}

// kv is a key as recorded.
type kv struct {
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Create  int64  `json:"create"`
	Mod     int64  `json:"mod"`
	Version int64  `json:"version"`
	Lease   int64  `json:"lease,omitempty"`
}

func (r *run) kv(k *mvccpb.KeyValue) *kv {
	if k == nil {
		return nil
	}
	v := string(k.Value)
	if len(v) > 64 {
		v = fmt.Sprintf("<%d bytes>", len(v))
	}
	return &kv{Key: strings.TrimPrefix(string(k.Key), r.prefix), Value: v, Create: r.rel(k.CreateRevision), Mod: r.rel(k.ModRevision), Version: k.Version, Lease: r.lease(k.Lease)}
}

func (r *run) kvs(in []*mvccpb.KeyValue) []*kv {
	var out []*kv
	for _, k := range in {
		out = append(out, r.kv(k))
	}
	return out
}

type rangeResult struct {
	Revision int64 `json:"revision"`
	KVs      []*kv `json:"kvs,omitempty"`
	Count    int64 `json:"count"`
	More     bool  `json:"more,omitempty"`
}

func (r *run) rangeResult(resp *pb.RangeResponse) *rangeResult {
	return &rangeResult{Revision: r.rel(resp.Header.Revision), KVs: r.kvs(resp.Kvs), Count: resp.Count, More: resp.More}
}

func (r *run) get(do string, req *pb.RangeRequest) *pb.RangeResponse {
	r.t.Helper()
	resp, err := r.c.kv.Range(r.ctx, req)
	var res any
	if err == nil {
		res = r.rangeResult(resp)
	}
	r.record(do, res, err)
	return resp
}

func (r *run) put(do string, req *pb.PutRequest) *pb.PutResponse {
	r.t.Helper()
	resp, err := r.c.kv.Put(r.ctx, req)
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
			PrevKV   *kv   `json:"prevKV,omitempty"`
		}{r.rel(resp.Header.Revision), r.kv(resp.PrevKv)}
	}
	r.record(do, res, err)
	return resp
}

func (r *run) del(do string, req *pb.DeleteRangeRequest) {
	r.t.Helper()
	resp, err := r.c.kv.DeleteRange(r.ctx, req)
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
			Deleted  int64 `json:"deleted"`
			PrevKVs  []*kv `json:"prevKVs,omitempty"`
		}{r.rel(resp.Header.Revision), resp.Deleted, r.kvs(resp.PrevKvs)}
	}
	r.record(do, res, err)
}

func (r *run) txn(do string, req *pb.TxnRequest) {
	r.t.Helper()
	resp, err := r.c.kv.Txn(r.ctx, req)
	var res any
	if err == nil {
		type opResult struct {
			Range *rangeResult `json:"range,omitempty"`
			// Revision is the header revision of a put or delete
			// result, zero when the server sends no header.
			Revision int64  `json:"revision,omitempty"`
			PrevKV   *kv    `json:"prevKV,omitempty"`
			Put      bool   `json:"put,omitempty"`
			Deleted  *int64 `json:"deleted,omitempty"`
		}
		var ops []opResult
		for _, o := range resp.Responses {
			switch x := o.Response.(type) {
			case *pb.ResponseOp_ResponseRange:
				ops = append(ops, opResult{Range: r.rangeResult(x.ResponseRange)})
			case *pb.ResponseOp_ResponsePut:
				ops = append(ops, opResult{Put: true, Revision: r.rel(x.ResponsePut.GetHeader().GetRevision()), PrevKV: r.kv(x.ResponsePut.PrevKv)})
			case *pb.ResponseOp_ResponseDeleteRange:
				d := x.ResponseDeleteRange.Deleted
				ops = append(ops, opResult{Revision: r.rel(x.ResponseDeleteRange.GetHeader().GetRevision()), Deleted: &d})
			default:
				r.t.Fatalf("%s: unexpected txn response %T", do, x)
			}
		}
		res = struct {
			Revision  int64      `json:"revision"`
			Succeeded bool       `json:"succeeded"`
			Ops       []opResult `json:"ops,omitempty"`
		}{r.rel(resp.Header.Revision), resp.Succeeded, ops}
	}
	r.record(do, res, err)
}

func (r *run) compact(do string, rev int64, physical bool) {
	r.t.Helper()
	resp, err := r.c.kv.Compact(r.ctx, &pb.CompactionRequest{Revision: rev, Physical: physical})
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
		}{r.rel(resp.Header.Revision)}
	}
	r.record(do, res, err)
}

func (r *run) grant(do string, id, ttl int64) int64 {
	r.t.Helper()
	resp, err := r.c.lease.LeaseGrant(r.ctx, &pb.LeaseGrantRequest{ID: id, TTL: ttl})
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
			Lease    int64 `json:"lease"`
			// AsAsked says whether a grant with an explicit ID got
			// that ID, since lease numbers hide the ID itself.
			AsAsked bool   `json:"asAsked,omitempty"`
			TTL     int64  `json:"ttl"`
			Error   string `json:"error,omitempty"`
		}{r.rel(resp.Header.Revision), r.lease(resp.ID), id != 0 && resp.ID == id, resp.TTL, resp.Error}
	}
	r.record(do, res, err)
	if err != nil {
		return 0
	}
	return resp.ID
}

func (r *run) revoke(do string, id int64) {
	r.t.Helper()
	resp, err := r.c.lease.LeaseRevoke(r.ctx, &pb.LeaseRevokeRequest{ID: id})
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
		}{r.rel(resp.Header.Revision)}
	}
	r.record(do, res, err)
}

// watch creates one watch and records what it delivers until the server
// confirms it has sent everything: the client asks for progress every
// 100 ms, and a progress notification marks the end, since the server
// sends one only for a watch that is caught up. A watch that ends
// another way, such as a compaction cancel, ends there. Not reaching the
// end within 10 s fails the step. The events are recorded as one list,
// since how the server batches them into responses may differ. Every
// response must keep the created watch ID and have a header revision that
// does not go back and covers its events; breaks are recorded as problems.
//
// Neither etcd nor tigerstore answers a progress request while a watch
// starts above the current revision. Such a watch can deliver nothing
// until a write, so the client ends it after 500 ms without a response.
// Responses come through a channel, so the stream ending after the client
// stopped cannot change the result.
func (r *run) watch(do string, req *pb.WatchCreateRequest) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()
	stream, err := r.c.watch.Watch(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: req}}); err != nil {
		r.t.Fatal(err)
	}
	type event struct {
		Type   string `json:"type"`
		KV     *kv    `json:"kv"`
		PrevKV *kv    `json:"prevKV,omitempty"`
	}
	var res struct {
		Created         bool     `json:"created"`
		CreatedRevision int64    `json:"createdRevision,omitempty"`
		Canceled        bool     `json:"canceled,omitempty"`
		CancelRevision  int64    `json:"cancelRevision,omitempty"`
		CompactRevision int64    `json:"compactRevision,omitempty"`
		CancelReason    string   `json:"cancelReason,omitempty"`
		Events          []event  `json:"events,omitempty"`
		Problems        []string `json:"problems,omitempty"`
	}
	type recv struct {
		resp *pb.WatchResponse
		err  error
	}
	recvs := make(chan recv, 64)
	go func() {
		for {
			resp, err := stream.Recv()
			recvs <- recv{resp, err}
			if err != nil {
				return
			}
		}
	}()
	var id, header int64
	ahead := false
	// A server ignores a progress request while the watch catches up, so
	// the client asks again every 100 ms; only the answer ends the step.
	ask := time.NewTicker(100 * time.Millisecond)
	defer ask.Stop()
	for !res.Canceled {
		var quiet <-chan time.Time
		if ahead {
			quiet = time.After(500 * time.Millisecond)
		}
		var resp *pb.WatchResponse
	wait:
		for {
			select {
			case got := <-recvs:
				if got.err != nil {
					r.t.Fatalf("%s: no progress notification to end the watch: %v", do, got.err)
				}
				resp = got.resp
				break wait
			case <-ask.C:
				if res.Created && !ahead {
					if err := stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}); err != nil {
						r.t.Fatalf("%s: %v", do, err)
					}
				}
			case <-quiet:
				break wait
			}
		}
		if resp == nil {
			break
		}
		if res.Created && !resp.Created && !resp.Canceled && len(resp.Events) == 0 {
			// The progress notification: everything up to its header
			// revision was delivered. etcd gives it watch ID -1.
			break
		}
		if resp.Created {
			ahead = req.StartRevision > resp.Header.GetRevision()
		}
		if !resp.Canceled && !ahead {
			if err := stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}); err != nil {
				r.t.Fatalf("%s: %v", do, err)
			}
		}
		problem := func(format string, args ...any) {
			res.Problems = append(res.Problems, fmt.Sprintf(format, args...))
		}
		switch {
		case resp.Created && !res.Created:
			id = resp.WatchId
			res.CreatedRevision = r.rel(resp.Header.GetRevision())
		case !res.Created:
			problem("a response came before the created response")
		case resp.WatchId != id:
			problem("watch ID %d after created ID %d", resp.WatchId, id)
		}
		if resp.Canceled {
			res.CancelRevision = r.rel(resp.Header.GetRevision())
		} else {
			rev := resp.Header.GetRevision()
			if rev < header {
				problem("header revision %d after %d", r.rel(rev), r.rel(header))
			}
			header = max(header, rev)
			for _, e := range resp.Events {
				if e.Kv.ModRevision > rev {
					problem("event at %d in a response with header revision %d", r.rel(e.Kv.ModRevision), r.rel(rev))
				}
			}
		}
		res.Created = res.Created || resp.Created
		res.Canceled = res.Canceled || resp.Canceled
		if resp.CompactRevision != 0 {
			res.CompactRevision = r.rel(resp.CompactRevision)
		}
		if resp.CancelReason != "" {
			res.CancelReason = resp.CancelReason
		}
		for _, e := range resp.Events {
			res.Events = append(res.Events, event{Type: e.Type.String(), KV: r.kv(e.Kv), PrevKV: r.kv(e.PrevKv)})
		}
	}
	r.record(do, res, nil)
}

// rangeStream records a KV.RangeStream: the number of keys in each chunk,
// and the last chunk's relative header revision, count and more, which
// only the last chunk carries.
func (r *run) rangeStream(do string, req *pb.RangeRequest) {
	r.t.Helper()
	s, err := r.c.kv.RangeStream(r.ctx, req, grpc.MaxCallRecvMsgSize(64<<20))
	var chunks []int
	var last *pb.RangeResponse
	for err == nil {
		var m *pb.RangeStreamResponse
		m, err = s.Recv()
		if err == nil {
			chunks = append(chunks, len(m.RangeResponse.Kvs))
			last = m.RangeResponse
		}
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	var res any
	if err == nil && last != nil {
		res = struct {
			Chunks   []int `json:"chunks"`
			Revision int64 `json:"revision"`
			Count    int64 `json:"count"`
			More     bool  `json:"more,omitempty"`
		}{chunks, r.rel(last.Header.GetRevision()), last.Count, last.More}
	}
	r.record(do, res, err)
}
