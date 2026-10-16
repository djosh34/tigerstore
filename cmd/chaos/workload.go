package main

// The recorder, request execution and workload shapes come from the
// correctness round 1 harness on #49 (experiments/correct-round1 at
// 07ed9fb7). The workload runs until the fault schedule ends instead of
// for a fixed time.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// workloadConfig shapes the traffic.
type workloadConfig struct {
	seed           uint64
	rate           int // operations per second per client
	keys           int
	valueSize      int
	requestTimeout time.Duration
	watchTimeout   time.Duration
	quiet          time.Duration
}

// dialClients makes one stock clientv3 client per endpoint, each pinned to
// its endpoint, without retries.
func dialClients(c *cluster, timeout time.Duration) ([]*clientv3.Client, error) {
	var out []*clientv3.Client
	for _, l := range c.clients {
		cl, err := clientv3.New(clientv3.Config{Endpoints: []string{l.Proxy.Addr()}, TLS: c.tls, DialTimeout: timeout, MaxUnaryRetries: 1, Logger: zap.NewNop(), DialOptions: []grpc.DialOption{grpc.WithDisableRetry()}})
		if err != nil {
			return nil, errors.Join(err, closeClients(out))
		}
		out = append(out, cl)
	}
	return out, nil
}

func closeClients(cs []*clientv3.Client) error {
	var errs []error
	for _, cl := range cs {
		errs = append(errs, cl.Close())
	}
	return errors.Join(errs...)
}

type recorder struct {
	mu          sync.Mutex
	origin      time.Time
	h           history.History
	errs        []error
	diagnostics []watchDiagnostic
}

func (r *recorder) now() int64 { return time.Since(r.origin).Nanoseconds() }
func (r *recorder) call(cl *clientv3.Client, client int, q history.Request, timeout time.Duration) history.Op {
	op := history.Op{Client: client, Call: r.now(), Req: q}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := execute(clientv3.WithRequireLeader(ctx), cl, q)
	cancel()
	op.Return = r.now()
	if err != nil {
		code := status.Code(err).String()
		if e, ok := errors.AsType[rpctypes.EtcdError](err); ok {
			code = e.Code().String()
		}
		// Every write failure stays ambiguous. This workload sends legal requests;
		// never infer abortion from a timeout, no-leader response or retry failure.
		ambiguous := true
		switch code {
		case "InvalidArgument", "OutOfRange", "NotFound", "AlreadyExists", "PermissionDenied", "Unauthenticated", "FailedPrecondition":
			ambiguous = false
		}
		op.Err = &history.Error{Code: code, Message: err.Error(), Ambiguous: ambiguous}
	} else {
		op.Resp = resp
	}
	r.mu.Lock()
	r.h.Ops = append(r.h.Ops, op)
	r.mu.Unlock()
	return op
}

func options(end string, prev bool) []clientv3.OpOption {
	var o []clientv3.OpOption
	if end != "" {
		o = append(o, clientv3.WithRange(end))
	}
	if prev {
		o = append(o, clientv3.WithPrevKV())
	}
	return o
}

func txnOp(o history.TxnOp) clientv3.Op {
	switch {
	case o.Put != nil:
		return clientv3.OpPut(o.Put.Key, o.Put.Value, options("", o.Put.PrevKV)...)
	case o.Delete != nil:
		return clientv3.OpDelete(o.Delete.Key, options(o.Delete.End, o.Delete.PrevKV)...)
	default:
		return clientv3.OpGet(o.Range.Key, options(o.Range.End, false)...)
	}
}

func execute(ctx context.Context, cl *clientv3.Client, q history.Request) (*history.Response, error) {
	switch {
	case q.Put != nil:
		r, e := cl.Put(ctx, q.Put.Key, q.Put.Value, options("", q.Put.PrevKV)...)
		if e != nil {
			return nil, e
		}
		return response((*pb.PutResponse)(r))
	case q.Delete != nil:
		r, e := cl.Delete(ctx, q.Delete.Key, options(q.Delete.End, q.Delete.PrevKV)...)
		if e != nil {
			return nil, e
		}
		return response((*pb.DeleteRangeResponse)(r))
	case q.Range != nil:
		r, e := cl.Get(ctx, q.Range.Key, options(q.Range.End, false)...)
		if e != nil {
			return nil, e
		}
		return response((*pb.RangeResponse)(r))
	case q.Txn != nil:
		var cmps []clientv3.Cmp
		for _, cmp := range q.Txn.Compare {
			var target clientv3.Cmp
			if cmp.Target == history.TargetVersion {
				target = clientv3.Version(cmp.Key)
			} else {
				target = clientv3.ModRevision(cmp.Key)
			}
			cmps = append(cmps, clientv3.Compare(target, "=", cmp.Number))
		}
		var yes, no []clientv3.Op
		for _, o := range q.Txn.Success {
			yes = append(yes, txnOp(o))
		}
		for _, o := range q.Txn.Failure {
			no = append(no, txnOp(o))
		}
		r, e := cl.Txn(ctx).If(cmps...).Then(yes...).Else(no...).Commit()
		if e != nil {
			return nil, e
		}
		return response((*pb.TxnResponse)(r))
	}
	return nil, errors.New("unsupported generated request")
}

// prefix holds every key the workload touches.
const prefix = "/chaos/"

// traffic is a running workload.
type traffic struct {
	cfg     workloadConfig
	cs      []*clientv3.Client
	r       *recorder
	workers sync.WaitGroup
	watches sync.WaitGroup
	stop    context.CancelFunc
	cancelW context.CancelFunc
}

// startTraffic checks that the cluster is fresh, then starts one watch and
// one sequential client per node until stopTraffic.
func startTraffic(cfg workloadConfig, cs []*clientv3.Client) (*traffic, error) {
	t := &traffic{cfg: cfg, cs: cs, r: &recorder{origin: time.Now()}}
	members := map[uint64]bool{}
	for _, cl := range cs {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.requestTimeout)
		g, e := cl.Get(ctx, "\x00", clientv3.WithFromKey())
		cancel()
		if e != nil {
			return nil, e
		}
		if members[g.Header.MemberId] || g.Header.MemberId == 0 {
			return nil, errors.New("duplicate or zero member ID")
		}
		members[g.Header.MemberId] = true
		if g.Count != 0 || g.Header.Revision != 1 {
			return nil, errors.New("workload requires a fresh empty revision-1 cluster")
		}
	}
	end := clientv3.GetPrefixRangeEnd(prefix)
	var watchCtx, stopCtx context.Context
	watchCtx, t.cancelW = context.WithCancel(context.Background())
	stopCtx, t.stop = context.WithCancel(context.Background())
	t.r.h.Watches = make([]history.Watch, len(cs))
	for i, cl := range cs {
		t.r.h.Watches[i] = history.Watch{Client: 100 + i, Req: history.WatchRequest{Key: prefix, End: end, StartRevision: 1, PrevKV: true, ProgressNotify: true}}
		t.watches.Go(func() { t.r.watch(watchCtx, cl, i, prefix) })
	}
	for i, cl := range cs {
		t.workers.Go(func() { t.client(stopCtx, i, cl) })
	}
	return t, nil
}

// client issues random Put, CAS Txn, DeleteRange and Range calls on hot
// keys, one at a time, at most cfg.rate per second.
func (t *traffic) client(ctx context.Context, i int, cl *clientv3.Client) {
	c, r := t.cfg, t.r
	end := clientv3.GetPrefixRangeEnd(prefix)
	rng := rand.New(rand.NewPCG(c.seed, uint64(i+1)))
	tick := time.NewTicker(time.Second / time.Duration(c.rate))
	defer tick.Stop()
	mods := map[string]int64{}
	versions := map[string]int64{}
	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		key := fmt.Sprintf("%sk%d", prefix, rng.IntN(c.keys))
		seq++
		identity := fmt.Sprintf("s%d-c%d-o%d:", c.seed, i, seq)
		value := identity + strings.Repeat("x", max(0, c.valueSize-len(identity)))
		var q history.Request
		switch rng.IntN(5) {
		case 0:
			q.Put = &history.PutRequest{Key: key, Value: value, PrevKV: true}
		case 1, 2:
			target, num := history.TargetMod, mods[key]
			if rng.IntN(2) == 0 {
				target, num = history.TargetVersion, versions[key]
			}
			q.Txn = &history.TxnRequest{Compare: []history.Compare{{Key: key, Target: target, Result: history.Equal, Number: num}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: value, PrevKV: true}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}}}
		case 3:
			q.Delete = &history.DeleteRequest{Key: key, PrevKV: true}
			if rng.IntN(3) == 0 {
				q.Delete.End = fmt.Sprintf("%sk%d", prefix, min(c.keys, int(key[len(key)-1]-'0')+2))
			}
		case 4:
			q.Range = &history.RangeRequest{Key: key}
			if rng.IntN(2) == 0 {
				q.Range.Key, q.Range.End = prefix, end
			}
		}
		op := r.call(cl, i, q, c.requestTimeout)
		if op.Resp == nil {
			continue
		}
		// Track what this client last saw, so its CAS calls often match.
		if op.Resp.Put != nil {
			mods[key] = op.Resp.Revision
			versions[key] = 1
			if p := op.Resp.Put.PrevKV; p != nil {
				versions[key] = p.Version + 1
			}
		}
		if op.Resp.Txn != nil && op.Resp.Txn.Succeeded {
			mods[key] = op.Resp.Revision
			versions[key] = 1
			if p := op.Resp.Txn.Results[0].Put.PrevKV; p != nil {
				versions[key] = p.Version + 1
			}
		}
		if op.Resp.Delete != nil {
			for _, p := range op.Resp.Delete.PrevKVs {
				mods[p.Key] = 0
				versions[p.Key] = 0
			}
		}
		if op.Resp.Range != nil {
			for _, kv := range op.Resp.Range.KVs {
				mods[kv.Key] = kv.ModRevision
				versions[kv.Key] = kv.Version
			}
		}
		if op.Resp.Txn != nil {
			for _, res := range op.Resp.Txn.Results {
				if res.Range != nil {
					if len(res.Range.KVs) == 0 {
						mods[key] = 0
						versions[key] = 0
					}
					for _, kv := range res.Range.KVs {
						mods[kv.Key] = kv.ModRevision
						versions[kv.Key] = kv.Version
					}
				}
			}
		}
	}
}

// stopClients ends the random calls.
func (t *traffic) stopClients() {
	t.stop()
	t.workers.Wait()
}

// finish runs on the healed cluster: it waits out calls that timed out
// on the client but may still commit, makes the commit index pass the
// last write, reads the final state on every node as the target, and
// waits until every watch reports progress through the target.
func (t *traffic) finish() (err error) {
	c, r, cs := t.cfg, t.r, t.cs
	// Give requests whose clients timed out longer than the server's 7s
	// timeout. The offline replay must still end exactly at the target.
	time.Sleep(c.quiet)
	// A failed CAS appends a Raft entry without a KV revision; later
	// entries move the persisted commit index past the last KV mutation.
	for i, cl := range cs {
		q := history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: prefix + "k0", Target: history.TargetVersion, Result: history.Equal, Number: -1}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: prefix + "k0", Value: fmt.Sprintf("barrier-%d", i), PrevKV: true}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: prefix + "k0"}}}}}
		barrier := r.call(cl, 20+i, q, c.requestTimeout)
		if barrier.Resp == nil || barrier.Resp.Txn.Succeeded {
			err = errors.Join(err, fmt.Errorf("terminal no-revision barrier failed on endpoint %d: %v", i, barrier.Err))
		}
	}
	var terminal history.Op
	for i, cl := range cs {
		observed := r.call(cl, 10+i, history.Request{Range: &history.RangeRequest{Key: "\x00", End: history.AllKeys}}, c.requestTimeout)
		if observed.Resp == nil {
			err = errors.Join(err, fmt.Errorf("terminal linearizable read failed on endpoint %d: %v", i, observed.Err))
			continue
		}
		if terminal.Resp == nil {
			terminal = observed
		} else if !reflect.DeepEqual(terminal.Resp, observed.Resp) {
			err = errors.Join(err, errors.New("terminal endpoint reads disagree"))
		}
	}
	if terminal.Resp != nil && err == nil {
		r.h.Target = terminal.Resp.Revision
	}
	finish := time.Now().Add(c.watchTimeout)
	for err == nil {
		complete := true
		r.mu.Lock()
		for _, w := range r.h.Watches {
			covered := false
			for _, wr := range w.Responses {
				if wr.Progress && wr.Revision >= r.h.Target {
					covered = true
				}
			}
			complete = complete && covered
		}
		r.mu.Unlock()
		if complete {
			break
		}
		if !time.Now().Before(finish) {
			err = errors.New("watches did not report progress through terminal target")
			break
		}
		for i, cl := range cs {
			ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(context.Background()), max(time.Millisecond, min(c.requestTimeout, time.Until(finish))))
			e := cl.RequestProgress(ctx)
			cancel()
			if e != nil {
				r.mu.Lock()
				r.diagnostics = append(r.diagnostics, watchDiagnostic{Client: 100 + i, Response: history.WatchResponse{Time: r.now()}, Error: "progress request: " + e.Error()})
				if !transientWatch(e) {
					r.errs = append(r.errs, e)
				}
				r.mu.Unlock()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.cancelW()
	t.watches.Wait()
	r.mu.Lock()
	err = errors.Join(err, errors.Join(r.errs...))
	r.mu.Unlock()
	return err
}
