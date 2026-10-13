package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func clients(c config) ([]*clientv3.Client, error) {
	if c.requestTimeout <= 0 {
		return nil, errors.New("request-timeout must be positive")
	}
	cert, err := tls.LoadX509KeyPair(c.cert, c.key)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(c.ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid CA")
	}
	var out []*clientv3.Client
	seen := map[string]bool{}
	for _, ep := range csv(c.endpoints) {
		if seen[ep] {
			return nil, errors.New("duplicate pinned endpoint")
		}
		seen[ep] = true
	}
	for _, ep := range csv(c.endpoints) {
		cl, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, TLS: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, DialTimeout: c.requestTimeout, MaxUnaryRetries: 1, DialOptions: []grpc.DialOption{grpc.WithDisableRetry()}})
		if err != nil {
			for _, cl := range out {
				err = errors.Join(err, cl.Close())
			}
			return nil, err
		}
		out = append(out, cl)
	}
	if len(out) == 0 {
		return nil, errors.New("no endpoints")
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

type nodeStatus struct {
	Endpoint string `json:"endpoint"`
	MemberID string `json:"member_id"`
	LeaderID string `json:"leader_id"`
	Revision int64  `json:"revision"`
	Ready    bool   `json:"ready"`
	Error    string `json:"error,omitempty"`
}

func networkStatus(c config) (err error) {
	cs, err := clients(c)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeClients(cs)) }()
	var nodes []nodeStatus
	var errs []error
	overall, stopStatus := context.WithTimeout(context.Background(), 2*c.requestTimeout)
	defer stopStatus()
	for i, cl := range cs {
		n := nodeStatus{Endpoint: csv(c.endpoints)[i]}
		ctx, cancel := context.WithTimeout(overall, c.requestTimeout)
		st, e := cl.Status(ctx, n.Endpoint)
		cancel()
		if e == nil {
			n.MemberID = strconv.FormatUint(st.Header.MemberId, 10)
			n.LeaderID = strconv.FormatUint(st.Leader, 10)
			n.Revision = st.Header.Revision
			if st.Leader == 0 || len(st.Errors) > 0 {
				e = fmt.Errorf("not ready: leader=%d errors=%v", st.Leader, st.Errors)
			}
		}
		if e == nil {
			ctx, cancel = context.WithTimeout(overall, c.requestTimeout)
			_, e = cl.Get(clientv3.WithRequireLeader(ctx), "\x00", clientv3.WithFromKey())
			cancel()
		}
		n.Ready = e == nil
		if e != nil {
			n.Error = e.Error()
			errs = append(errs, e)
		}
		nodes = append(nodes, n)
	}
	b, e := json.Marshal(struct {
		Nodes []nodeStatus `json:"nodes"`
	}{nodes})
	if e != nil {
		return e
	}
	fmt.Println(string(b))
	return errors.Join(errs...)
}

type receipt struct {
	Seed      uint64                 `json:"seed"`
	Endpoints []string               `json:"endpoints"`
	Target    int64                  `json:"target"`
	Range     *history.RangeResponse `json:"range"`
	Ops       int                    `json:"ops"`
	Failure   string                 `json:"failure,omitempty"`
	History   string                 `json:"history"`
}

func verify(c config) (err error) {
	var want receipt
	if err = load(c.receipt, &want); err != nil {
		return err
	}
	if want.Target <= 0 || want.Range == nil || want.Failure != "" {
		return errors.New("receipt has no successful terminal state")
	}
	cs, err := clients(c)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeClients(cs)) }()
	var nodes []nodeStatus
	var errs []error
	for i, cl := range cs {
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		r, e := cl.Get(clientv3.WithRequireLeader(ctx), "\x00", clientv3.WithFromKey())
		cancel()
		n := nodeStatus{Endpoint: csv(c.endpoints)[i]}
		if e == nil {
			n.Revision = r.Header.Revision
			if r.Header.Revision != want.Target || !reflect.DeepEqual(rangeResponse((*pb.RangeResponse)(r)), want.Range) {
				e = fmt.Errorf("terminal state mismatch: got revision %d want %d", r.Header.Revision, want.Target)
			}
		}
		n.Ready = e == nil
		if e != nil {
			n.Error = e.Error()
			errs = append(errs, e)
		}
		nodes = append(nodes, n)
	}
	if c.out != "" {
		errs = append(errs, save(c.out, struct {
			Nodes []nodeStatus `json:"nodes"`
		}{nodes}))
	}
	b, e := json.Marshal(nodes)
	if e != nil {
		return e
	}
	fmt.Println(string(b))
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
		if e, ok := err.(rpctypes.EtcdError); ok {
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

func workload(c config) (err error) {
	if c.clients != 3 || len(csv(c.endpoints)) != 3 || c.rate < 1 || c.rate > 50 || c.keys < 6 || c.keys > 8 || c.duration <= 0 || c.watchTimeout <= 0 || c.quiet < 0 || c.valueSize < 1 || c.valueSize > 1536*1024 {
		return errors.New("require 3 clients/endpoints, rate 1..50, keys 6..8 and positive duration/watch-timeout")
	}
	cs, err := clients(c)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeClients(cs)) }()
	r := &recorder{origin: time.Now()}
	rc := receipt{Seed: c.seed, Endpoints: csv(c.endpoints), History: filepath.Join(c.out, "history.json")}
	// Save evidence on every normal error path, including terminal/watch failure.
	defer func() {
		rc.Ops = len(r.h.Ops)
		if err != nil {
			rc.Failure = err.Error()
		}
		err = errors.Join(err, save(rc.History, &r.h), save(filepath.Join(c.out, "receipt.json"), rc), save(filepath.Join(c.out, "watch-responses.json"), r.diagnostics))
	}()
	members := map[uint64]bool{}
	for _, cl := range cs {
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		g, e := cl.Get(ctx, "\x00", clientv3.WithFromKey())
		cancel()
		if e != nil {
			return e
		}
		if members[g.Header.MemberId] || g.Header.MemberId == 0 {
			return errors.New("duplicate or zero member ID")
		}
		members[g.Header.MemberId] = true
		if g.Count != 0 || g.Header.Revision != 1 {
			return errors.New("workload requires a fresh empty revision-1 cluster")
		}
	}
	prefix := "/correct-round1/"
	end := clientv3.GetPrefixRangeEnd(prefix)
	watchCtx, cancelWatches := context.WithCancel(context.Background())
	defer cancelWatches()
	var watchWG sync.WaitGroup
	r.h.Watches = make([]history.Watch, 3)
	for i, cl := range cs {
		r.h.Watches[i] = history.Watch{Client: 100 + i, Req: history.WatchRequest{Key: prefix, End: end, StartRevision: 1, PrevKV: true, ProgressNotify: true}}
		watchWG.Add(1)
		go func(i int, cl *clientv3.Client) { defer watchWG.Done(); r.watch(watchCtx, cl, i, prefix) }(i, cl)
	}
	extraBytes := 0
	if c.nearLimit {
		extraBytes, err = probeLimit(r, cs, c, prefix)
		if err != nil {
			cancelWatches()
			watchWG.Wait()
			return err
		}
	}
	stopCtx, stop := context.WithTimeout(context.Background(), c.duration)
	defer stop()
	var workers sync.WaitGroup
	for i, cl := range cs {
		workers.Add(1)
		go func(i int, cl *clientv3.Client) {
			defer workers.Done()
			rng := rand.New(rand.NewPCG(c.seed, uint64(i+1)))
			tick := time.NewTicker(time.Second / time.Duration(c.rate))
			defer tick.Stop()
			mods := map[string]int64{}
			versions := map[string]int64{}
			seq := 0
			for {
				select {
				case <-stopCtx.Done():
					return
				case <-tick.C:
				}
				if c.stop != "" {
					if _, e := os.Stat(c.stop); e == nil {
						return
					} else if !errors.Is(e, os.ErrNotExist) {
						r.mu.Lock()
						r.errs = append(r.errs, e)
						r.mu.Unlock()
						return
					}
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
				if c.nearLimit {
					fitLimit(&q, identity, extraBytes)
				}
				op := r.call(cl, i, q, c.requestTimeout)
				if op.Resp != nil {
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
		}(i, cl)
	}
	workers.Wait()
	// Give requests whose clients timed out longer than the server's 7s timeout.
	// The offline replay must still end exactly at the captured target.
	time.Sleep(c.quiet)
	// A successful write-shaped failed CAS appends a Raft entry without a KV
	// revision. Later barriers advance persisted HardState.Commit past the last
	// KV mutation. Every attempt remains part of the checked client history.
	for i, cl := range cs {
		q := history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: prefix + "k0", Target: history.TargetVersion, Result: history.Equal, Number: -1}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: prefix + "k0", Value: fmt.Sprintf("barrier-%d", i), PrevKV: true}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: prefix + "k0"}}}}}
		barrier := r.call(cl, 20+i, q, c.requestTimeout)
		if barrier.Resp == nil || barrier.Resp.Txn.Succeeded {
			err = errors.Join(err, fmt.Errorf("terminal no-revision barrier failed on endpoint %d", i))
		}
	}
	var terminal history.Op
	for i, cl := range cs {
		observed := r.call(cl, 10+i, history.Request{Range: &history.RangeRequest{Key: "\x00", End: history.AllKeys}}, c.requestTimeout)
		if observed.Resp == nil {
			err = errors.Join(err, fmt.Errorf("terminal linearizable read failed on endpoint %d", i))
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
		rc.Target = r.h.Target
		rc.Range = terminal.Resp.Range
	}
	finish := time.Now().Add(c.watchTimeout)
	if err == nil {
		for {
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
	}
	cancelWatches()
	watchWG.Wait()
	r.mu.Lock()
	err = errors.Join(err, errors.Join(r.errs...))
	r.mu.Unlock()
	return err
}
