//go:build difftest

package difftest

import (
	"context"
	"strconv"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	epb "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc/status"
)

// The Lock and Election services name a caller's key by its lease in hex,
// as in m/694d9a8c3b2e0f11. Lease IDs differ between servers and runs,
// so the record shows the lease's number in the case instead:
// m/<lease 1>.

// sessionKey is a key of the Lock or Election service as recorded.
func (r *run) sessionKey(key []byte) string {
	k := strings.TrimPrefix(string(key), r.prefix)
	i := strings.LastIndexByte(k, '/')
	id, err := strconv.ParseInt(k[i+1:], 16, 64)
	if i < 0 || err != nil {
		return k
	}
	return k[:i+1] + "<lease " + strconv.FormatInt(r.lease(id), 10) + ">"
}

// sessionKV is a key of the Lock or Election service, with its value.
func (r *run) sessionKV(k *mvccpb.KeyValue) *kv {
	out := r.kv(k)
	if out != nil {
		out.Key = r.sessionKey(k.Key)
	}
	return out
}

// waiting is a call that may wait, running in a goroutine.
type waiting[T any] struct {
	done chan struct{}
	resp T
	err  error
}

// begin starts a call that should wait, and records whether it still
// waits after 300 ms.
func begin[T any](r *run, do string, fn func(ctx context.Context) (T, error)) *waiting[T] {
	w := &waiting[T]{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		w.resp, w.err = fn(r.ctx)
	}()
	var res struct {
		Waits bool `json:"waits"`
	}
	select {
	case <-w.done:
	case <-time.After(300 * time.Millisecond):
		res.Waits = true
	}
	r.record(do, res, nil)
	return w
}

// end returns the answer of a call begin started, failing after 10 s.
func (w *waiting[T]) end(r *run, do string) (T, error) {
	r.t.Helper()
	select {
	case <-w.done:
	case <-time.After(10 * time.Second):
		r.t.Fatalf("%s: no answer in 10 s", do)
	}
	return w.resp, w.err
}

// deadline records the code of a call that runs out of its 300 ms
// deadline while it waits.
func (r *run) deadline(do string, fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(r.ctx, 300*time.Millisecond)
	defer cancel()
	r.record(do, struct {
		Code string `json:"code"`
	}{status.Code(fn(ctx)).String()}, nil)
}

// sessionKeys records the keys under name/, oldest first. A server may
// still be deleting the key of a call that just failed, so it reads
// again for up to 5 s until there are want keys.
func (r *run) sessionKeys(do, name string, want int) {
	r.t.Helper()
	prefix := r.key(name + "/")
	var resp *pb.RangeResponse
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		var err error
		resp, err = r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: prefix, RangeEnd: prefixEnd(prefix), SortTarget: pb.RangeRequest_CREATE, SortOrder: pb.RangeRequest_ASCEND})
		if err != nil {
			r.t.Fatal(err)
		}
		if len(resp.Kvs) == want || time.Now().After(deadline) {
			break
		}
	}
	var res []*kv
	for _, k := range resp.Kvs {
		res = append(res, r.sessionKV(k))
	}
	r.record(do, res, nil)
}

// timeToLive records the granted TTL of a lease and its keys. The TTL
// left depends on timing, so it is not recorded.
func (r *run) timeToLive(do string, id int64) {
	r.t.Helper()
	resp, err := r.c.lease.LeaseTimeToLive(r.ctx, &pb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	var res any
	if err == nil {
		var ks []string
		for _, k := range resp.Keys {
			ks = append(ks, r.sessionKey(k))
		}
		res = struct {
			GrantedTTL int64    `json:"grantedTTL"`
			Keys       []string `json:"keys,omitempty"`
		}{resp.GrantedTTL, ks}
	}
	r.record(do, res, err)
}

func (r *run) recordLock(do string, resp *v3lockpb.LockResponse, err error) {
	r.t.Helper()
	var res any
	if err == nil {
		res = struct {
			Revision int64  `json:"revision"`
			Key      string `json:"key"`
		}{r.rel(resp.Header.Revision), r.sessionKey(resp.Key)}
	}
	r.record(do, res, err)
}

func (r *run) lock(do string, req *v3lockpb.LockRequest) *v3lockpb.LockResponse {
	r.t.Helper()
	resp, err := r.c.lock.Lock(r.ctx, req)
	r.recordLock(do, resp, err)
	return resp
}

func (r *run) unlock(do string, key []byte) {
	r.t.Helper()
	resp, err := r.c.lock.Unlock(r.ctx, &v3lockpb.UnlockRequest{Key: key})
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
		}{r.rel(resp.Header.Revision)}
	}
	r.record(do, res, err)
}

// leaderKey is a LeaderKey as recorded.
type leaderKey struct {
	Name  string `json:"name"`
	Key   string `json:"key"`
	Rev   int64  `json:"rev"`
	Lease int64  `json:"lease"`
}

func (r *run) recordCampaign(do string, resp *epb.CampaignResponse, err error) {
	r.t.Helper()
	var res any
	if err == nil {
		l := resp.Leader
		res = struct {
			Revision int64     `json:"revision"`
			Leader   leaderKey `json:"leader"`
		}{r.rel(resp.Header.Revision), leaderKey{strings.TrimPrefix(string(l.Name), r.prefix), r.sessionKey(l.Key), r.rel(l.Rev), r.lease(l.Lease)}}
	}
	r.record(do, res, err)
}

func (r *run) campaign(do string, req *epb.CampaignRequest) *epb.CampaignResponse {
	r.t.Helper()
	resp, err := r.c.election.Campaign(r.ctx, req)
	r.recordCampaign(do, resp, err)
	return resp
}

func (r *run) proclaim(do string, req *epb.ProclaimRequest) {
	r.t.Helper()
	resp, err := r.c.election.Proclaim(r.ctx, req)
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
		}{r.rel(resp.Header.Revision)}
	}
	r.record(do, res, err)
}

func (r *run) resign(do string, req *epb.ResignRequest) {
	r.t.Helper()
	resp, err := r.c.election.Resign(r.ctx, req)
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
		}{r.rel(resp.Header.Revision)}
	}
	r.record(do, res, err)
}

// leaderResult is a LeaderResponse as recorded.
type leaderResult struct {
	Revision int64 `json:"revision"`
	KV       *kv   `json:"kv"`
}

func (r *run) leader(do, name string) {
	r.t.Helper()
	resp, err := r.c.election.Leader(r.ctx, &epb.LeaderRequest{Name: r.key(name)})
	var res any
	if err == nil {
		res = leaderResult{r.rel(resp.Header.Revision), r.sessionKV(resp.Kv)}
	}
	r.record(do, res, err)
}

// observer is an Observe stream, read in a goroutine.
type observer struct {
	ch  chan *epb.LeaderResponse
	err chan error
}

func (r *run) observe(name string) *observer {
	r.t.Helper()
	ctx, cancel := context.WithCancel(r.ctx)
	r.t.Cleanup(cancel)
	s, err := r.c.election.Observe(ctx, &epb.LeaderRequest{Name: r.key(name)})
	if err != nil {
		r.t.Fatal(err)
	}
	o := &observer{ch: make(chan *epb.LeaderResponse, 64), err: make(chan error, 1)}
	go func() {
		for {
			resp, err := s.Recv()
			if err != nil {
				o.err <- err
				return
			}
			o.ch <- resp
		}
	}()
	return o
}

// observed records the next n responses of an Observe stream. A stream
// that ends or sends nothing for 10 s ends the list, and a response
// beyond the n that comes within 300 ms is recorded too.
func (r *run) observed(do string, o *observer, n int) {
	r.t.Helper()
	var res struct {
		Leaders []leaderResult `json:"leaders"`
		Code    string         `json:"code,omitempty"`
	}
	for {
		wait := 10 * time.Second
		if len(res.Leaders) >= n {
			wait = 300 * time.Millisecond
		}
		select {
		case resp := <-o.ch:
			res.Leaders = append(res.Leaders, leaderResult{r.rel(resp.Header.Revision), r.sessionKV(resp.Kv)})
			continue
		case err := <-o.err:
			res.Code = status.Code(err).String()
		case <-time.After(wait):
		}
		break
	}
	r.record(do, res, nil)
}

// lockCase records Lock and Unlock: the holder's key, waiters served in
// order, a lease revoke that hands the lock on, a waiter that gives up,
// a waiter whose key goes while it waits, and etcd's errors.
func lockCase(r *run) {
	a := r.grant("grant a", 0, 60)
	b := r.grant("grant b", 0, 60)
	c := r.grant("grant c", 0, 60)
	m := r.key("m")
	held := r.lock("lock m with a", &v3lockpb.LockRequest{Name: m, Lease: a})
	r.lock("lock m with a again", &v3lockpb.LockRequest{Name: m, Lease: a})
	lockB := begin(r, "lock m with b waits", func(ctx context.Context) (*v3lockpb.LockResponse, error) {
		return r.c.lock.Lock(ctx, &v3lockpb.LockRequest{Name: m, Lease: b})
	})
	lockC := begin(r, "lock m with c waits", func(ctx context.Context) (*v3lockpb.LockResponse, error) {
		return r.c.lock.Lock(ctx, &v3lockpb.LockRequest{Name: m, Lease: c})
	})
	r.sessionKeys("keys of m", "m", 3)
	if held != nil {
		r.unlock("unlock a", held.Key)
	}
	gotB, err := lockB.end(r, "lock m with b")
	r.recordLock("lock m with b", gotB, err)
	r.revoke("revoke b", b)
	gotC, err := lockC.end(r, "lock m with c")
	r.recordLock("lock m with c", gotC, err)

	d := r.grant("grant d", 0, 60)
	r.deadline("lock m with d and a deadline", func(ctx context.Context) error {
		_, err := r.c.lock.Lock(ctx, &v3lockpb.LockRequest{Name: m, Lease: d})
		return err
	})
	r.sessionKeys("keys of m after d gave up", "m", 1)

	e := r.grant("grant e", 0, 60)
	lockE := begin(r, "lock m with e waits", func(ctx context.Context) (*v3lockpb.LockResponse, error) {
		return r.c.lock.Lock(ctx, &v3lockpb.LockRequest{Name: m, Lease: e})
	})
	r.revoke("revoke e", e)
	if gotC != nil {
		r.unlock("unlock c", gotC.Key)
	}
	gotE, err := lockE.end(r, "lock m with e")
	r.recordLock("lock m with e after its lease was revoked", gotE, err)

	n := r.lock("lock n without a lease", &v3lockpb.LockRequest{Name: r.key("n")})
	if n != nil {
		lease, err := strconv.ParseInt(strings.TrimPrefix(string(n.Key), string(r.key("n/"))), 16, 64)
		if err != nil {
			r.t.Fatalf("lock key %q: %v", n.Key, err)
		}
		r.timeToLive("lease of the lock of n", lease)
	}
	r.lock("lock with a missing lease", &v3lockpb.LockRequest{Name: m, Lease: 0x7a11})
	r.unlock("unlock without a key", nil)
	r.unlock("unlock a key that is not a lock", r.key("other"))
}

// electionCase records the Election service: the first candidate leads,
// Observe follows each leader and value, later candidates wait in order,
// Proclaim and Resign by the leader key, a candidate that gives up, and
// etcd's errors.
func electionCase(r *run) {
	a := r.grant("grant a", 0, 60)
	b := r.grant("grant b", 0, 60)
	e := r.key("e")
	r.leader("leader of e before a campaign", "e")
	campA := r.campaign("campaign e with a", &epb.CampaignRequest{Name: e, Lease: a, Value: []byte("a1")})
	obs := r.observe("e")
	r.observed("observe e", obs, 1)
	r.leader("leader of e", "e")
	campB := begin(r, "campaign e with b waits", func(ctx context.Context) (*epb.CampaignResponse, error) {
		return r.c.election.Campaign(ctx, &epb.CampaignRequest{Name: e, Lease: b, Value: []byte("b1")})
	})
	var la *epb.LeaderKey
	if campA != nil {
		la = campA.Leader
	}
	r.proclaim("proclaim a2", &epb.ProclaimRequest{Leader: la, Value: []byte("a2")})
	r.campaign("campaign e with a and another value", &epb.CampaignRequest{Name: e, Lease: a, Value: []byte("a3")})
	r.campaign("campaign e with a and the same value", &epb.CampaignRequest{Name: e, Lease: a, Value: []byte("a3")})
	r.resign("resign a", &epb.ResignRequest{Leader: la})
	gotB, err := campB.end(r, "campaign e with b")
	r.recordCampaign("campaign e with b", gotB, err)
	r.observed("observe e after the changes", obs, 3)
	r.leader("leader of e after a resigned", "e")
	r.proclaim("proclaim with a's old leader key", &epb.ProclaimRequest{Leader: la, Value: []byte("a4")})
	r.resign("resign with a's old leader key", &epb.ResignRequest{Leader: la})
	r.proclaim("proclaim without a leader key", &epb.ProclaimRequest{Value: []byte("v")})
	r.resign("resign without a leader key", &epb.ResignRequest{})

	r.deadline("campaign e with a and a deadline", func(ctx context.Context) error {
		_, err := r.c.election.Campaign(ctx, &epb.CampaignRequest{Name: e, Lease: a, Value: []byte("a5")})
		return err
	})
	r.sessionKeys("keys of e after a gave up", "e", 1)
	if gotB != nil {
		r.resign("resign b", &epb.ResignRequest{Leader: gotB.Leader})
	}
	r.leader("leader of e after b resigned", "e")
	r.observed("observe e after b resigned", obs, 0)

	// Observe of an election without candidates waits for the first.
	obsF := r.observe("f")
	r.observed("observe f before a campaign", obsF, 0)
	r.campaign("campaign f without a lease", &epb.CampaignRequest{Name: r.key("f"), Value: []byte("f1")})
	r.observed("observe f", obsF, 1)
	r.leader("leader of f", "f")
	r.campaign("campaign with a missing lease", &epb.CampaignRequest{Name: r.key("g"), Lease: 0x7a11, Value: []byte("g")})
}
