//go:build difftest

package difftest

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// proxyNamespace is the --namespace of the gRPC proxy that
// scripts/difftest.sh starts for TestProxy. The proxy puts it in front
// of each key as it is, so the case's keys, which start with a slash,
// get two.
const proxyNamespace = "/ns/"

// TestProxy runs through etcd's gRPC proxy with --namespace /ns/, which
// scripts/difftest.sh starts in front of the server, without TLS, at
// TIGERSTORE_DIFF_PROXY: etcd v3.7.2's own in front of etcd and
// `tigerstore grpc-proxy start` in front of tigerstore. The server
// itself, at TIGERSTORE_DIFF_ENDPOINT, shows where the keys went.
func TestProxy(t *testing.T) {
	direct := dial(t)
	runCasesOn(t, "etcd-v3.7.2-proxy.json", dialAt(t, "TIGERSTORE_DIFF_PROXY", ""), []testCase{
		{"namespace", func(r *run) { namespaced(r, direct) }},
	})
}

// namespaced writes, reads, watches and leases keys through the proxy and
// reads them back from the server under the namespace.
func namespaced(r *run, direct *client) {
	r.put("put a", &pb.PutRequest{Key: r.key("a"), Value: []byte("1")})
	r.get("get a", &pb.RangeRequest{Key: r.key("a")})
	r.directRange("the server has a under the namespace", direct, "a", false)
	if _, err := direct.kv.Put(r.ctx, &pb.PutRequest{Key: []byte(proxyNamespace + r.prefix + "b"), Value: []byte("2")}); err != nil {
		r.t.Fatal(err)
	}
	r.get("range of the case's keys", &pb.RangeRequest{Key: r.key(""), RangeEnd: prefixEnd(r.key(""))})
	r.txn("txn on a that puts c", &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: r.key("a"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte("1")}}},
		Success: []*pb.RequestOp{putOp(r.key("c"), "3"), {Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: r.key("b")}}}},
	})
	r.get("count every key of the namespace", &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, CountOnly: true})
	r.proxyWatch("watch the case's keys from its start", &pb.WatchCreateRequest{Key: r.key(""), RangeEnd: prefixEnd(r.key("")), StartRevision: r.abs(1)}, 3)
	id := r.grant("grant", 0, 60)
	r.put("put d with the lease", &pb.PutRequest{Key: r.key("d"), Value: []byte("4"), Lease: id})
	r.ttlKeys("time to live with keys", id)
	r.revoke("revoke", id)
	r.get("d went with the lease", &pb.RangeRequest{Key: r.key("d")})
	r.del("delete the case's keys", &pb.DeleteRangeRequest{Key: r.key(""), RangeEnd: prefixEnd(r.key("")), PrevKv: true})
	r.directRange("the server has none of them left", direct, "", true)
}

// directRange records a range of the server itself over the case's keys
// under the namespace: key k, or every key from k on with prefix.
func (r *run) directRange(do string, c *client, k string, prefix bool) {
	r.t.Helper()
	key := []byte(proxyNamespace + r.prefix + k)
	req := &pb.RangeRequest{Key: key}
	if prefix {
		req.RangeEnd = prefixEnd(key)
	}
	resp, err := c.kv.Range(r.ctx, req)
	var res any
	if err == nil {
		var keys []string
		for _, kv := range resp.Kvs {
			keys = append(keys, r.nsKey(kv.Key)+"="+string(kv.Value))
		}
		res = struct {
			Keys  []string `json:"keys,omitempty"`
			Count int64    `json:"count"`
		}{keys, resp.Count}
	}
	r.record(do, res, err)
}

// nsKey is a key of the case as recorded: the namespace and the key
// within the case, without the case's prefix, which differs by run.
func (r *run) nsKey(k []byte) string {
	if rest, ok := bytes.CutPrefix(k, []byte(proxyNamespace+r.prefix)); ok {
		return proxyNamespace + string(rest)
	}
	return strings.TrimPrefix(string(k), r.prefix)
}

// ttlKeys records a lease's TTL and keys.
func (r *run) ttlKeys(do string, id int64) {
	r.t.Helper()
	resp, err := r.c.lease.LeaseTimeToLive(r.ctx, &pb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	var res any
	if err == nil {
		var keys []string
		for _, k := range resp.Keys {
			keys = append(keys, r.nsKey(k))
		}
		res = struct {
			GrantedTTL int64    `json:"grantedTTL"`
			Keys       []string `json:"keys,omitempty"`
		}{resp.GrantedTTL, keys}
	}
	r.record(do, res, err)
}

// proxyWatch records a watch through the proxy until it delivered n
// events. The proxy does not pass progress requests on, which the watch
// step relies on to end.
func (r *run) proxyWatch(do string, req *pb.WatchCreateRequest, n int) {
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
		Type string `json:"type"`
		KV   *kv    `json:"kv"`
	}
	var res struct {
		Created bool    `json:"created"`
		Events  []event `json:"events,omitempty"`
	}
	for len(res.Events) < n {
		resp, err := stream.Recv()
		if err != nil {
			r.t.Fatalf("%s: %d events of %d: %v", do, len(res.Events), n, err)
		}
		res.Created = res.Created || resp.Created
		for _, e := range resp.Events {
			res.Events = append(res.Events, event{e.Type.String(), r.kv(e.Kv)})
		}
	}
	r.record(do, res, nil)
}

// TestGateway runs through etcd's TCP gateway, which scripts/difftest.sh
// starts in front of the server at TIGERSTORE_DIFF_GATEWAY: etcd
// v3.7.2's own in front of etcd and `tigerstore gateway start` in front
// of tigerstore. TLS passes through it to the server.
func TestGateway(t *testing.T) {
	runCasesOn(t, "etcd-v3.7.2-gateway.json", dialAt(t, "TIGERSTORE_DIFF_GATEWAY", os.Getenv("TIGERSTORE_DIFF_CA")), []testCase{{"gateway", func(r *run) {
		r.put("put a", &pb.PutRequest{Key: r.key("a"), Value: []byte("1")})
		r.get("get a", &pb.RangeRequest{Key: r.key("a")})
		r.watch("watch a from the case's start", &pb.WatchCreateRequest{Key: r.key("a"), StartRevision: r.abs(1)})
		id := r.grant("grant", 0, 60)
		r.revoke("revoke", id)
		r.del("delete a", &pb.DeleteRangeRequest{Key: r.key("a"), PrevKv: true})
	}}})
}
