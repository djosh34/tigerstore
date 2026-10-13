package core

import (
	"errors"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

func (s *single) ok(id uint64, op proto.Message) proto.Message {
	s.t.Helper()
	r := s.do(id, op)
	if r.Err != nil {
		s.t.Fatalf("request %d: %v", id, r.Err)
	}
	return r.Resp
}

func (s *single) keys() string {
	s.t.Helper()
	r, ok := s.ok(1000+uint64(s.n.now), &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}).(*pb.RangeResponse)
	if !ok {
		s.t.Fatal("not a range response")
	}
	var b strings.Builder
	for _, kv := range r.Kvs {
		b.WriteString(string(kv.Key) + " ")
	}
	return b.String()
}

func TestLeaseRules(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	g, _ := s.ok(1, &pb.LeaseGrantRequest{ID: 7, TTL: 60}).(*pb.LeaseGrantResponse)
	if g.ID != 7 || g.TTL != 60 || g.Header.Revision != 1 {
		t.Fatalf("grant = %v", g)
	}
	if r := s.do(2, &pb.LeaseGrantRequest{ID: 7, TTL: 60}); !errors.Is(r.Err, rpctypes.ErrGRPCLeaseExist) {
		t.Fatalf("grant of a taken ID: %v", r.Err)
	}
	if r := s.do(3, &pb.LeaseGrantRequest{TTL: 9_000_000_001}); !errors.Is(r.Err, rpctypes.ErrGRPCLeaseTTLTooLarge) {
		t.Fatalf("grant of a huge TTL: %v", r.Err)
	}
	auto, _ := s.ok(4, &pb.LeaseGrantRequest{TTL: 0}).(*pb.LeaseGrantResponse)
	if auto.ID <= 0 || auto.TTL != 2 {
		t.Fatalf("grant with ID 0 and TTL 0 = %v", auto)
	}
	if r := s.do(5, &pb.LeaseGrantRequest{ID: -3, TTL: 60}); r.Err != nil {
		t.Fatalf("grant of a negative ID: %v", r.Err)
	}

	s.ok(6, &pb.PutRequest{Key: []byte("k"), Value: []byte("v"), Lease: 7})
	s.ok(7, &pb.PutRequest{Key: []byte("j"), Value: []byte("v"), Lease: 7})
	s.ok(8, &pb.PutRequest{Key: []byte("j"), Value: []byte("w")}) // detaches j
	s.ok(9, &pb.PutRequest{Key: []byte("k"), Value: []byte("w"), IgnoreLease: true})
	ttl, _ := s.ok(10, &pb.LeaseTimeToLiveRequest{ID: 7, Keys: true}).(*pb.LeaseTimeToLiveResponse)
	if len(ttl.Keys) != 1 || string(ttl.Keys[0]) != "k" || ttl.GrantedTTL != 60 || ttl.TTL > 60 || ttl.TTL < 59 {
		t.Fatalf("time to live = %v", ttl)
	}
	before := s.n.kv.Rev()
	rv, _ := s.ok(11, &pb.LeaseRevokeRequest{ID: 7}).(*pb.LeaseRevokeResponse)
	if rv.Header.Revision != before+1 || s.keys() != "j " {
		t.Fatalf("revoke = %v, keys %q", rv, s.keys())
	}
	if r := s.do(12, &pb.LeaseRevokeRequest{ID: 7}); !errors.Is(r.Err, rpctypes.ErrGRPCLeaseNotFound) {
		t.Fatalf("revoke again: %v", r.Err)
	}
	if r := s.do(13, &pb.LeaseRevokeRequest{ID: 0}); !errors.Is(r.Err, rpctypes.ErrGRPCLeaseNotFound) {
		t.Fatalf("revoke of lease 0: %v", r.Err)
	}
	rv, _ = s.ok(14, &pb.LeaseRevokeRequest{ID: -3}).(*pb.LeaseRevokeResponse)
	if rv.Header.Revision != s.n.kv.Rev() || s.n.kv.Rev() != before+1 {
		t.Fatalf("revoke of an empty lease moved the revision: %v", rv)
	}
	if missing, _ := s.ok(15, &pb.LeaseTimeToLiveRequest{ID: 7}).(*pb.LeaseTimeToLiveResponse); missing.TTL != -1 {
		t.Fatalf("time to live of a missing lease = %v", missing)
	}
	if ka, _ := s.ok(16, &pb.LeaseKeepAliveRequest{ID: 7}).(*pb.LeaseKeepAliveResponse); ka.TTL != 0 {
		t.Fatalf("keepalive of a missing lease = %v", ka)
	}
}

// TestLeaseExpiry lets two keys' lease expire on the leader: both go at
// one revision. A keepalive pushes the expiry back.
func TestLeaseExpiry(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.ok(1, &pb.LeaseGrantRequest{ID: 1, TTL: 3})
	s.ok(2, &pb.PutRequest{Key: []byte("a"), Value: []byte("1"), Lease: 1})
	s.ok(3, &pb.PutRequest{Key: []byte("b"), Value: []byte("1"), Lease: 1})
	rev := s.n.kv.Rev()
	tick := func(n int) {
		for range n {
			s.n.Tick()
			s.run()
		}
	}
	tick(20)
	s.ok(4, &pb.LeaseKeepAliveRequest{ID: 1})
	tick(25)
	if s.keys() != "a b " {
		t.Fatalf("keys 2.5 s after a keepalive of a 3 s lease = %q", s.keys())
	}
	tick(10)
	if s.keys() != "" || s.n.kv.Rev() != rev+1 {
		t.Fatalf("after expiry: keys %q, revision %d, want %d", s.keys(), s.n.kv.Rev(), rev+1)
	}
	if leases, _ := s.ok(5, &pb.LeaseLeasesRequest{}).(*pb.LeaseLeasesResponse); len(leases.Leases) != 0 {
		t.Fatalf("leases after expiry = %v", leases)
	}
}

// TestKeepAliveBeforeExpiryKeepsTheLease puts a keepalive and the leader's
// expiry decision into one batch: the keepalive applies first, answers
// with the full TTL, and the expiry then no longer applies.
func TestKeepAliveBeforeExpiryKeepsTheLease(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.ok(1, &pb.LeaseGrantRequest{ID: 1, TTL: 2})
	s.ok(2, &pb.PutRequest{Key: []byte("a"), Value: []byte("1"), Lease: 1})
	for range 19 {
		s.n.Tick()
		s.run()
	}
	s.n.Submit(Request{ID: 3, Op: &pb.LeaseKeepAliveRequest{ID: 1}})
	s.n.Tick()
	var ka *pb.LeaseKeepAliveResponse
	for _, r := range s.run() {
		if r.ID == 3 {
			ka, _ = r.Resp.(*pb.LeaseKeepAliveResponse)
		}
	}
	if ka == nil || ka.TTL != 2 {
		t.Fatalf("keepalive = %v", ka)
	}
	if s.keys() != "a " || len(s.n.Leases()) != 1 {
		t.Fatalf("after a keepalive answered with TTL 2: keys %q, leases %v", s.keys(), s.n.Leases())
	}
}
