package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	epb "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// observed is what an Observe stream sends, read in a goroutine.
type observed struct {
	ch  chan *epb.LeaderResponse
	err chan error
}

func observe(t *testing.T, el epb.ElectionClient, ctx context.Context, name string) *observed {
	t.Helper()
	s, err := el.Observe(ctx, &epb.LeaderRequest{Name: []byte(name)})
	if err != nil {
		t.Fatal(err)
	}
	o := &observed{ch: make(chan *epb.LeaderResponse, 16), err: make(chan error, 1)}
	go func() {
		for {
			r, err := s.Recv()
			if err != nil {
				o.err <- err
				return
			}
			o.ch <- r
		}
	}()
	return o
}

// next returns the next leader the stream sends.
func (o *observed) next(t *testing.T) *epb.LeaderResponse {
	t.Helper()
	select {
	case r := <-o.ch:
		return r
	case err := <-o.err:
		t.Fatalf("observe ended: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("observe sent nothing in 10 s")
	}
	return nil
}

// TestElection checks the Election service as etcd v3.7.2 serves it:
// the first campaign leads, later ones wait in order, Proclaim changes
// the value, Resign hands over leadership, and Observe sends each leader
// and each value.
func TestElection(t *testing.T) {
	conn := node2(t)
	el, kv, leases := epb.NewElectionClient(conn), pb.NewKVClient(conn), pb.NewLeaseClient(conn)
	ctx := context.Background()
	a, b := grant(t, leases), grant(t, leases)

	// Observe waits for the first candidate.
	obs := observe(t, el, ctx, "e")
	campA, err := el.Campaign(ctx, &epb.CampaignRequest{Name: []byte("e"), Lease: a, Value: []byte("a1")})
	if err != nil {
		t.Fatal(err)
	}
	la := campA.Leader
	if string(la.Name) != "e" || string(la.Key) != fmt.Sprintf("e/%x", a) || la.Rev != campA.Header.Revision || la.Lease != a {
		t.Fatalf("campaign a = %v", campA)
	}
	if r := obs.next(t); string(r.Kv.Value) != "a1" || r.Header.Revision != la.Rev {
		t.Fatalf("observed %v, want a1 at %d", r, la.Rev)
	}
	lead, err := el.Leader(ctx, &epb.LeaderRequest{Name: []byte("e")})
	if err != nil || string(lead.Kv.Key) != string(la.Key) || string(lead.Kv.Value) != "a1" {
		t.Fatalf("leader = %v, %v", lead, err)
	}

	campB := start(func() (*epb.CampaignResponse, error) {
		return el.Campaign(ctx, &epb.CampaignRequest{Name: []byte("e"), Lease: b, Value: []byte("b1")})
	})
	campB.waiting(t, "campaign b")

	pr, err := el.Proclaim(ctx, &epb.ProclaimRequest{Leader: la, Value: []byte("a2")})
	if err != nil {
		t.Fatal(err)
	}
	if r := obs.next(t); string(r.Kv.Value) != "a2" || r.Header.Revision != pr.Header.Revision {
		t.Fatalf("observed %v, want a2 at %d", r, pr.Header.Revision)
	}
	// A campaign with the leader's lease and another value proclaims it.
	again, err := el.Campaign(ctx, &epb.CampaignRequest{Name: []byte("e"), Lease: a, Value: []byte("a3")})
	if err != nil || again.Leader.Rev != la.Rev {
		t.Fatalf("campaign a again = %v, %v", again, err)
	}
	if r := obs.next(t); string(r.Kv.Value) != "a3" {
		t.Fatalf("observed %v, want a3", r)
	}

	res, err := el.Resign(ctx, &epb.ResignRequest{Leader: la})
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := campB.wait(t, "campaign b")
	if err != nil || string(gotB.Leader.Key) != fmt.Sprintf("e/%x", b) || gotB.Header.Revision >= res.Header.Revision {
		t.Fatalf("campaign b = %v, %v; resign at %d", gotB, err, res.Header.Revision)
	}
	if r := obs.next(t); string(r.Kv.Value) != "b1" {
		t.Fatalf("observed %v, want b1", r)
	}

	// The old leader key no longer leads.
	if _, err := el.Proclaim(ctx, &epb.ProclaimRequest{Leader: la, Value: []byte("a4")}); status.Code(err) != codes.Unknown || status.Convert(err).Message() != "election: not leader" {
		t.Fatalf("proclaim after resign: %v", err)
	}
	if _, err := el.Resign(ctx, &epb.ResignRequest{Leader: la}); err != nil {
		t.Fatalf("resign again: %v", err)
	}

	// A candidate that gives up resigns.
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := el.Campaign(short, &epb.CampaignRequest{Name: []byte("e"), Lease: a, Value: []byte("a5")}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("campaign with a deadline: %v", err)
	}
	want := string(gotB.Leader.Key)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got := keys(t, kv, "e")
		if len(got) == 1 && got[0] == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("keys after the campaign gave up: %v, want [%s]", got, want)
		}
	}
}

// TestElectionErrors checks etcd v3.7.2's errors of the Election
// service, which come with code Unknown.
func TestElectionErrors(t *testing.T) {
	conn := node2(t)
	el, kv := epb.NewElectionClient(conn), pb.NewKVClient(conn)
	ctx := context.Background()
	for _, tc := range []struct {
		name, want string
		call       func() error
	}{
		{"leader of an empty election", "election: no leader", func() error {
			_, err := el.Leader(ctx, &epb.LeaderRequest{Name: []byte("none")})
			return err
		}},
		{"proclaim without a leader", `"leader" field must be provided`, func() error {
			_, err := el.Proclaim(ctx, &epb.ProclaimRequest{Value: []byte("v")})
			return err
		}},
		{"resign without a leader", `"leader" field must be provided`, func() error {
			_, err := el.Resign(ctx, &epb.ResignRequest{})
			return err
		}},
		{"campaign with a missing lease", "etcdserver: requested lease not found", func() error {
			_, err := el.Campaign(ctx, &epb.CampaignRequest{Name: []byte("none"), Lease: 12345})
			return err
		}},
	} {
		if err := tc.call(); status.Code(err) != codes.Unknown || status.Convert(err).Message() != tc.want {
			t.Errorf("%s: %v, want Unknown %q", tc.name, err, tc.want)
		}
	}
	// Lease 0 gets a new lease of 60 s.
	camp, err := el.Campaign(ctx, &epb.CampaignRequest{Name: []byte("z"), Value: []byte("v")})
	if err != nil || camp.Leader.Lease == 0 || string(camp.Leader.Key) != fmt.Sprintf("z/%x", camp.Leader.Lease) {
		t.Fatalf("campaign without a lease = %v, %v", camp, err)
	}
	if resp, err := kv.Range(ctx, &pb.RangeRequest{Key: camp.Leader.Key}); err != nil || len(resp.Kvs) != 1 || resp.Kvs[0].Lease != camp.Leader.Lease {
		t.Fatalf("candidate key = %v, %v", resp, err)
	}
}
