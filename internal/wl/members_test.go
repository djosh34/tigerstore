package wl

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdMember = etcdserverpb.Member

// waitMembers waits until the member list through c holds want members,
// each with a name and client URLs.
func waitMembers(t *testing.T, c *clientv3.Client, want int) *clientv3.MemberListResponse {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := c.MemberList(ctx)
		cancel()
		if err == nil && len(r.Members) == want && !slices.ContainsFunc(r.Members, func(m *etcdMember) bool { return m.Name == "" || len(m.ClientURLs) == 0 }) {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("the member list is %v (%v), want %d members with names and client URLs", r, err, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The members change through Raft: each node publishes its client URLs;
// a learner is added, joins the running cluster, catches up and is
// promoted; a member is updated and removed, with etcd's errors where a
// change is refused; and the list comes back after a restart.
func TestMembersChangeThroughRaft(t *testing.T) {
	smallCheckpoints(t)
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r := waitMembers(t, c, 3)
	for i, m := range r.Members {
		if m.ID != uint64(i+1) || m.ClientURLs[0] != "https://"+tc.endpoints[i] || m.PeerURLs[0] != tc.peers[i] || m.IsLearner {
			t.Fatalf("member %d is %v", i, m)
		}
	}
	for i := range 200 {
		if _, err := c.Put(ctx, "/before-join", string(rune('a'+i%26))); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := c.MemberAdd(ctx, []string{tc.peers[0]}); !errors.Is(err, rpctypes.ErrPeerURLExist) {
		t.Fatalf("a used peer URL added: %v", err)
	}
	if _, err := c.MemberRemove(ctx, 99); !errors.Is(err, rpctypes.ErrMemberNotFound) {
		t.Fatalf("an unknown member removed: %v", err)
	}
	if _, err := c.MemberPromote(ctx, 1); err == nil {
		t.Fatal("a voter promoted")
	}

	// A learner joins: it is added, started with the list, and catches up.
	// It is added through the leader, and a follower's list, which is
	// linearizable, has it at once.
	leader := tc.rawClient(t, tc.leaderEndpoint(t, c))
	follower := tc.rawClient(t, tc.followerEndpoint(t, c))
	i := tc.addPorts(t, 0, true)
	added, err := leader.MemberAddAsLearner(ctx, []string{tc.peers[i]})
	if err != nil {
		t.Fatal(err)
	}
	if !added.Member.IsLearner || len(added.Members) != 4 {
		t.Fatalf("MemberAdd answered %v", added)
	}
	if fl, err := follower.MemberList(ctx); err != nil || len(fl.Members) != 4 {
		t.Fatalf("a follower's list right after the add: %v, %v", fl, err)
	}
	if _, err := c.MemberAddAsLearner(ctx, []string{"https://127.0.0.1:1"}); !errors.Is(err, rpctypes.ErrTooManyLearners) {
		t.Fatalf("a second learner added: %v", err)
	}
	// It learns the members and the cluster from a member, as
	// cmd/tigerstore does for a node that joins.
	ptls, err := loadTLS(tc.certFile, tc.keyFile, tc.caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	ms, cluster, err := RemoteMembers(ctx, nil, ptls, []string{"127.0.0.1:1", strings.TrimPrefix(tc.peers[1], "https://")})
	if err != nil || cluster != 0x7769726531 || len(ms) != 4 {
		t.Fatalf("RemoteMembers: %v, cluster %x, %v", ms, cluster, err)
	}
	tc.members = ms
	tc.ids[i] = added.Member.ID
	tc.startNode(t, i)
	lc := tc.rawClient(t, tc.endpoints[i])
	want, err := c.Get(ctx, "/before-join")
	if err != nil {
		t.Fatal(err)
	}
	for {
		got, err := lc.Get(ctx, "/before-join", clientv3.WithSerializable())
		if err == nil && len(got.Kvs) == 1 && got.Kvs[0].ModRevision == want.Kvs[0].ModRevision {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the learner never caught up: %v %v", got, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := lc.Put(ctx, "/on-learner", "x"); rpctypes.ErrorDesc(err) != "etcdserver: rpc not supported for learner" {
		t.Fatalf("a learner took a write: %v", err)
	}

	// A follower forwards the promotion to the leader, as etcd's does;
	// then the learner is a voter like the others.
	if _, err := follower.MemberPromote(ctx, added.Member.ID); err != nil {
		t.Fatal(err)
	}
	r = waitMembers(t, c, 4)
	if slices.ContainsFunc(r.Members, func(m *etcdMember) bool { return m.IsLearner }) {
		t.Fatalf("after the promotion the members are %v", r.Members)
	}
	if _, err := c.MemberUpdate(ctx, added.Member.ID, []string{tc.peers[i]}); err != nil {
		t.Fatal(err)
	}

	// Removed, it stops; the others go on without it.
	if _, err := c.MemberRemove(ctx, added.Member.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-tc.stopped[i]:
		if !errors.Is(err, ErrRemoved) {
			t.Fatalf("the removed node stopped with %v", err)
		}
		tc.cancels[i]()
		tc.cancels[i] = nil
	case <-ctx.Done():
		t.Fatal("the removed node did not stop")
	}
	if _, err := c.MemberAdd(ctx, []string{tc.peers[i]}); err != nil {
		t.Fatalf("its peer URL is free again: %v", err)
	}
	r = waitMembersAtLeast(t, c, 4)
	// The member just added never started: remove it again.
	for _, m := range r.Members {
		if m.Name == "" {
			if _, err := c.MemberRemove(ctx, m.ID); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The list comes back after a restart, from checkpoints and the log.
	for k := range 3 {
		tc.stopNode(t, k)
	}
	for k := range 3 {
		tc.startNode(t, k)
	}
	c = tc.client(t)
	r = waitMembers(t, c, 3)
	for k, m := range r.Members {
		if m.ID != uint64(k+1) {
			t.Fatalf("after the restart the members are %v", r.Members)
		}
	}
}

// waitMembersAtLeast waits until the member list holds want members.
func waitMembersAtLeast(t *testing.T, c *clientv3.Client, want int) *clientv3.MemberListResponse {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		r, err := c.MemberList(context.Background())
		if err == nil && len(r.Members) >= want {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("the member list is %v (%v), want %d members", r, err, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// leaderEndpoint is the client endpoint of the cluster's leader.
func (tc *testCluster) leaderEndpoint(t *testing.T, c *clientv3.Client) string {
	t.Helper()
	for _, ep := range tc.endpoints[:3] {
		s, err := c.Status(context.Background(), ep)
		if err == nil && s.Leader == s.Header.MemberId {
			return ep
		}
	}
	t.Fatal("no leader")
	return ""
}

// followerEndpoint is the endpoint of a node of the first three that does
// not lead.
func (tc *testCluster) followerEndpoint(t *testing.T, c *clientv3.Client) string {
	t.Helper()
	for _, ep := range tc.endpoints[:3] {
		s, err := c.Status(context.Background(), ep)
		if err == nil && s.Leader != 0 && s.Leader != s.Header.MemberId {
			return ep
		}
	}
	t.Fatal("no follower")
	return ""
}

// As etcd's, a leader that stops hands its leadership to a peer first:
// once it has stopped, a peer leads, without an election timeout.
func TestALeaderThatStopsHandsItsLeadershipOver(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := c.Status(ctx, tc.endpoints[0])
	if err != nil {
		t.Fatal(err)
	}
	lead := slices.Index(tc.ids, st.Leader)
	if lead < 0 {
		t.Fatalf("leader %x is no node of the cluster", st.Leader)
	}
	tc.stopNode(t, lead)
	peer := (lead + 1) % 3
	st, err = tc.rawClient(t, tc.endpoints[peer]).Status(ctx, tc.endpoints[peer])
	if err != nil {
		t.Fatal(err)
	}
	if st.Leader == 0 || st.Leader == tc.ids[lead] {
		t.Fatalf("once node %d, the leader, stopped, node %d names leader %x", lead+1, peer+1, st.Leader)
	}
}

// A follower answers a membership change once it applied it, which may be
// before the leader has. The client's next change, through the follower,
// is then proposed while the leader's Raft still holds the first as not
// applied: Raft dropped it, and the client waited for requestTimeout
// (#705, difftest's Boundaries). The leader now holds such a change back
// until its Raft takes it. Each round is answered at once, through a
// follower and through the leader.
func TestMembershipChangesInARowAreNeverDropped(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	waitMembers(t, c, 3)
	for _, ep := range []string{tc.followerEndpoint(t, c), tc.leaderEndpoint(t, c)} {
		cl := tc.rawClient(t, ep)
		for i := range 150 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			added, err := cl.MemberAddAsLearner(ctx, []string{"https://127.0.0.1:3", "https://127.0.0.2:1"})
			if err != nil {
				cancel()
				t.Fatalf("%s, round %d: add a learner: %v", ep, i, err)
			}
			if _, err := cl.MemberAddAsLearner(ctx, []string{"https://127.0.0.1:4"}); !errors.Is(err, rpctypes.ErrTooManyLearners) {
				cancel()
				t.Fatalf("%s, round %d: add a second learner: %v", ep, i, err)
			}
			if _, err := cl.MemberAddAsLearner(ctx, []string{"https://127.0.0.2:1"}); !errors.Is(err, rpctypes.ErrPeerURLExist) {
				cancel()
				t.Fatalf("%s, round %d: add a learner with a taken peer URL: %v", ep, i, err)
			}
			if _, err := cl.MemberRemove(ctx, added.Member.ID); err != nil {
				cancel()
				t.Fatalf("%s, round %d: remove the learner: %v", ep, i, err)
			}
			cancel()
		}
	}
}
