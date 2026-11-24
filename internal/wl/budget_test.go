package wl

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// The node's bounded buffers, each at its worst (sizes.worst), fit
// every budget from the least for its followers up (leastBudget) with
// room for the store: at least a quarter of the budget, for its tables
// and its slab (store.relimit). Such a budget keeps every
// smaller cluster too. A smaller one is refused (Config.check,
// node.budgetKeeps): a node at 96 MiB took the budget and then neither
// kept it nor refused it (#760).
func TestTheBuffersFitTheBudget(t *testing.T) {
	const mostFollowers = 8
	var least [mostFollowers + 1]int64
	for f := range least {
		least[f] = leastBudget(f)
		if keeps(least[f]-1<<20, f) {
			t.Errorf("%d followers: a budget of %d MiB, below the least, keeps its buffers too", f, (least[f]-1<<20)>>20)
		}
		if f > 0 && least[f] < least[f-1] {
			t.Errorf("%d followers keep a least budget of %d MiB, below %d MiB for one fewer", f, least[f]>>20, least[f-1]>>20)
		}
	}
	budgets := []int64{16 << 30, 48 << 30}
	for b := least[0]; b <= 4<<30; b += 1 << 20 {
		budgets = append(budgets, b)
	}
	for _, budget := range budgets {
		z := sizeFor(budget)
		for followers := 0; followers <= mostFollowers && budget >= least[followers]; followers++ {
			for fewer := 0; fewer <= followers; fewer++ {
				if !keeps(budget, fewer) {
					t.Fatalf("budget %d MiB, %d followers, at least the %d MiB of %d: the buffers take %d MiB at their worst (mapped %d MiB, held %d MiB, uncommitted %d MiB, kept %d buffers, clients %d MiB and a reserve of %d MiB, fetched %d MiB); want the store left at least a quarter",
						budget>>20, fewer, least[followers]>>20, followers, z.worst(budget, fewer)>>20, z.mapped()>>20, z.held>>20, z.uncommitted>>20, z.kept, z.clientPool>>20, z.reserve()>>20, fetchedMax(budget)>>20)
				}
			}
			if z.inflight(followers+1) > uint64(z.peerBacklog(followers)) {
				t.Errorf("budget %d MiB, %d followers: Raft keeps %d bytes in flight to a follower, past its backlog of %d", budget>>20, followers, z.inflight(followers+1), z.peerBacklog(followers))
			}
		}
		// The idle connections the node takes fit what the client
		// buffers leave of the client pool; the clients that hold buffers, at most as many as
		// the buffers hold their read and opened buffers, have shares of
		// what waits to be sent within the buffers, and each a share of
		// requests sent in part of at least one whole request.
		if idle := int64(z.maxClients()) * idleConn; idle+z.clientBufs() > z.clientPool {
			t.Errorf("budget %d MiB: %d idle clients take %d MiB, past what the client buffers leave of the client pool's %d MiB", budget>>20, z.maxClients(), idle>>20, z.clientPool>>20)
		}
		most := int(z.clientInBufs() / (clientInBuf + clientPtBuf))
		for _, active := range []int{1, most} {
			// The shares fit the send half; the reserve for the
			// messages past them (#727) is beyond it, counted above.
			if out := int64(active) * z.connOut(active); out > z.clientBufs()-z.clientInBufs() || z.connIn(active) < maxMsg {
				t.Errorf("budget %d MiB, %d active clients: their send shares sum to %d MiB, past the send half of the client buffers' %d MiB, or a share of requests sent in part of %d KiB",
					budget>>20, active, out>>20, z.clientBufs()>>20, z.connIn(active)>>10)
			}
		}
		// One message, at most maxMsg, can pass a share by no more than
		// the whole reserve.
		if z.reserve() < maxMsg {
			t.Errorf("budget %d MiB: a reserve of %d KiB, less than one message of %d KiB", budget>>20, z.reserve()>>10, maxMsg>>10)
		}
		if z.stage&(z.stage-1) != 0 || z.stage < 4<<20 || z.stage > z.ring {
			t.Errorf("budget %d MiB: a stage of %d bytes, want a power of two from 4 MiB up to the ring's %d", budget>>20, z.stage, z.ring)
		}
	}
	// Thousands of clients at the bench's 3 GiB, as a cluster of several
	// API servers, controllers and tools may hold.
	if most := sizeFor(3 << 30).maxClients(); most < 3000 {
		t.Errorf("at 3 GiB at most %d client connections, want at least 3,000", most)
	}
	// The bench's 3 GiB keeps the sizes it measured.
	if z := sizeFor(3 << 30); z.ring != 64<<20 || z.side != 16<<20 || z.stage != 32<<20 || z.held != 64<<20 || z.kept != 64 {
		t.Errorf("at 3 GiB: rings %d, side %d, stage %d, held %d MiB, %d kept buffers; want 64, 16, 32 and 64 MiB, 64 buffers", z.ring>>20, z.side>>20, z.stage>>20, z.held>>20, z.kept)
	}
}

// The least budgets of #760's table: a cluster of three keeps 110 MiB,
// and its members that bootstrap it start with at least 137 MiB, room for
// a learner as well (startFollowers); a learner that joins them starts at
// the same 137 MiB. A cluster of seven needs 220 MiB.
func TestTheLeastBudgetsAreTheTables(t *testing.T) {
	for _, c := range []struct {
		followers int
		mib       int64
	}{{1, 83}, {2, 110}, {3, 137}, {4, 164}, {5, 191}, {6, 220}} {
		if got := leastBudget(c.followers); got != c.mib<<20 {
			t.Errorf("%d followers: a least budget of %d MiB, want %d MiB", c.followers, got>>20, c.mib)
		}
	}
	if got := leastBudget(startFollowers(3, false)); got != 137<<20 {
		t.Errorf("three voters start with a least budget of %d MiB, want 137 MiB", got>>20)
	}
	if got := leastBudget(startFollowers(4, true)); got != 137<<20 {
		t.Errorf("a learner that joins three voters starts with a least budget of %d MiB, want 137 MiB", got>>20)
	}
}

// startConfig is a Config that Config.check takes, of members, with
// budget.
func startConfig(members []Member, join bool, budget int64) *Config {
	return &Config{
		ID: 1, Members: members, Join: join, MemoryBudget: budget, Logger: log.New(io.Discard, "", 0),
		PeerServerTLS: &tls.Config{}, PeerClientTLS: &tls.Config{},
		PlainClientListeners: []net.Listener{nil}, PeerListeners: []net.Listener{nil},
	}
}

// A node refuses at start a budget below the least for its members, and a
// learner if it bootstraps the cluster, and names that least budget; it
// takes the least itself, and no budget.
func TestAStartBelowTheLeastBudgetIsRefused(t *testing.T) {
	three := []Member{{ID: 1}, {ID: 2}, {ID: 3}}
	four := append(slices.Clone(three), Member{ID: 4})
	for _, c := range []struct {
		members []Member
		join    bool
	}{{three, false}, {four, true}, {three[:1], false}} {
		least := leastBudget(startFollowers(len(c.members), c.join))
		err := startConfig(c.members, c.join, least-1).check()
		if !errors.Is(err, errConfig) || !strings.Contains(err.Error(), strconv.FormatInt(least, 10)) {
			t.Errorf("%d members, join %v: a budget a byte below %d MiB: %v, want it refused naming %d bytes", len(c.members), c.join, least>>20, err, least)
		}
		if err := startConfig(c.members, c.join, least).check(); err != nil {
			t.Errorf("%d members, join %v: the least budget of %d MiB refused: %v", len(c.members), c.join, least>>20, err)
		}
		if err := startConfig(c.members, c.join, 0).check(); err != nil {
			t.Errorf("%d members, join %v: no budget refused: %v", len(c.members), c.join, err)
		}
	}
}

// A member add that takes the cluster past what the budget of the node
// asked keeps is refused as it is proposed, with the least budget the
// cluster would need, and the list stays as it was. n1 runs at three
// voters' least budget, the others at more; the adds are asked of n1,
// which takes a learner, which joins and catches up at that budget, but
// not a fifth member; a second learner gets etcd's error.
func TestAMemberAddPastTheBudgetIsRefused(t *testing.T) {
	budget := leastBudget(startFollowers(3, false))
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.budget = budget
		tc.config = func(c *Config) {
			if c.Name == "n2" || c.Name == "n3" {
				c.MemoryBudget = leastBudget(4)
			}
		}
	})
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	waitMembers(t, c, 3)
	if _, err := c.Put(ctx, "/before-join", "v"); err != nil {
		t.Fatal(err)
	}
	n1 := tc.rawClient(t, tc.endpoints[0])
	i := tc.addPorts(t, 0, true)
	added, err := n1.MemberAddAsLearner(ctx, []string{tc.peers[i]})
	if err != nil {
		t.Fatalf("a learner added to three voters at %d MiB: %v", budget>>20, err)
	}
	ptls, err := loadTLS(tc.certFile, tc.keyFile, tc.caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	ms, _, err := RemoteMembers(ctx, nil, ptls, []string{strings.TrimPrefix(tc.peers[0], "https://")})
	if err != nil || len(ms) != 4 {
		t.Fatalf("RemoteMembers: %v, %v", ms, err)
	}
	tc.members = ms
	tc.ids[i] = added.Member.ID
	tc.startNode(t, i)
	lc := tc.rawClient(t, tc.endpoints[i])
	for {
		got, err := lc.Get(ctx, "/before-join", clientv3.WithSerializable())
		if err == nil && len(got.Kvs) == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the learner at %d MiB never caught up: %v %v", budget>>20, got, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// An add the apply refuses anyway gets etcd's error, not the budget's.
	if _, err := n1.MemberAddAsLearner(ctx, []string{"https://127.0.0.1:1"}); !errors.Is(err, rpctypes.ErrTooManyLearners) {
		t.Fatalf("a second learner: %v, want etcd's error for too many learners", err)
	}
	if _, err := n1.MemberAdd(ctx, []string{"https://127.0.0.1:2"}); err == nil || !strings.Contains(err.Error(), strconv.FormatInt(leastBudget(4), 10)) {
		t.Fatalf("a fifth member asked of n1 at %d MiB: %v, want it refused naming %d bytes", budget>>20, err, leastBudget(4))
	}
	if r, err := c.MemberList(ctx); err != nil || len(r.Members) != 4 {
		t.Fatalf("the member list after the refused add: %v (%v), want 4 members", r, err)
	}
}
