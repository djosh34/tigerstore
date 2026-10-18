package peer

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
	"go.etcd.io/raft/v3/raftpb"
)

// The acceptor has only applied the old membership. The dialer is a newer
// voter. Accepting its vote is not enough: the acceptor also needs a route
// for the vote response, independent of its applied membership.
func TestDiag250UnknownVoter(t *testing.T) {
	for _, removed := range []bool{false, true} {
		name := "lagging_member_can_reply"
		if removed {
			name = "removed_member_is_still_refused"
		}
		t.Run(name, func(t *testing.T) {
			ca, err := testcert.NewCA()
			if err != nil {
				t.Fatal(err)
			}
			cert, _, _, err := ca.Issue("node", 2)
			if err != nil {
				t.Fatal(err)
			}
			addrs, lns := listeners(t, 2, 4)
			a := startWith(t, 2, lns[2], addrs, cert, ca.Pool, func(c *Config) {
				c.Peers = map[uint64]string{} // member 4 has not applied here
			})
			t.Cleanup(func() { _ = a.t.Close() })
			if removed {
				a.t.SetPeers(map[uint64]string{}, []uint64{4})
			}
			removedSignal := make(chan struct{}, 10)
			b := startWith(t, 4, lns[4], addrs, cert, ca.Pool, func(c *Config) {
				c.Removed = func() { removedSignal <- struct{}{} }
			})
			t.Cleanup(func() { _ = b.t.Close() })
			vote := &raftpb.Message{Type: raftpb.MsgVote.Enum(), From: new(uint64(4)), To: new(uint64(2)), Term: new(uint64(8)), Index: new(uint64(10)), LogTerm: new(uint64(7))}
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			retry := time.NewTicker(10 * time.Millisecond)
			defer retry.Stop()
			for {
				select {
				case <-retry.C:
					b.t.Send(vote)
				case <-removedSignal:
					if !removed {
						t.Fatal("unknown is not removed")
					}
					select {
					case m := <-a.got:
						t.Fatalf("removed member delivered %v", m)
					default:
					}
					return
				case m := <-a.got:
					if removed {
						t.Fatalf("removed member delivered %v", m)
					}
					if m.GetType() != raftpb.MsgVote || m.GetFrom() != 4 {
						t.Fatalf("unexpected message: %v", m)
					}
					// An applied-membership refresh must not lose the temporary
					// route; adding a remote must not add a Raft voter.
					a.t.SetPeers(map[uint64]string{}, nil)
					diag250Reply(t, a, b, deadline.C)
					return
				case <-deadline.C:
					t.Fatal("lagging member refused the newer voter's MsgVote (or removal was not reported)")
				}
			}
		})
	}
}

func diag250Reply(t *testing.T, a, b *end, timeout <-chan time.Time) {
	t.Helper()
	retry := time.NewTicker(10 * time.Millisecond)
	defer retry.Stop()
	for {
		select {
		case <-retry.C:
			a.t.Send(&raftpb.Message{Type: raftpb.MsgVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(4)), Term: new(uint64(8))})
		case m := <-b.got:
			if m.GetType() != raftpb.MsgVoteResp || m.GetFrom() != 2 || m.GetReject() {
				t.Fatalf("unexpected response: %v", m)
			}
			t.Log("newer voter received the lagging member's vote, without changing applied membership")
			return
		case <-timeout:
			t.Fatal("accepted unknown voter, but could not route its vote response")
		}
	}
}
