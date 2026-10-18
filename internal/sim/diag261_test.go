package sim

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/wal"
)

// Reference-only reproduction of #261/#250. Node 2's disk is erased.
// Node 1 predates member 4, node 3 knows its promotion and updated URL,
// and node 4 has a newer, uncommitted log tail. Only node 4 can win an
// election, but it needs node 1's vote for the four-voter quorum.
func TestDiag261ColdRestart(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprintf("lagging-knows-four=%t", known), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := newSim(Config{Seed: 261, Membership: true})
				if err != nil {
					t.Fatal(err)
				}
				defer s.shutdown()
				s.healed = true
				var entries []*raftpb.Entry
				conf := func(typ raftpb.ConfChangeType, op byte, m *pb.Member) {
					// core.encodeConfContext: origin, boot, request, operation, member.
					ctx := binary.AppendUvarint(nil, 0)
					ctx = binary.AppendUvarint(ctx, 0)
					ctx = binary.AppendUvarint(ctx, 0)
					ctx = append(ctx, op)
					ctx, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(ctx, m)
					if err != nil {
						t.Fatal(err)
					}
					data, err := proto.Marshal(&raftpb.ConfChange{Type: typ.Enum(), NodeId: new(m.ID), Context: ctx})
					if err != nil {
						t.Fatal(err)
					}
					entries = append(entries, &raftpb.Entry{Index: new(uint64(len(entries) + 1)), Term: new(uint64(1)), Type: raftpb.EntryConfChange.Enum(), Data: data})
				}
				for i := range 3 {
					conf(raftpb.ConfChangeAddNode, 1, &pb.Member{ID: uint64(i + 1), PeerURLs: []string{peerURL(i)}})
				}
				conf(raftpb.ConfChangeAddLearnerNode, 1, &pb.Member{ID: 4, PeerURLs: []string{memberURL}, IsLearner: true})
				conf(raftpb.ConfChangeUpdateNode, 4, &pb.Member{ID: 4, PeerURLs: []string{updatedMemberURL}})
				conf(raftpb.ConfChangeAddNode, 2, &pb.Member{ID: 4})
				entries = append(entries, &raftpb.Entry{Index: new(uint64(7)), Term: new(uint64(2)), Type: raftpb.EntryNormal.Enum()})
				for i, n := range s.nodes {
					if i == 1 {
						n.fenced = true
						continue
					}
					last, commit := 6, uint64(6)
					if i == 0 && !known {
						last, commit = 3, 3
					}
					if i == 3 {
						last = 7
					}
					w, _, err := wal.Open(n.disk)
					if err != nil {
						t.Fatal(err)
					}
					w.Append(&wal.HardState{Term: 2, Commit: commit}, entries[:last])
					if err := w.Sync(); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					n.disk = n.disk.Durable()
				}
				for _, n := range s.nodes {
					if err := s.start(n); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.runFor(25 * Second); err != nil {
					t.Fatalf("%v\n%s", err, s.report())
				}
				for _, n := range s.nodes {
					if n.fenced {
						continue
					}
					if n.proc == nil || n.proc.core.Leader() == 0 || n.proc.core.Applied() < 7 || len(n.proc.core.Members()) != 4 {
						t.Fatalf("cold restart did not converge\n%s\n%s", s.report(), strings.Join(s.trace, "\n"))
					}
				}
				if a, _ := disagree(s.nodes); a != nil {
					t.Fatalf("survivors disagree\n%s", s.report())
				}
				t.Log("all three survivors elected and converged")
			})
		})
	}
}
