package core

import (
	"errors"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestDiag280RepairGuards(t *testing.T) {
	for _, which := range []string{"one", "two", "different-term", "different-context", "remove", "not-initial-member", "interrupted-sync"} {
		t.Run(which, func(t *testing.T) {
			cfg := DefaultConfig(2, []uint64{1, 2, 3})
			d := simfs.New()
			w, rec, err := wal.Open(d)
			if err != nil {
				t.Fatal(err)
			}
			n, err := New(cfg, rec, w)
			if err != nil {
				t.Fatal(err)
			}
			eff, err := n.Process()
			if err != nil {
				t.Fatal(err)
			}
			prefix := []*raftpb.Entry{proto.CloneOf(eff.Persist[0].Entries[0])}
			repair := which == "one" || which == "two" || which == "interrupted-sync"
			switch which {
			case "two":
				prefix = append(prefix, eff.Persist[0].Entries[1])
			case "different-term":
				prefix[0].Term = new(uint64(2))
			case "different-context", "remove":
				var cc raftpb.ConfChange
				if err := proto.Unmarshal(prefix[0].GetData(), &cc); err != nil {
					t.Fatal(err)
				}
				if which == "remove" {
					cc.Type = raftpb.ConfChangeRemoveNode.Enum()
				} else {
					cc.Context = []byte("not the persisted initial member")
				}
				prefix[0].Data, err = proto.Marshal(&cc)
				if err != nil {
					t.Fatal(err)
				}
			case "not-initial-member":
				cfg.ID = 4
			}
			hs := wal.HardState{Term: 2, Vote: 1}
			w.Append(&hs, prefix)
			if err := errors.Join(w.Sync(), w.Close()); err != nil {
				t.Fatal(err)
			}
			w, rec, err = wal.Open(d)
			if err != nil {
				t.Fatal(err)
			}
			if which == "interrupted-sync" {
				interrupted := fmt.Errorf("power lost before repair sync")
				d.Fault = func(op simfs.Op, _ string) error {
					if op == simfs.OpSync {
						return interrupted
					}
					return nil
				}
				if err := RepairTornBootstrap(cfg, rec, w); !errors.Is(err, interrupted) {
					t.Fatalf("repair returned %v, want interrupted sync", err)
				}
				// Power loss need not keep any of the unsynced suffix.
				// Restart from precisely the previous durable prefix.
				d = d.Durable()
				w, rec, err = wal.Open(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := RepairTornBootstrap(cfg, rec, w); err != nil {
					t.Fatal(err)
				}
				if rec.HardState != hs {
					t.Fatalf("changed term/vote/commit: %+v -> %+v", hs, rec.HardState)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				w, rec, err = wal.Open(d.Durable())
				if err != nil {
					t.Fatal(err)
				}
			}
			want := uint64(len(prefix))
			if repair {
				want = 3
			}
			if rec.Last != want || rec.HardState != hs {
				t.Fatalf("got last=%d hs=%+v, want last=%d hs=%+v", rec.Last, rec.HardState, want, hs)
			}
			if repair {
				n, err := New(cfg, rec, w)
				if err != nil {
					t.Fatal(err)
				}
				if len(n.Members()) != 3 {
					t.Fatalf("got partial membership: %v", n.Members())
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
