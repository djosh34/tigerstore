package wl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	pb "go.etcd.io/raft/v3/raftpb"
)

// committedOps reads node i's log and returns the keys of the puts under
// /log/ and the announcements it committed, after checking every batch
// reads.
func committedOps(t *testing.T, tc *testCluster, i int) ([]string, []*Announcement) {
	t.Helper()
	first, commit, ents, err := ReadLog(tc.dataDir(i))
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || commit == 0 || commit > first+uint64(len(ents))-1 {
		t.Fatalf("node %d: entries %d to %d, commit %d", i+1, first, first+uint64(len(ents))-1, commit)
	}
	var keys []string
	var anns []*Announcement
	for _, e := range ents[:commit-first+1] {
		if e.GetType() != pb.EntryNormal || len(e.GetData()) == 0 {
			continue
		}
		b, err := ParseBatch(e.GetData())
		if err != nil {
			t.Fatalf("node %d entry %d: %v", i+1, e.GetIndex(), err)
		}
		if b.Format < baseFormat || b.Format > maxFormat {
			t.Fatalf("node %d entry %d is of format %d", i+1, e.GetIndex(), b.Format)
		}
		for _, o := range b.Ops {
			m, err := o.Message()
			if err != nil {
				t.Fatalf("node %d entry %d: %v", i+1, e.GetIndex(), err)
			}
			switch m := m.(type) {
			case *etcdpb.PutRequest:
				if strings.HasPrefix(string(m.Key), "/log/") {
					keys = append(keys, string(m.Key))
				}
			case *Announcement:
				anns = append(anns, m)
			}
		}
	}
	return keys, anns
}

func TestTheLogOfAStoppedNodeReadsBack(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var want []string
	for i := range 20 {
		k := fmt.Sprintf("/log/%02d", i)
		if _, err := c.Put(ctx, k, "v"); err != nil {
			t.Fatal(err)
		}
		want = append(want, k)
	}
	runs := func(run uint64) {
		t.Helper()
		for i := range 3 {
			keys, anns := committedOps(t, tc, i)
			if !slices.Equal(keys, want) {
				t.Fatalf("node %d committed puts %v, want %v", i+1, keys, want)
			}
			// Every member announced this run's support, then confirmed.
			for _, id := range []uint64{1, 2, 3} {
				confirmed := false
				for _, a := range anns {
					if a.ID == id && a.Run == run && a.Format == maxFormat && a.Epoch != 0 {
						confirmed = true
					}
				}
				if !confirmed {
					t.Fatalf("node %d's log has no confirmation of member %d's run %d: %+v", i+1, id, run, anns)
				}
			}
		}
	}
	// The confirmations follow the announcements by a publish interval.
	time.Sleep(3 * publishEvery)
	tc.stop(t)
	runs(1)
	tc.start(t)
	c = tc.client(t)
	if _, err := c.Put(ctx, "/log/after", "v"); err != nil {
		t.Fatal(err)
	}
	want = append(want, "/log/after")
	time.Sleep(3 * publishEvery)
	tc.stop(t)
	runs(2)
}

func TestABatchReadsBackAsWritten(t *testing.T) {
	b := Batch{Origin: 7, Seq: 3<<32 | 9, Format: baseFormat, Ops: []Op{
		{Kind: kPut, Req: appendBytesField(nil, 1, []byte("k"))},
		{Kind: kMemberAttrs, Req: appendAnnouncement(nil, &member{id: 7, format: 2, run: 3}, 11)},
	}}
	got, err := ParseBatch(AppendBatch(nil, b))
	if err != nil || got.Origin != b.Origin || got.Seq != b.Seq || got.Format != b.Format || len(got.Ops) != 2 {
		t.Fatalf("read back %+v, %v", got, err)
	}
	m, err := got.Ops[1].Message()
	if a, ok := m.(*Announcement); err != nil || !ok || *a != (Announcement{ID: 7, Format: 2, Run: 3, Epoch: 11}) {
		t.Fatalf("the announcement read back as %+v, %v", m, err)
	}
	if _, err := ParseBatch([]byte("not a batch at all, no")); !errors.Is(err, ErrNotBatch) {
		t.Fatalf("a non-batch gave %v", err)
	}
}
