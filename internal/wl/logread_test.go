package wl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// committedOps reads node i's log and returns the keys of the puts under
// /log/ and the announcements it committed, after checking every batch
// reads.
func committedOps(t *testing.T, tc *testCluster, i int) ([]string, []*Announcement) {
	t.Helper()
	l, err := ReadLog(nil, tc.dataDir(i))
	if err != nil {
		t.Fatal(err)
	}
	first, commit, ents := l.Base+1, l.Commit, l.Entries
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

// A stopped node's log reads back from behind its newest checkpoint: the
// checkpoint's index and term, then every entry after it, each op of
// every kind decoding as etcd's request; and its records lie where the
// pointer says the log starts. The nodes take checkpoints in a first run,
// and none in a second, whose ops of every kind lie after the newest.
func TestALogBehindACheckpointReadsBackWithEveryKindOfOp(t *testing.T) {
	smallCheckpoints(t)
	tc := newTestCluster(t)
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for i := range 50 {
		if _, err := c.Put(ctx, fmt.Sprintf("/b/%02d", i), strings.Repeat("v", 200)); err != nil {
			t.Fatal(err)
		}
	}
	tc.stop(t)
	ckptMin = 1 << 30
	tc.start(t)
	c = tc.clientOf(t, tc.endpoints[0])
	steps := []func() error{
		func() error { _, err := c.Put(ctx, "/b/again", "v"); return err },
		func() error { _, err := c.Delete(ctx, "/b/00"); return err },
		func() error {
			_, err := c.Txn(ctx).If(clientv3.Compare(clientv3.Value("/b/01"), "=", "x")).Then(clientv3.OpPut("/b/01", "y")).Else(clientv3.OpPut("/b/02", "z")).Commit()
			return err
		},
		func() error {
			g, err := c.Grant(ctx, 60)
			if err != nil {
				return err
			}
			if _, err := c.KeepAliveOnce(ctx, g.ID); err != nil {
				return err
			}
			_, err = c.Revoke(ctx, g.ID)
			return err
		},
		func() error {
			r, err := c.Get(ctx, "/b/03")
			if err != nil {
				return err
			}
			_, err = c.Compact(ctx, r.Header.Revision)
			return err
		},
		func() error {
			_, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: tc.ids[0], Alarm: etcdpb.AlarmType_NOSPACE})
			return err
		},
		func() error { _, err := c.Downgrade(ctx, clientv3.DowngradeEnable, "3.6"); return err },
		func() error { _, err := c.Downgrade(ctx, clientv3.DowngradeCancel, ""); return err },
		func() error { _, err := c.UserAdd(ctx, "root", "rootpw"); return err },
		func() error { _, err := c.UserGrantRole(ctx, "root", "root"); return err },
		func() error { _, err := c.AuthEnable(ctx); return err },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	root, err := clientv3.New(clientv3.Config{Endpoints: tc.endpoints[:1], TLS: tc.clientTLS(), Username: "root", Password: "rootpw", DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if _, err := root.Put(ctx, "/b/after", "w"); err != nil {
		t.Fatal(err)
	}
	tc.stop(t)
	l, err := ReadLog(nil, tc.dataDir(0))
	if err != nil {
		t.Fatal(err)
	}
	if l.Base == 0 || l.BaseTerm == 0 || len(l.Entries) == 0 || l.Entries[0].GetIndex() != l.Base+1 {
		t.Fatalf("the log reads back from %d (term %d) with %d entries; want it behind a checkpoint", l.Base, l.BaseTerm, len(l.Entries))
	}
	kinds := map[string]bool{}
	for _, e := range l.Entries {
		if e.GetType() != pb.EntryNormal || len(e.GetData()) == 0 {
			continue
		}
		b, err := ParseBatch(e.GetData())
		if err != nil {
			t.Fatalf("entry %d: %v", e.GetIndex(), err)
		}
		for _, o := range b.Ops {
			m, err := o.Message()
			if err != nil {
				t.Fatalf("entry %d: %v", e.GetIndex(), err)
			}
			kinds[fmt.Sprintf("%T", m)] = true
		}
	}
	// Every kind the second run proposed; the put after auth came on
	// carries its caller.
	kindsWanted := []string{
		"*etcdserverpb.PutRequest", "*etcdserverpb.DeleteRangeRequest", "*etcdserverpb.TxnRequest",
		"*etcdserverpb.LeaseGrantRequest", "*etcdserverpb.LeaseKeepAliveRequest", "*etcdserverpb.LeaseRevokeRequest",
		"*etcdserverpb.CompactionRequest", "*etcdserverpb.AlarmRequest", "*etcdserverpb.DowngradeRequest",
		"*etcdserverpb.InternalRaftRequest", "*wl.Caller",
	}
	for _, k := range kindsWanted {
		if !kinds[k] {
			t.Errorf("no %s read back after the checkpoint; got %v", k, kinds)
		}
	}
	ptr, err := os.ReadFile(filepath.Join(tc.dataDir(0), "ckpt", "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ReadPointer(ptr)
	if err != nil {
		t.Fatal(err)
	}
	seg := func(k int64) []byte {
		b, err := os.ReadFile(filepath.Join(tc.dataDir(0), "wal", fmt.Sprintf("%016x.seg", k)))
		if err != nil {
			return nil
		}
		return b
	}
	recs := Records(seg, p.LogFrom)
	if len(recs) == 0 || recs[0].Off != p.LogFrom {
		t.Fatalf("the records from %d: %v", p.LogFrom, recs)
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Off != recs[i-1].Off+recs[i-1].Len {
			t.Fatalf("record %d at %d does not follow the one before at %d of %d bytes", i, recs[i].Off, recs[i-1].Off, recs[i-1].Len)
		}
	}
}
