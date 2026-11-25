//go:build !(sim || simio)

package wl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// A checkpoint takes no more room than it asks for once scanned
// (ckptNeed), and not much less, whatever the store holds:
//   - keys with empty, small or larger values, leased with long IDs, at
//     revisions past 2^40, each replaced in the history and half of them
//     deleted, which a bound on the quota's size missed;
//   - revisions of a Put that keeps its value, which share its bytes and
//     which a count of every revision's value took for 100 copies;
//   - a long key put and deleted at the revision the store is compacted
//     at, whose changes at it name the key twice.
func TestACheckpointFitsTheRoomItAsksFor(t *testing.T) {
	put := func(t *testing.T, st *store, id uint32, value string, lease, voff int64) {
		t.Helper()
		if _, err := st.put(id, []byte(value), lease, st.rev, voff, false); err != nil {
			t.Fatal(err)
		}
	}
	intern := func(t *testing.T, st *store, key []byte) uint32 {
		t.Helper()
		id, err := st.intern(key)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	cases := map[string]func(t *testing.T, st *store, l *memLog){}
	for _, value := range []string{"", "v", strings.Repeat("v", 3000)} {
		cases[fmt.Sprintf("leased keys with values of %d bytes", len(value))] = func(t *testing.T, st *store, l *memLog) {
			st.rev = 1 << 40
			const leases = 300
			for i := range leases {
				st.leases[1<<62+int64(i)] = &lease{ttl: maxLeaseTTL, keys: map[uint32]struct{}{}}
			}
			for i := range 3000 {
				id := intern(t, st, fmt.Appendf(nil, "/k/%05d", i))
				for range 3 {
					st.rev++
					put(t, st, id, value, 1<<62+int64(i%leases), l.add(value))
					st.endApply()
				}
				if i%2 == 0 {
					st.rev++
					if _, _, err := st.del(id, st.rev, false); err != nil {
						t.Fatal(err)
					}
					st.endApply()
				}
			}
		}
	}
	cases["a value kept by 100 revisions"] = func(t *testing.T, st *store, l *memLog) {
		value := strings.Repeat("v", 128<<10)
		id, voff := intern(t, st, []byte("/kept")), l.add(value)
		for range 100 {
			st.rev++
			put(t, st, id, value, 0, voff)
			st.endApply()
		}
	}
	cases["a long key put and deleted at the compacted revision"] = func(t *testing.T, st *store, l *memLog) {
		key := []byte("/" + strings.Repeat("k", 64<<10))
		id := intern(t, st, key)
		st.rev++
		put(t, st, id, "v", 0, l.add("v"))
		if _, _, err := st.del(id, st.rev, false); err != nil {
			t.Fatal(err)
		}
		st.endApply()
		st.compact(st.rev)
		other := intern(t, st, []byte("/other"))
		st.rev++
		put(t, st, other, "v", 0, l.add("v"))
		st.endApply()
	}
	for name, fill := range cases {
		t.Run(name, func(t *testing.T) {
			n, l := ckptNode(t)
			fill(t, n.st, l)
			takeEveryValue(t)
			n.ckptFrom = int64(len(l.b))
			if err := n.startCkpt(5, 1, 0); err != nil {
				t.Fatal(err)
			}
			need := int64(-1)
			for n.ckpt != nil {
				r := n.ckpt
				if err := n.ckptStep(r.sent > 0); err != nil {
					t.Fatal(err)
				}
				if need < 0 && r.phase != ckptScanning {
					need = n.ckptNeed(r)
				}
			}
			info, err := os.Stat(filepath.Join(n.ckptDir, n.ckptName))
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() > need || need > 3*info.Size()+16<<10 {
				t.Fatalf("the checkpoint took %d bytes; it asked for %d", info.Size(), need)
			}
			t.Logf("the checkpoint took %d bytes; it asked for %d", info.Size(), need)
		})
	}
}

// A node whose disk has no room for its next checkpoint beside the one it
// holds raises NOSPACE and stays up, as etcd's past its quota: it refuses
// puts and serves reads, deletes, compactions, defrags and the alarm's
// disarm, and writes no checkpoint. Once there is room and the alarm is
// disarmed, puts are taken and checkpoints written again. Before #769 the
// node wrote the checkpoint anyway, stopped on ENOSPC halfway through it,
// and stopped again at every start.
func TestACheckpointWithoutRoomRaisesNOSPACEAndTheNodeServes(t *testing.T) {
	var room atomic.Int64
	was := freeSpaceOf
	freeSpaceOf = func(path string) (int64, error) {
		if filepath.Base(path) == "ckpt" {
			return room.Load(), nil
		}
		return was(path)
	}
	t.Cleanup(func() { freeSpaceOf = was })
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) { c.CheckpointLog = 32 << 10 }
	})
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	value := strings.Repeat("v", 4<<10)
	var err error
	put := 0
	for ; put < 256; put++ {
		if _, err = c.Put(ctx, fmt.Sprintf("/r/%03d", put), value); err != nil {
			break
		}
	}
	if !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("after %d puts of 4 KiB, %d KiB of log a checkpoint, on disks with no room for one: %v", put, 32, err)
	}
	// Every node found no room, and raises its own NOSPACE.
	alarms := waitAlarms(ctx, t, c, len(tc.ids))
	ptrs := make([]string, len(tc.ids))
	for i := range tc.ids {
		ptrs[i] = currentCkpt(t, tc, i)
	}
	if _, err := c.Put(ctx, "/r/small", "x"); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a put while NOSPACE is up: %v", err)
	}
	r, err := c.Get(ctx, "/r/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil || r.Count != int64(put) {
		t.Fatalf("a read while NOSPACE is up: %v %v", r, err)
	}
	d, err := c.Delete(ctx, "/r/", clientv3.WithPrefix())
	if err != nil || d.Deleted != int64(put) {
		t.Fatalf("a delete while NOSPACE is up: %v %v", d, err)
	}
	if _, err := c.Compact(ctx, d.Header.Revision, clientv3.WithCompactPhysical()); err != nil {
		t.Fatalf("a compaction while NOSPACE is up: %v", err)
	}
	for _, ep := range tc.endpoints {
		if _, err := c.Defragment(ctx, ep); err != nil {
			t.Fatalf("a defrag of %s while NOSPACE is up: %v", ep, err)
		}
	}
	for i := range tc.ids {
		select {
		case err := <-tc.stopped[i]:
			t.Fatalf("node %d stopped with NOSPACE up: %v", i+1, err)
		default:
		}
		if p := currentCkpt(t, tc, i); p != ptrs[i] {
			t.Fatalf("node %d wrote checkpoint %s on a disk with no room for it", i+1, p)
		}
	}
	room.Store(1 << 40)
	// A node may raise its NOSPACE again from a check before the room
	// came: disarm until none is left.
	for len(alarms.Alarms) > 0 {
		for _, a := range alarms.Alarms {
			if _, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: a.MemberID, Alarm: a.Alarm}); err != nil {
				t.Fatal(err)
			}
		}
		if alarms, err = c.AlarmList(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; ; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("/r/after/%03d", i), value); err != nil {
			t.Fatalf("a put once there is room and the alarm is disarmed: %v", err)
		}
		moved := 0
		for j := range tc.ids {
			if currentCkpt(t, tc, j) != ptrs[j] {
				moved++
			}
		}
		if moved == len(tc.ids) {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("after %d puts once there is room, %d of %d nodes wrote a checkpoint", i+1, moved, len(tc.ids))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitAlarms waits until n NOSPACE alarms are up and returns the list.
func waitAlarms(ctx context.Context, t *testing.T, c *clientv3.Client, n int) *clientv3.AlarmResponse {
	t.Helper()
	for {
		alarms, err := c.AlarmList(ctx)
		if err == nil && len(alarms.Alarms) >= n && !slices.ContainsFunc(alarms.Alarms, func(a *etcdpb.AlarmMember) bool { return a.Alarm != etcdpb.AlarmType_NOSPACE }) {
			return alarms
		}
		if ctx.Err() != nil {
			t.Fatalf("waiting for %d NOSPACE alarms: %v %v", n, alarms, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// currentCkpt is the checkpoint node i's CURRENT names, "" for none.
func currentCkpt(t *testing.T, tc *testCluster, i int) string {
	t.Helper()
	p, err := readPointer(sysEnv{}, filepath.Join(tc.dataDir(i), "ckpt"))
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		return ""
	}
	return p.name
}

// A checkpoint whose write meets ENOSPC, the room counted wrong or taken
// meanwhile, is dropped: its file is removed, the checkpoint before stays
// current, the node raises NOSPACE and goes on, and tries again a while
// later, the checkpoint still wanted, so that a client's Snapshot that
// waited for it gets the next. Before #769 the error stopped the node,
// and its file stayed on the full disk.
func TestACheckpointThatRunsOutOfRoomIsDropped(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skipf("no /dev/full, whose writes fail with ENOSPC: %v", err)
	}
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/a", strings.Repeat("v", 1000))
	writeCkpt(t, n, 3)
	cur := n.ckptName
	putValue(t, n.st, l, "/b", strings.Repeat("v", 1000))
	n.now, n.applied = time.Unix(1000, 0), 5
	n.id, n.ring = 1, newTestRing(t) // which the alarm's proposal takes
	// The checkpoint's file is /dev/full.
	tmp := filepath.Join(n.ckptDir, ckptName(5)+".tmp")
	if err := os.Symlink("/dev/full", tmp); err != nil {
		t.Fatal(err)
	}
	n.snapWaiters = []handle{7} // a client's Snapshot
	if err := n.startCkpt(5, 1, 0); err != nil {
		t.Fatal(err)
	}
	n.storage.wantCkpt = false // as maybeCkpt leaves it
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatalf("a checkpoint that met ENOSPC stopped the node: %v", err)
		}
	}
	if !slices.Equal(n.snapWaiters, []handle{7}) || !n.storage.wantCkpt {
		t.Fatalf("after the dropped checkpoint the Snapshots %v wait, and a checkpoint is wanted: %v", n.snapWaiters, n.storage.wantCkpt)
	}
	if n.ckptName != cur {
		t.Fatalf("the current checkpoint is %s, want %s", n.ckptName, cur)
	}
	if p, err := readPointer(sysEnv{}, n.ckptDir); err != nil || p == nil || p.name != cur {
		t.Fatalf("CURRENT after the dropped checkpoint: %+v %v, want %s", p, err, cur)
	}
	for _, name := range []string{tmp, filepath.Join(n.ckptDir, ckptName(5))} {
		if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the dropped checkpoint left %s: %v", name, err)
		}
	}
	if n.noSpaceAsked != n.applied || n.batchOps == 0 {
		t.Fatal("the dropped checkpoint raised no NOSPACE")
	}
	if !n.ckptRoomAt.Equal(n.now.Add(ckptRoomRetry)) {
		t.Fatalf("the next checkpoint may start at %v, want %v", n.ckptRoomAt, n.now.Add(ckptRoomRetry))
	}
	n.snapWaiters = nil
	writeCkptAt(t, n, 6)
	if n.ckptName != ckptName(6) {
		t.Fatalf("the checkpoint after is %s, want %s", n.ckptName, ckptName(6))
	}
}

// A start removes the checkpoints a node was writing when it stopped,
// before it writes anything: half written, or whole but not yet named by
// CURRENT. One left when the disk filled took the room the start's own
// files need, and the node stopped again at every start (#769). It keeps
// CURRENT and the checkpoint it names.
func TestAStartDropsHalfWrittenCheckpoints(t *testing.T) {
	data := t.TempDir()
	if err := cleanCkptDirAtStart(sysEnv{}, data); err != nil {
		t.Fatalf("a data dir with no checkpoint dir: %v", err)
	}
	dir := filepath.Join(data, "ckpt")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writePointer(sysEnv{}, dir, &ckptPointer{name: ckptName(3)}); err != nil {
		t.Fatal(err)
	}
	keep := []string{ckptName(3), ptrName}
	for _, name := range []string{ckptName(3), ckptName(9), ckptName(11) + ".tmp", ptrName + ".tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanCkptDirAtStart(sysEnv{}, data); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	slices.Sort(keep)
	if !slices.Equal(names, keep) {
		t.Fatalf("the checkpoint dir holds %v, want %v", names, keep)
	}
}
