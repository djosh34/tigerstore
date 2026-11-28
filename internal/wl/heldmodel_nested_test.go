package wl

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Exercise actual nested execution and flattening of held reply records,
// including an explicitly old range, put prev_kv, and delete prev_kv.
func (m *heldModel) nested() {
	st := m.n.st
	r := m.low() + m.rng.Int63n(st.rev-m.low()+1)
	a := m.key()
	b := m.key()
	for b == a {
		b = m.key()
	}
	v := fmt.Sprintf("%s@%d", a, st.rev+1)
	rangeOp := func(rev int64) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("/"), RangeEnd: []byte{0}, Revision: rev}}}
	}
	inner := &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(a), Value: []byte(v), PrevKv: true}}},
		rangeOp(0),
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte(b), PrevKv: true}}},
	}}
	req := &pb.TxnRequest{Success: []*pb.RequestOp{rangeOp(r), {Request: &pb.RequestOp_RequestTxn{RequestTxn: inner}}, rangeOp(0)}}
	wire, err := proto.Marshal(req)
	if err != nil {
		m.fail("marshal nested: %v", err)
		return
	}
	if err := checkRequest(kTxn, wire, 0); err != nil {
		m.fail("validate nested: %v", err)
		return
	}
	tr := txnRun{req: wire, voff: m.l.add(string(wire)), rev: st.rev + 1, reply: true}
	if err := m.n.checkTxnOps(wire, true, &tr.path); err != nil {
		m.fail("nested preflight: %v", err)
		return
	}
	r0, r1, err := m.n.runBranch(&tr, wire, true, 0)
	if err != nil {
		m.fail("nested apply: %v", err)
		return
	}
	next := maps.Clone(m.states[st.rev])
	next[a] = v
	delete(next, b)
	st.rev = tr.rev
	m.states[st.rev] = next
	m.puts[st.rev] = map[string]string{a: v}
	defer st.endApply() // the production reply/hold is built before apply frees old records
	expected := slices.Clone(tr.kvs)
	for _, x := range tr.res {
		if x.kind == kPut && x.prev != nil {
			expected = append(expected, x.prev)
		}
	}
	m.sid += 2
	m.n.holdTxn(m.c, m.c.newStream(m.sid, kTxn), true, tr.res, r0, r1, tr.kvs)
	hr := m.n.heldReplies[len(m.n.heldReplies)-1]
	h := &modelHold{kind: "reply", rev: st.rev, hr: hr}
	for _, rec := range expected {
		key, mod, _ := recIdent(rec)
		h.reads = append(h.reads, modelRead{key: string(key), rev: mod, put: true, want: m.puts[mod][string(key)]})
	}
	if m.taken(h) {
		m.holds = append(m.holds, h)
	}
}

// Installs terminate old-store holders: they are not reads which survive
// the replacement. Continue the model on the installed store afterward.
func (m *heldModel) install(step int) {
	m.installing = true
	m.checkpoint(step)
	m.installing = false
	if m.failed != "" {
		return
	}
	n := m.n
	src, err := os.Open(filepath.Join(n.ckptDir, n.ckptName))
	if err != nil {
		m.fail("snapshot source: %v", err)
		return
	}
	path := filepath.Join(n.ckptDir, "received.tmp")
	dst, err := os.Create(path)
	if err != nil {
		_ = src.Close()
		m.fail("snapshot dest: %v", err)
		return
	}
	err = selfContained(context.Background(), src, n.st.log.(durableLog), dst)
	_ = src.Close()
	_ = dst.Close()
	if err != nil {
		m.fail("snapshot self contained: %v", err)
		return
	}
	if n.wal == nil {
		n.wal, err = openWAL(sysEnv{}, filepath.Join(n.ckptDir, "wal"), sizeFor(0).stage, 0, 0, nil)
		if err != nil {
			m.fail("snapshot wal: %v", err)
			return
		}
		m.t.Cleanup(n.wal.close)
	}
	want := n.st.stateHash()
	old := n.st
	m.index++
	sn := &raftpb.Snapshot{Data: []byte(path), Metadata: &raftpb.SnapshotMetadata{Index: new(m.index), Term: new(uint64(1)), ConfState: n.confState}}
	if err := n.installSnapshot(sn); err != nil {
		m.fail("snapshot install: %v", err)
		return
	}
	if !old.closed || n.st == old || n.st.stateHash() != want {
		m.fail("snapshot replacement/hash")
		return
	}
	for _, h := range m.holds {
		if h.hr != nil {
			n.endHeld(h.hr)
		}
	}
	m.holds = nil
	n.heldReplies = nil
	if n.st.heldReads() != 0 || len(n.st.heldStates) != 0 {
		m.fail("snapshot inherited holds")
	}
	m.check(step)
}

// Every range holder depends on the ordered tree, not just hash lookups;
// watches depend on the change index, not just direct putAt calls.
func (m *heldModel) checkIndexes(step int) {
	st := m.n.st
	for _, h := range m.holds {
		if h.hr != nil {
			if len(h.hr.refs) != len(h.reads) {
				m.fail("step %d held refs count %d want %d", step, len(h.hr.refs), len(h.reads))
				return
			}
			for i, ref := range h.hr.refs {
				rec, err := st.putAt(ref.id, ref.mod)
				if ref.rec != nil {
					rec, err = ref.rec, nil
				}
				if err != nil || string(rec) != h.reads[i].record {
					m.fail("step %d held reply ref %d (%s@%d): got %x want %x: %v", step, i, h.reads[i].key, h.reads[i].rev, rec, h.reads[i].record, err)
					return
				}
			}
		}
		if h.kind == "range" {
			got := map[string]string{}
			st.tree.scan([]byte("/"), nil, func(id uint32) bool {
				rec, err := st.at(id, h.rev)
				if err != nil {
					m.fail("tree step %d: %v", step, err)
					return false
				}
				if rec != nil {
					got[string(st.key(id))] = string(recValue(rec))
				}
				return true
			})
			if !maps.Equal(got, m.states[h.rev]) {
				m.fail("tree step %d at %d: %v want %v", step, h.rev, got, m.states[h.rev])
			}
		}
		if h.kind == "watch" {
			for _, key := range heldModelKeys {
				if m.states[h.rev][key] != "" || (m.states[h.rev-1][key] == "" && m.puts[h.rev][key] == "") {
					continue
				}
				found := false
				for _, ch := range st.changes {
					if ch.rev == h.rev && ch.del && string(st.key(ch.key)) == key {
						found = true
					}
				}
				if !found {
					m.fail("watch step %d: missing delete change %s at %d", step, key, h.rev)
				}
			}
			for _, r := range h.reads {
				if !r.put {
					continue
				}
				found := false
				for _, ch := range st.changes {
					if ch.rev == h.rev && !ch.del && string(st.key(ch.key)) == r.key {
						found = true
					}
				}
				if !found {
					m.fail("watch step %d: missing put change %s at %d", step, r.key, h.rev)
				}
			}
		}
	}
}

// Held states let go of their map's memory as they are released: one
// state left of 200,000 holds no more than its own (the invariant
// re-check, Astra: the map kept 7 MB that nothing counted).
func TestReleasedHeldStatesGiveTheirMemoryBack(t *testing.T) {
	st, l := newTestStore(t)
	_ = l
	for i := range 200000 {
		st.holdState(0, int64(i))
	}
	for i := 1; i < 200000; i++ {
		st.releaseState(0, int64(i))
	}
	runtime.GC()
	var held, released runtime.MemStats
	runtime.ReadMemStats(&held)
	st.releaseState(0, 0)
	runtime.GC()
	runtime.ReadMemStats(&released)
	retained := int64(held.HeapAlloc) - int64(released.HeapAlloc)
	t.Logf("one remaining held state charged %d bytes; releasing it frees %d heap bytes", heldStateSize, retained)
	if retained > 1<<20 {
		t.Fatalf("one state retained %d bytes of map backing after the other 199999 charges were returned", retained)
	}
}

// An install's cleanup of the old checkpoints and the remover that
// ckptDone hands the replaced one may race for a file: the cleanup takes
// a file already gone as removed (the invariant re-check, Astra: the
// install failed after it had replaced the store).
func TestAnInstallsCleanupToleratesTheRemover(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	for i := range 4096 {
		p := filepath.Join(dir, ckptName(uint64(i+1)))
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	rm := newRemover(sysEnv{}, func() {})
	submitted := make(chan struct{})
	go func() {
		for _, p := range paths {
			rm.remove(p)
		}
		close(submitted)
	}()
	err := removeCkpts(sysEnv{}, dir, "CURRENT-SNAPSHOT.ckpt", false)
	<-submitted
	rm.close()
	if err != nil {
		t.Fatalf("install cleanup raced checkpoint remover: %v", err)
	}
}

// A snapshot install ends the old store's held reads: a large range, a
// reply waiting for the reserve and a large watch's piece go no further
// on the replaced store, which is closed. Holds are of one live store.
func TestASnapshotInstallEndsTheOldStoresHeldReads(t *testing.T) {
	n, sn := installFixture(t)
	defer func() { _ = n.st.close() }()
	n.sizes, n.connOut = sizeFor(256<<20), 64<<10
	c, _ := flowClient(t)
	c.node = n
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	old := n.st
	rs := c.newStream(3, kRange)
	b := &bigRange{c: c, h: mkHandle(c, rs.id), st: old, req: rangeReq{key: []byte("/"), end: []byte{0}}}
	if !n.planBig(b, c, rs) {
		t.Fatal("range not planned")
	}
	id, _ := old.find([]byte("/follower"))
	rec, err := old.live(id)
	if err != nil {
		t.Fatal(err)
	}
	n.holdPut(c, c.newStream(5, kPut), rec)
	hr := n.heldReplies[0]
	ws := c.newStream(7, kWatch)
	ws.watch = true
	wi := n.w.add(watcher{sock: 0, gen: 1, sid: ws.id, id: 1, key: []byte("/follower"), syncRev: old.rev})
	ws.wids = append(ws.wids, wi)
	if !n.startBigWatch(wi, &n.w.ws[wi], c, ws, revStart(old, old.rev)) {
		t.Fatal("watch not started")
	}
	bw := n.bigWatches[0]
	if err := n.installSnapshot(sn); err != nil {
		t.Fatal(err)
	}
	if !old.closed {
		t.Fatal("old store not closed")
	}
	rangeWait, replyWait, watchWait := n.stepBig(b), n.stepHeld(hr), n.stepBigWatch(bw)
	t.Logf("after install: range continues=%v reply continues=%v watch piece continues=%v old store closed=%v", rangeWait, replyWait, watchWait, old.closed)
	if rangeWait || replyWait || watchWait {
		t.Fatal("old-store holder continued")
	}
	n.endBig(b)
	n.endHeld(hr)
	n.endBigWatch(bw)
}
