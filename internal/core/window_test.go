package core

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// memCluster runs three cores in memory: messages and disk writes are
// queues that a test can hold back. Nothing is written anywhere.
type memCluster struct {
	t     *testing.T
	nodes []*Node
	jobs  [][]PersistJob
	msgs  []*raftpb.Message
	// holdAcks keeps the followers' append responses from the leader, so
	// nothing commits; holdDisk keeps a node's disk writes from finishing.
	holdAcks bool
	holdDisk [3]bool
	// down marks nodes that neither run nor get messages, and handOffs
	// collects Effects.HandOff.
	down     [3]bool
	handOffs []uint64
	replies  int
	// noSpace counts NOSPACE answers, the one error a node may give.
	noSpace int
}

func newMemCluster(t *testing.T, change func(*Config)) *memCluster {
	c := &memCluster{t: t, jobs: make([][]PersistJob, 3)}
	for id := uint64(1); id <= 3; id++ {
		cfg := DefaultConfig(id, []uint64{1, 2, 3})
		change(&cfg)
		n, err := New(cfg, &wal.Recovered{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
	}
	for range 100 {
		c.nodes[0].Tick()
		c.step()
		if c.nodes[0].isLeader {
			return c
		}
	}
	t.Fatal("node 1 did not become leader")
	return nil
}

// step processes every node once, delivers the messages that are not
// held, and finishes the disk writes that are not held.
func (c *memCluster) step() {
	for i, n := range c.nodes {
		if c.down[i] {
			continue
		}
		eff, err := n.Process()
		if err != nil {
			c.t.Fatalf("node %d: %v", i+1, err)
		}
		if eff.HandOff != 0 {
			c.handOffs = append(c.handOffs, eff.HandOff)
		}
		c.jobs[i] = append(c.jobs[i], eff.Persist...)
		c.msgs = append(c.msgs, eff.Send...)
		for _, r := range eff.Replies {
			if errors.Is(r.Err, rpctypes.ErrGRPCNoSpace) {
				c.noSpace++
				continue
			}
			if r.Err != nil {
				c.t.Fatalf("node %d answered %d with %v", i+1, r.ID, r.Err)
			}
			c.replies++
		}
	}
	msgs := c.msgs
	c.msgs = nil
	for _, m := range msgs {
		if c.holdAcks && m.GetType() == raftpb.MsgAppResp {
			c.msgs = append(c.msgs, m)
			continue
		}
		if c.down[m.GetTo()-1] || c.down[m.GetFrom()-1] {
			continue
		}
		if err := c.nodes[m.GetTo()-1].Step(m); err != nil {
			c.t.Fatalf("step %s: %v", m.GetType(), err)
		}
	}
	for i, n := range c.nodes {
		if c.holdDisk[i] {
			continue
		}
		for _, j := range c.jobs[i] {
			n.Persisted(j)
		}
		c.jobs[i] = nil
	}
}

// TestWindowBound writes 1 GiB of log through three nodes and checks
// after every step that no node holds more entry bytes in memory than
// its window maximum plus its budget for entries not applied yet, plus
// one entry: while all runs well, while the leader gets no
// acknowledgements and nothing commits, and while the leader's or a
// follower's disk holds back its writes and so what it applies.
func TestWindowBound(t *testing.T) {
	const (
		window, windowMax, unapplied = 8 << 20, 16 << 20, 16 << 20
		valueBytes                   = 256 << 10
		total                        = 1 << 30
	)
	c := newMemCluster(t, func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes, cfg.UnappliedBytes = window, windowMax, unapplied
		cfg.MaxBatchBytes = 1 << 20
		cfg.CheckpointBytes = 1 << 40
	})
	leader := c.nodes[0]
	value := make([]byte, valueBytes)
	limit := int64(windowMax + unapplied + leader.cfg.MaxBatchBytes + batchFraming)
	var sent, peak int64
	id := uint64(0)
	for step := 0; c.replies*valueBytes < total; step++ {
		// Hold something back for 150 steps out of every 400.
		phase := step % 400
		c.holdAcks = phase >= 50 && phase < 200 && step/400%3 == 0
		c.holdDisk[0] = phase >= 50 && phase < 200 && step/400%3 == 1
		c.holdDisk[1] = phase >= 50 && phase < 200 && step/400%3 == 2
		for int(id)-c.replies < 256 {
			id++
			leader.Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id%1000), Value: value}})
			sent += valueBytes
		}
		c.step()
		for i, n := range c.nodes {
			b := n.EntryBytes()
			peak = max(peak, b)
			if b > limit {
				t.Fatalf("step %d: node %d holds %d entry bytes in memory, more than %d", step, i+1, b, limit)
			}
		}
		if step > 200000 {
			t.Fatalf("only %d of %d bytes applied after %d steps", c.replies*valueBytes, total, step)
		}
	}
	t.Logf("%d MiB written; at most %d MiB of entries in memory on a node, against a bound of %d MiB", c.replies*valueBytes>>20, peak>>20, limit>>20)
}

// TestRestartFromCheckpoint writes and compacts with a tiny window and
// frequent checkpoints, so that old segments go, then restarts the node
// from its disk: it loads the newest checkpoint, applies the log after it,
// and holds the same state and every value, read from disk.
func TestRestartFromCheckpoint(t *testing.T) {
	tiny := func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
		cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
	}
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, tiny)
	id := uint64(0)
	for round := range 30 {
		var rev int64
		for k := range 10 {
			id++
			r := s.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: fmt.Appendf(nil, "value %d of round %d", k, round)})
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			rev = s.n.kv.Rev()
		}
		id++
		if r := s.do(id, &pb.CompactionRequest{Revision: rev}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	root := s.n.DurableCheckpoint()
	if root.Index == 0 {
		t.Fatal("no checkpoint became durable")
	}
	if names := disk.DurableNames(); names[0] == "0000000000000001.wal" {
		t.Fatalf("no segment was removed: %v", names)
	}
	want, applied := s.n.Hash(), s.n.Applied()
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}
	s = startSingleWith(t, disk, 2, tiny)
	if s.n.Applied() < applied || s.n.DurableCheckpoint() != root {
		t.Fatalf("restarted at %d with root %+v, want %d and %+v", s.n.Applied(), s.n.DurableCheckpoint(), applied, root)
	}
	if s.n.Applied() == applied && s.n.Hash() != want {
		t.Fatal("the restarted node's state differs")
	}
	id++
	r := s.do(id, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	kvs := r.Resp.(*pb.RangeResponse).Kvs
	for k, kv := range kvs {
		if string(kv.Value) != fmt.Sprintf("value %d of round 29", k) {
			t.Fatalf("k%d is %q after the restart", k, kv.Value)
		}
	}
	if len(kvs) != 10 {
		t.Fatalf("%d keys after the restart", len(kvs))
	}
}

// walSink writes a received snapshot into a WAL, as the drivers do.
type walSink struct {
	w    *wal.WAL
	ckpt wal.File
}

func (k *walSink) Entries(ents []*raftpb.Entry) error { k.w.Append(nil, ents); return nil }

func (k *walSink) Value(index uint64, off uint32, value []byte) error {
	k.w.AppendValue(index, off, value)
	return nil
}

func (k *walSink) CheckpointChunk(index uint64, data []byte) error {
	if k.ckpt == nil {
		f, err := k.w.CreateCheckpoint(index)
		if err != nil {
			return err
		}
		k.ckpt = f
	}
	_, err := k.ckpt.Write(data)
	return err
}

func (k *walSink) CheckpointDone(uint64) error {
	err := errors.Join(k.ckpt.Sync(), k.ckpt.Close(), k.w.SyncDir())
	k.ckpt = nil
	return err
}

func (k *walSink) Root(r wal.Root) error { k.w.AppendRoot(r); return nil }

// TestSnapshotStream serves two snapshots from a node whose old segments
// are gone: from index 1, its checkpoint, the values it names and the
// entries after it; then, after more writes, the raw entries the
// follower lacks. The follower installs each into its log and reaches the
// source's state. Every damaged byte of a stream, and a stream for
// another index or term, is refused.
func TestSnapshotStream(t *testing.T) {
	tiny := func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
		cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
	}
	src := startSingleWith(t, simfs.New(), 1, tiny)
	id := uint64(0)
	write := func(rounds int, compact bool) {
		for round := range rounds {
			for k := range 10 {
				id++
				if r := src.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: fmt.Appendf(nil, "value %d of round %d", k, round)}); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			if compact {
				id++
				if r := src.do(id, &pb.CompactionRequest{Revision: src.n.kv.Rev()}); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
		}
	}
	// install receives a stream on a copy of disk and starts a node on it.
	install := func(disk *simfs.Disk, data []byte, in *Install) (*Node, *simfs.Disk, error) {
		disk = disk.Durable()
		w, _, err := wal.Open(disk)
		if err != nil {
			t.Fatal(err)
		}
		if err := ReceiveSnapshot(bytes.NewReader(data), &walSink{w: w}, w, in); err != nil {
			return nil, nil, err
		}
		w.Append(&wal.HardState{Term: in.Term, Commit: in.Index}, nil)
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		w, rec, err := wal.Open(disk)
		if err != nil {
			t.Fatal(err)
		}
		cfg := DefaultConfig(1, []uint64{1})
		tiny(&cfg)
		n, err := New(cfg, rec, w)
		return n, disk, err
	}
	follower := simfs.New()
	write(30, true)
	if src.log.Has(1) {
		t.Fatal("the source still has entry 1")
	}
	for phase, applied := range []uint64{0, 0} {
		if phase == 1 {
			applied = src.n.Applied()
			write(5, false)
		}
		last := src.n.Applied()
		term, err := src.n.termAt(last)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := ServeSnapshot(src.log, []wal.Root{src.n.DurableCheckpoint()}, applied+1, last, &buf); err != nil {
			t.Fatal(err)
		}
		stream := buf.Bytes()
		in := &Install{From: 1, Index: last, Term: term, Applied: applied}
		n, disk, err := install(follower, stream, in)
		if err != nil {
			t.Fatalf("from %d: %v", applied+1, err)
		}
		if (phase == 0) != (in.Checkpoint.Index > 0) {
			t.Fatalf("from %d: the stream carried the checkpoint %+v", applied+1, in.Checkpoint)
		}
		if n.Applied() != last || n.Hash() != src.n.Hash() {
			t.Fatalf("from %d: the follower is at %d with another state", applied+1, n.Applied())
		}
		for _, bad := range []*Install{{Index: last + 1, Term: term, Applied: applied}, {Index: last, Term: term + 1, Applied: applied}} {
			if _, _, err := install(follower, stream, bad); err == nil {
				t.Fatalf("from %d: a stream for %d term %d installed as %d term %d", applied+1, last, term, bad.Index, bad.Term)
			}
		}
		for at := 0; at < len(stream); at += 1 + len(stream)/200 {
			bad := bytes.Clone(stream)
			bad[at] ^= 0x10
			if _, _, err := install(follower, bad, &Install{Index: last, Term: term, Applied: applied}); err == nil {
				t.Fatalf("from %d: a stream damaged at byte %d of %d installed", applied+1, at, len(stream))
			}
		}
		follower = disk
	}
}

// keepSink is a walSink that notes the first entry it appends and checks,
// at each root, that a crash then still leaves the entries up to held.
type keepSink struct {
	*walSink
	t     *testing.T
	disk  *simfs.Disk
	held  uint64
	first uint64
	roots []wal.Root
}

func (k *keepSink) Entries(ents []*raftpb.Entry) error {
	if len(ents) > 0 && k.first == 0 {
		k.first = ents[0].GetIndex()
	}
	return k.walSink.Entries(ents)
}

func (k *keepSink) Root(r wal.Root) error {
	k.roots = append(k.roots, r)
	if err := k.walSink.Root(r); err != nil {
		return err
	}
	if err := k.w.Sync(); err != nil {
		return err
	}
	_, rec, err := wal.Open(k.disk.Durable())
	if err != nil {
		return err
	}
	if rec.Last < k.held {
		k.t.Errorf("a crash after the root at %d keeps entries to %d, below the %d the follower held", r.Index, rec.Last, k.held)
	}
	return nil
}

// TestServeOlderCheckpoint serves a snapshot that Raft sent at a
// checkpoint after a newer root became durable, while the older
// checkpoint's file is still on disk because a fetch held back the
// removal. The newest checkpoint alone cannot serve it; with both, the
// stream carries the older one and the follower installs it.
func TestServeOlderCheckpoint(t *testing.T) {
	src := startSingleWith(t, simfs.New(), 1, func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
		cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
	})
	id := uint64(0)
	round := func() {
		for k := range 10 {
			id++
			if r := src.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: fmt.Appendf(nil, "value %d of write %d", k, id)}); r.Err != nil {
				t.Fatal(r.Err)
			}
		}
		id++
		if r := src.do(id, &pb.CompactionRequest{Revision: src.n.kv.Rev()}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	for range 30 {
		round()
	}
	older := src.n.DurableCheckpoint()
	if older.Index == 0 || src.log.Has(1) {
		t.Fatalf("setup: checkpoint at %d, entry 1 on disk %v", older.Index, src.log.Has(1))
	}
	src.keep = true
	for range 100 {
		if src.n.DurableCheckpoint() != older {
			break
		}
		round()
	}
	newer := src.n.DurableCheckpoint()
	if newer == older {
		t.Fatal("setup: no newer checkpoint")
	}
	var buf bytes.Buffer
	if err := ServeSnapshot(src.log, []wal.Root{newer}, 1, older.Index, &buf); !errors.Is(err, errNoSnapshot) {
		t.Fatalf("served the snapshot at %d from the checkpoint at %d alone: %v", older.Index, newer.Index, err)
	}
	buf.Reset()
	if err := ServeSnapshot(src.log, []wal.Root{older, newer}, 1, older.Index, &buf); err != nil {
		t.Fatal(err)
	}
	w, _, err := wal.Open(simfs.New())
	if err != nil {
		t.Fatal(err)
	}
	in := &Install{From: 1, Index: older.Index, Term: older.Term}
	if err := ReceiveSnapshot(bytes.NewReader(buf.Bytes()), &walSink{w: w}, w, in); err != nil {
		t.Fatal(err)
	}
	if in.Checkpoint != older {
		t.Fatalf("the stream carried the checkpoint %+v, want %+v", in.Checkpoint, older)
	}
}

// TestInstallKeepsHeldEntries installs snapshots on a follower that holds
// entries past its applied index, as one whose commit index trails does.
// It may have acknowledged them, so the install must keep them durable:
// it skips them, the root keeps them, and the first entry it appends is
// the first it lacked. Once by raw entries, once by a checkpoint.
func TestInstallKeepsHeldEntries(t *testing.T) {
	tiny := func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
		cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
	}
	src := startSingleWith(t, simfs.New(), 1, tiny)
	id := uint64(0)
	write := func(rounds int, compact bool) {
		for round := range rounds {
			for k := range 10 {
				id++
				if r := src.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: fmt.Appendf(nil, "value %d of round %d", k, round)}); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			if compact {
				id++
				if r := src.do(id, &pb.CompactionRequest{Revision: src.n.kv.Rev()}); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
		}
	}
	write(30, true)
	ckpt := src.n.DurableCheckpoint()
	held := src.n.Applied()
	follower := src.disk.Durable()
	write(1, false)
	last := src.n.Applied()
	term, err := src.n.termAt(last)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt.Index == 0 || held <= ckpt.Index || src.n.DurableCheckpoint() != ckpt || src.log.Has(1) {
		t.Fatalf("setup: checkpoint at %d then %d, held to %d, entry 1 on disk %v", ckpt.Index, src.n.DurableCheckpoint().Index, held, src.log.Has(1))
	}
	for _, applied := range []uint64{ckpt.Index, 0} {
		var buf bytes.Buffer
		if err := ServeSnapshot(src.log, []wal.Root{ckpt}, applied+1, last, &buf); err != nil {
			t.Fatal(err)
		}
		disk := follower.Durable()
		if applied == 0 {
			// The follower's copy has the leader's checkpoint, which a
			// follower that applied less never has.
			if err := disk.Remove(wal.CheckpointName(ckpt.Index)); err != nil {
				t.Fatal(err)
			}
		}
		w, _, err := wal.Open(disk)
		if err != nil {
			t.Fatal(err)
		}
		if applied == 0 && !w.Has(ckpt.Index) {
			t.Fatalf("setup: the follower lacks the checkpoint's entry %d", ckpt.Index)
		}
		sink := &keepSink{walSink: &walSink{w: w}, t: t, disk: disk, held: held}
		in := &Install{From: 1, Index: last, Term: term, Applied: applied}
		if err := ReceiveSnapshot(bytes.NewReader(buf.Bytes()), sink, w, in); err != nil {
			t.Fatalf("from %d: %v", applied+1, err)
		}
		if sink.first != held+1 {
			t.Errorf("from %d: the first entry appended is %d, the follower held to %d", applied+1, sink.first, held)
		}
		if applied == 0 && (len(sink.roots) != 1 || sink.roots[0].Keep != held) {
			t.Errorf("from %d: roots %+v, want one that keeps entries to %d", applied+1, sink.roots, held)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, rec, err := wal.Open(disk.Durable()); err != nil || rec.Last != last {
			t.Errorf("from %d: the log ends at %d, want %d (%v)", applied+1, rec.Last, last, err)
		}
	}
}

// TestRepliesCopyValues checks that the values a reply carries, current
// and previous, are not slices of their Raft entries, which are still in
// memory: a reply can wait behind a slow client long after its entry left
// the window.
func TestRepliesCopyValues(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, nil)
	put := func(id uint64, value string) (Reply, []byte) {
		r := s.do(id, &pb.PutRequest{Key: []byte("k"), Value: []byte(value), PrevKv: true})
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		data, ok := s.n.storage.entryData(s.n.Applied())
		if !ok {
			t.Fatal("the entry left memory")
		}
		return r, data
	}
	// shared reports whether value changes with the entry's bytes.
	shared := func(value, entry []byte) bool {
		i := bytes.Index(entry, value)
		if i < 0 {
			t.Fatalf("value %q is not in its entry", value)
		}
		entry[i] ^= 0xff
		changed := value[0] != entry[i]^0xff
		entry[i] ^= 0xff
		return changed
	}
	_, first := put(1, "first value")
	r, second := put(2, "second value")
	if prev := r.Resp.(*pb.PutResponse).GetPrevKv().GetValue(); shared(prev, first) {
		t.Fatal("the previous value of a put is a slice of its entry")
	}
	r = s.do(3, &pb.RangeRequest{Key: []byte("k"), Serializable: true})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if v := r.Resp.(*pb.RangeResponse).Kvs[0].Value; shared(v, second) {
		t.Fatal("a range's value is a slice of its entry")
	}
}
