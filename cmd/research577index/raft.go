package main

import (
	"fmt"
	"io"
	"log"
	"runtime"

	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type cluster struct {
	nodes   [4]*raft.RawNode
	stores  [4]*raft.MemoryStorage
	inbox   [4][]*pb.Message
	applied [4]uint64
	reads   int
}

func newCluster() *cluster {
	c := &cluster{}
	logger := &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	for i := 1; i <= 3; i++ {
		c.inbox[i] = make([]*pb.Message, 0, 16384)
		st := raft.NewMemoryStorage()
		check(st.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}))
		check(st.SetHardState(&pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}))
		c.stores[i] = st
		var err error
		c.nodes[i], err = raft.NewRawNode(&raft.Config{ID: uint64(i), Storage: st, Applied: 1, AsyncStorageWrites: true, ElectionTick: 10, HeartbeatTick: 1, MaxSizePerMsg: 4 << 20, MaxCommittedSizePerReady: 1 << 30, MaxInflightMsgs: 256, MaxInflightBytes: 1 << 30, CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe, Logger: logger})
		check(err)
		c.applied[i] = 1
	}
	check(c.nodes[1].Campaign())
	c.pump(nil)
	if c.nodes[1].BasicStatus().Lead != 1 {
		panic("no leader")
	}
	return c
}
func (c *cluster) route(id int, m *pb.Message) {
	if m.GetTo() == uint64(id) {
		check(c.nodes[id].Step(m))
		return
	}
	to := int(m.GetTo())
	if to < 1 || to > 3 {
		panic("invalid destination")
	}
	c.inbox[to] = append(c.inbox[to], m)
}

// Drain the async append/apply messages and their attached response messages.
// AsyncStorageWrites forbids Advance: MsgStorage{Append,Apply}Resp replaces it.
func (c *cluster) serve(id int) {
	n := c.nodes[id]
	in := c.inbox[id]
	c.inbox[id] = in[:0]
	for i, m := range in {
		check(n.Step(m))
		in[i] = nil
	}
	for n.HasReady() {
		rd := n.Ready()
		if id == 1 {
			c.reads += len(rd.ReadStates)
		}
		for _, m := range rd.Messages {
			switch m.GetType() {
			case pb.MsgStorageAppend:
				if !raft.IsEmptySnap(m.GetSnapshot()) {
					check(c.stores[id].ApplySnapshot(m.GetSnapshot()))
				}
				if m.Term != nil || m.Vote != nil || m.Commit != nil {
					check(c.stores[id].SetHardState(&pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}))
				}
				check(c.stores[id].Append(m.Entries))
				for _, resp := range m.Responses {
					c.route(id, resp)
				}
			case pb.MsgStorageApply:
				if len(m.Entries) > 0 {
					c.applied[id] = m.Entries[len(m.Entries)-1].GetIndex()
				}
				for _, resp := range m.Responses {
					c.route(id, resp)
				}
			default:
				c.route(id, m)
			}
		}
	}
}

type tally struct {
	ns           int64
	alloc, bytes uint64
}

// Only leader intervals are timed and sampled for allocations. ReadMemStats is
// outside the CPU interval; no follower work runs between the two snapshots.
func leaderTime(t *tally, fn func()) {
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	start := cpu()
	fn()
	dt := cpu() - start - clockCost
	runtime.ReadMemStats(&b)
	t.ns += max(1, dt)
	t.alloc += b.Mallocs - a.Mallocs
	t.bytes += b.TotalAlloc - a.TotalAlloc
}
func (c *cluster) pump(t *tally) {
	for {
		did := false
		for id := 1; id <= 3; id++ {
			if len(c.inbox[id]) != 0 || c.nodes[id].HasReady() {
				did = true
				if id == 1 && t != nil {
					leaderTime(t, func() { c.serve(1) })
				} else {
					c.serve(id)
				}
			}
		}
		if !did {
			return
		}
	}
}
func (c *cluster) compact() {
	for id := 1; id <= 3; id++ {
		if c.applied[id] > 1 {
			err := c.stores[id].Compact(c.applied[id])
			if err != nil && err != raft.ErrCompacted {
				panic(err)
			}
		}
	}
}
func (c *cluster) write(payload []byte, entries int, t *tally) {
	before := c.applied[1]
	leaderTime(t, func() {
		for j := 0; j < entries; j++ {
			check(c.nodes[1].Propose(payload))
		}
		c.serve(1)
	})
	c.pump(t)
	if c.applied[1] != before+uint64(entries) {
		panic(fmt.Sprintf("not committed: before=%d after=%d entries=%d", before, c.applied[1], entries))
	}
}

var readContext = []byte("research577-read")

func (c *cluster) read(t *tally) {
	before := c.reads
	leaderTime(t, func() { c.nodes[1].ReadIndex(readContext); c.serve(1) })
	c.pump(t)
	if c.reads != before+1 {
		panic("read not completed")
	}
}
func raftBench(smoke bool) {
	sizes := []int{1024, 64 << 10, 512 << 10}
	batches := []int{1, 8, 64}
	if smoke {
		sizes = sizes[:1]
		batches = []int{1, 8}
	}
	for _, size := range sizes {
		for _, entries := range batches {
			for rep := 0; rep < repetitions; rep++ {
				runtime.GC()
				c := newCluster()
				payload := make([]byte, size)
				for i := range payload {
					payload[i] = byte(i)
				}
				var warm tally
				c.write(payload, entries, &warm)
				c.read(&warm)
				c.compact()
				runtime.GC()
				var w, r tally
				var writes, reads uint64
				for w.ns < int64(duration) || r.ns < int64(duration) {
					if w.ns < int64(duration) {
						c.write(payload, entries, &w)
						writes++
						if writes%256 == 0 {
							c.compact()
						}
					}
					if r.ns < int64(duration) {
						c.read(&r)
						reads++
					}
				}
				emit("B", "raft.write", size, entries, rep, writes, w.ns, w.alloc, w.bytes)
				emit("B", "raft.readindex", size, entries, rep, reads, r.ns, r.alloc, r.bytes)
			}
		}
	}
	// Payloads are not copied: a descriptor append, 2 durable acknowledgements,
	// and the majority commit-index calculation, once per batch.
	for _, entries := range batches {
		for rep := 0; rep < repetitions; rep++ {
			f := floorLeader{ring: make([]descriptor, 1<<16)}
			var ns int64
			var ops uint64
			for ns < int64(duration) {
				const chunk = 65536
				start := cpu()
				for i := 0; i < chunk; i++ {
					f.step(entries)
				}
				ns += cpu() - start - clockCost
				ops += chunk
			}
			sink = f.commit + f.ring[f.last&uint64(len(f.ring)-1)].index
			emit("B", "bookkeeping", 0, entries, rep, ops, ns, 0, 0)
			runtime.KeepAlive(f.ring)
		}
	}
}

type descriptor struct {
	index       uint64
	bytes, term uint32
}
type floorLeader struct {
	ring         []descriptor
	last, commit uint64
	durable      [3]uint64
}

//go:noinline
func (f *floorLeader) step(entries int) {
	for j := 0; j < entries; j++ {
		f.last++
		f.ring[f.last&uint64(len(f.ring)-1)] = descriptor{f.last, 1024, 1}
	}
	f.durable[0] = f.last
	f.durable[1] = f.last
	f.durable[2] = f.last
	f.commit = median3(f.durable[0], f.durable[1], f.durable[2])
}

//go:noinline
func median3(a, b, c uint64) uint64 {
	if a > b {
		a, b = b, a
	}
	if b > c {
		b, c = c, b
	}
	if a > b {
		b = a
	}
	return b
}
