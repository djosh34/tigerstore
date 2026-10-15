package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wal"
)

// memNet connects nodes in one process. Each node takes its messages from
// its own queue. A fetch reads from the source's Serve through a pipe;
// the first cuts streams end early, as when the leader crashes.
type memNet struct {
	mu     sync.Mutex
	queues map[uint64]chan *raftpb.Message
	nodes  map[uint64]*Node
	cuts   []int
	cut    int
}

// join makes n reachable until ctx ends.
func (m *memNet) join(ctx context.Context, n *Node) {
	q := make(chan *raftpb.Message, 4096)
	m.mu.Lock()
	m.queues[n.ID()], m.nodes[n.ID()] = q, n
	m.mu.Unlock()
	go func() {
		for {
			select {
			case msg := <-q:
				n.Deliver(msg)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// leave makes node id unreachable.
func (m *memNet) leave(id uint64) {
	m.mu.Lock()
	delete(m.queues, id)
	delete(m.nodes, id)
	m.mu.Unlock()
}

type memPeer struct {
	net *memNet
	id  uint64
}

func (p memPeer) Send(msg *raftpb.Message) {
	p.net.mu.Lock()
	q := p.net.queues[msg.GetTo()]
	p.net.mu.Unlock()
	if q == nil {
		return
	}
	select {
	case q <- proto.Clone(msg).(*raftpb.Message):
	default:
	}
}

func (p memPeer) Close() error { return nil }

func (p memPeer) SetPeers(map[uint64]string) {}

func (p memPeer) Call(context.Context, uint64, []byte) ([]byte, error) {
	return nil, errors.New("memPeer answers no calls")
}

func (p memPeer) Fetch(_ context.Context, id, first, last uint64) (io.ReadCloser, error) {
	p.net.mu.Lock()
	src := p.net.nodes[id]
	limit := -1
	if len(p.net.cuts) > 0 {
		limit, p.net.cuts = p.net.cuts[0], p.net.cuts[1:]
		p.net.cut++
	}
	p.net.mu.Unlock()
	if src == nil {
		return nil, fmt.Errorf("node %d is down", id)
	}
	r, w := io.Pipe()
	go func() {
		var out io.Writer = w
		if limit >= 0 {
			out = &cutWriter{w: w, left: limit}
		}
		w.CloseWithError(src.Serve(p.id, first, last, out))
	}()
	return r, nil
}

// cutWriter fails once it wrote left bytes.
type cutWriter struct {
	w    io.Writer
	left int
}

var errCut = errors.New("the leader crashed during the fetch")

func (c *cutWriter) Write(p []byte) (int, error) {
	if len(p) > c.left {
		n, err := c.w.Write(p[:c.left])
		c.left = 0
		return n, errors.Join(err, errCut)
	}
	c.left -= len(p)
	return c.w.Write(p)
}

var errFollowerStopped = errors.New("the follower stopped")

// behind is three nodes with a window of a few kilobytes and frequent
// checkpoints, where node 3 was down while the others wrote and compacted
// until their segments with the entries it lacks were gone.
type behind struct {
	t     *testing.T
	net   *memNet
	dirs  map[uint64]string
	nodes map[uint64]*Node
	ran   map[uint64]chan error
	stops map[uint64]context.CancelFunc
	// rev is the revision of the last write, which set every key k00 to
	// k49 to value(i, 40).
	rev int64
}

func value(i, round int) []byte { return fmt.Appendf(nil, "value %d of round %d %0200d", i, round, 0) }

func (c *behind) config(id uint64) Config {
	cfg := core.DefaultConfig(id, []uint64{1, 2, 3})
	cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
	cfg.UnappliedBytes = 1 << 20
	cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
	// The leader stops itself when an append takes two election
	// timeouts; 50 ms ticks keep a slow runner's fsync below that.
	return Config{Core: cfg, DataDir: c.dirs[id], Tick: 50 * time.Millisecond, SegmentBytes: 32 << 10, Logf: func(f string, args ...any) {
		c.t.Logf("node %d: "+f, append([]any{id}, args...)...)
	}}
}

// start runs node id until close.
func (c *behind) start(id uint64) (*Node, error) {
	n, err := New(c.config(id))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.net.join(ctx, n)
	done := make(chan error, 1)
	c.ran[id], c.stops[id], c.nodes[id] = done, cancel, n
	go func() { done <- n.Run(ctx, memPeer{net: c.net, id: id}) }()
	return n, nil
}

func (c *behind) stop(id uint64) error {
	c.net.leave(id)
	c.stops[id]()
	delete(c.stops, id)
	return <-c.ran[id]
}

// close stops the nodes and waits up to 2 s for each, so none writes to
// its directory or logs after the test. A node whose error was read
// already does not answer again.
func (c *behind) close() {
	for id, stop := range c.stops {
		c.net.leave(id)
		stop()
		select {
		case <-c.ran[id]:
		case <-time.After(2 * time.Second):
		}
	}
}

// do retries op on n for up to 20 s.
func do(n *Node, op proto.Message) (proto.Message, error) {
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r, err := n.Do(ctx, op)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return r, err
		}
	}
}

// newBehind sets up a behind cluster with node 3 stopped. The caller
// closes it, also after an error.
func newBehind(t *testing.T) (*behind, error) {
	c := &behind{
		t:     t,
		net:   &memNet{queues: map[uint64]chan *raftpb.Message{}, nodes: map[uint64]*Node{}},
		dirs:  map[uint64]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()},
		nodes: map[uint64]*Node{},
		ran:   map[uint64]chan error{},
		stops: map[uint64]context.CancelFunc{},
	}
	for id := uint64(1); id <= 3; id++ {
		if _, err := c.start(id); err != nil {
			return c, err
		}
	}
	write := func(round int) (int64, error) {
		var rev int64
		for i := range 50 {
			r, err := do(c.nodes[1], &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: value(i, round)})
			if err != nil {
				return 0, fmt.Errorf("put: %w", err)
			}
			rev = r.(*pb.PutResponse).Header.Revision
		}
		return rev, nil
	}
	if _, err := write(0); err != nil {
		return c, err
	}
	if err := c.stop(3); err != nil {
		return c, fmt.Errorf("stop node 3: %w", err)
	}
	for round := 1; round <= 40; round++ {
		var err error
		if c.rev, err = write(round); err != nil {
			return c, err
		}
		if _, err := do(c.nodes[1], &pb.CompactionRequest{Revision: c.rev}); err != nil {
			return c, fmt.Errorf("compact: %w", err)
		}
	}
	for id := uint64(1); id <= 2; id++ {
		first := filepath.Join(c.dirs[id], "wal", wal.SegmentName(1))
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if _, err := os.Stat(first); os.IsNotExist(err) {
				break
			}
			if time.Now().After(deadline) {
				return c, fmt.Errorf("setup: node %d still has its first segment", id)
			}
		}
	}
	return c, nil
}

// followerRetriesFetch starts node 3 of a behind cluster. Its first two
// snapshot streams break off partway, after it wrote some of each to its
// log. It must start again from its log, fetch another snapshot, and
// serve the newest values, without stopping.
func followerRetriesFetch(t *testing.T) error {
	c, err := newBehind(t)
	defer c.close()
	if err != nil {
		return err
	}
	// The first stream breaks off within its header or checkpoint, the
	// second further on.
	c.net.mu.Lock()
	c.net.cuts = []int{100, 16 << 10}
	c.net.mu.Unlock()
	n3, err := c.start(3)
	if err != nil {
		return err
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		select {
		case err := <-c.ran[3]:
			return fmt.Errorf("%w: %w", errFollowerStopped, err)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r, err := n3.Do(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Serializable: true})
		cancel()
		if err == nil && r.(*pb.RangeResponse).Header.Revision >= c.rev {
			kvs := r.(*pb.RangeResponse).Kvs
			if len(kvs) != 50 {
				return fmt.Errorf("node 3 has %d keys", len(kvs))
			}
			for i, kv := range kvs {
				if string(kv.Value) != string(value(i, 40)) {
					return fmt.Errorf("node 3 has %q for k%02d", kv.Value, i)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			if err == nil {
				err = fmt.Errorf("it is at revision %d of %d", r.(*pb.RangeResponse).Header.Revision, c.rev)
			}
			return fmt.Errorf("node 3 did not catch up: %w", err)
		}
	}
	c.net.mu.Lock()
	cut := c.net.cut
	c.net.mu.Unlock()
	if cut != 2 {
		return fmt.Errorf("%d streams were cut, want 2", cut)
	}
	names, err := os.ReadDir(filepath.Join(c.dirs[3], "wal"))
	if err != nil {
		return err
	}
	for _, e := range names {
		if strings.HasPrefix(e.Name(), "ckpt-") {
			return nil
		}
	}
	return errors.New("node 3 caught up without a checkpoint")
}

// TestFollowerRetriesFetch: a follower whose snapshot fetch breaks off
// starts again from its log and catches up.
func TestFollowerRetriesFetch(t *testing.T) {
	if err := followerRetriesFetch(t); err != nil {
		t.Fatal(err)
	}
}

// TestQueuedInstalls runs two installs on node 3 of a behind cluster from
// the same applied index, as when Raft's second snapshot is queued behind
// the first: the leader's checkpoint, then the entry after it. The second
// stream carries the checkpoint the first wrote. It must end the run for
// a reload, not stop the node as a failed disk write would.
func TestQueuedInstalls(t *testing.T) {
	c, err := newBehind(t)
	defer c.close()
	if err != nil {
		t.Fatal(err)
	}
	n3, err := New(c.config(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n3.log.close(); err != nil {
			t.Error(err)
		}
	}()
	leader := c.nodes[1]
	leader.log.mu.Lock()
	ckpt := leader.log.ckpts[len(leader.log.ckpts)-1]
	leader.log.mu.Unlock()
	next, err := leader.log.Entry(ckpt.Index + 1)
	if err != nil {
		t.Fatal(err)
	}
	applied := n3.core.Applied()
	if applied >= ckpt.Index {
		t.Fatalf("setup: node 3 applied %d, the checkpoint is at %d", applied, ckpt.Index)
	}
	p := memPeer{net: c.net, id: 3}
	first := &core.Install{From: 1, Index: ckpt.Index, Term: ckpt.Term, Applied: applied}
	if err := n3.install(t.Context(), p, first); err != nil || first.Checkpoint != ckpt {
		t.Fatalf("first install: %v, checkpoint %+v, want %+v", err, first.Checkpoint, ckpt)
	}
	second := &core.Install{From: 1, Index: ckpt.Index + 1, Term: next.GetTerm(), Applied: applied}
	if err := n3.install(t.Context(), p, second); !errors.Is(err, errReload) {
		t.Fatalf("second install: %v, want a reload", err)
	}
}
