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

// followerRetriesFetch runs three nodes with a window of a few kilobytes
// and frequent checkpoints. Node 3 is down while the others write and
// compact, until their segments with the entries it lacks are gone. When
// node 3 starts, its first two snapshot streams break off partway, after
// it wrote some of each to its log. It must start again from its log,
// fetch another snapshot, and serve the newest values, without stopping.
func followerRetriesFetch(t *testing.T) error {
	// The leader stops itself when an append takes two election
	// timeouts; 50 ms ticks keep a slow runner's fsync below that.
	tick := 50 * time.Millisecond
	net := &memNet{queues: map[uint64]chan *raftpb.Message{}, nodes: map[uint64]*Node{}}
	dirs := map[uint64]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}
	ran := map[uint64]chan error{}
	stops := map[uint64]context.CancelFunc{}
	start := func(id uint64) (*Node, error) {
		cfg := core.DefaultConfig(id, []uint64{1, 2, 3})
		cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
		cfg.UnappliedBytes = 1 << 20
		cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
		n, err := New(Config{Core: cfg, DataDir: dirs[id], Tick: tick, SegmentBytes: 32 << 10, Logf: func(f string, args ...any) {
			t.Logf("node %d: "+f, append([]any{id}, args...)...)
		}})
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		net.join(ctx, n)
		done := make(chan error, 1)
		ran[id], stops[id] = done, cancel
		go func() { done <- n.Run(ctx, memPeer{net: net, id: id}) }()
		return n, nil
	}
	stop := func(id uint64) error {
		net.leave(id)
		stops[id]()
		return <-ran[id]
	}
	defer func() {
		for id := range stops {
			net.leave(id)
			stops[id]()
		}
	}()
	nodes := map[uint64]*Node{}
	for id := uint64(1); id <= 3; id++ {
		n, err := start(id)
		if err != nil {
			return err
		}
		nodes[id] = n
	}
	value := func(i, round int) []byte { return fmt.Appendf(nil, "value %d of round %d %0200d", i, round, 0) }
	do := func(n *Node, op proto.Message) (proto.Message, error) {
		for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := n.Do(ctx, op)
			cancel()
			if err == nil || time.Now().After(deadline) {
				return r, err
			}
		}
	}
	write := func(round int) (int64, error) {
		var rev int64
		for i := range 50 {
			r, err := do(nodes[1], &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: value(i, round)})
			if err != nil {
				return 0, fmt.Errorf("put: %w", err)
			}
			rev = r.(*pb.PutResponse).Header.Revision
		}
		return rev, nil
	}
	if _, err := write(0); err != nil {
		return err
	}
	if err := stop(3); err != nil {
		return fmt.Errorf("stop node 3: %w", err)
	}
	var rev int64
	for round := 1; round <= 40; round++ {
		var err error
		if rev, err = write(round); err != nil {
			return err
		}
		if _, err := do(nodes[1], &pb.CompactionRequest{Revision: rev}); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
	}
	for id := uint64(1); id <= 2; id++ {
		first := filepath.Join(dirs[id], "wal", wal.SegmentName(1))
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if _, err := os.Stat(first); os.IsNotExist(err) {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("setup: node %d still has its first segment", id)
			}
		}
	}
	// The first stream breaks off within its header or checkpoint, the
	// second further on.
	net.mu.Lock()
	net.cuts = []int{100, 16 << 10}
	net.mu.Unlock()
	n3, err := start(3)
	if err != nil {
		return err
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		select {
		case err := <-ran[3]:
			return fmt.Errorf("%w: %w", errFollowerStopped, err)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r, err := n3.Do(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Serializable: true})
		cancel()
		if err == nil && r.(*pb.RangeResponse).Header.Revision >= rev {
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
				err = fmt.Errorf("it is at revision %d of %d", r.(*pb.RangeResponse).Header.Revision, rev)
			}
			return fmt.Errorf("node 3 did not catch up: %w", err)
		}
	}
	net.mu.Lock()
	cut := net.cut
	net.mu.Unlock()
	if cut != 2 {
		return fmt.Errorf("%d streams were cut, want 2", cut)
	}
	names, err := os.ReadDir(filepath.Join(dirs[3], "wal"))
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
	if testing.Short() {
		t.Skip("runs a three-node cluster for several seconds")
	}
	if err := followerRetriesFetch(t); err != nil {
		t.Fatal(err)
	}
}
