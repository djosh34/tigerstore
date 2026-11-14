//go:build !gosim

package node

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/diaglog"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// diag680Sink is a fake diagnostic file whose next matching Write waits
// for the shared device to recover. Zap still uses its real encoder and
// locked WriteSyncer. No wall-clock disk pressure or privileges required.
type diag680Sink struct {
	prefix  string
	armed   atomic.Bool
	entered chan time.Time
	release <-chan struct{}
	// A leader draining client diagnostics can resume later than followers;
	// the artifact's RTT-10 heartbeat warning follows device recovery by ~25ms.
	tail time.Duration
}

func (w *diag680Sink) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.prefix) && w.armed.CompareAndSwap(true, false) {
		w.entered <- time.Now()
		<-w.release
		time.Sleep(w.tail)
	}
	return len(p), nil
}

// diag680Peer models the production peer lanes' relevant property:
// Send only queues; the node's end-of-turn loop.Flush writes the queue.
// The bytes then reach the existing in-memory test network.
type diag680Peer struct {
	memPeer
	n           *Node
	pending     []diag680Message
	beats       atomic.Int64
	maxVoteWait atomic.Int64
}
type diag680Message struct {
	m  *raftpb.Message
	at time.Time
}

func (p *diag680Peer) Send(m *raftpb.Message) {
	p.pending = append(p.pending, diag680Message{m, time.Now()})
	p.n.loop.Dirty(p)
}
func (p *diag680Peer) Flush() {
	for _, queued := range p.pending {
		switch queued.m.GetType() {
		case raftpb.MsgHeartbeat:
			p.beats.Add(1)
		case raftpb.MsgPreVote:
			p.maxVoteWait.Store(max(p.maxVoteWait.Load(), int64(time.Since(queued.at))))
		}
		p.memPeer.Send(queued.m)
	}
	p.pending = p.pending[:0]
}

// Test680DiagnosticWrite demonstrates the same all-node stall without
// mmap, page faults, actual disk I/O, or client RPCs on the followers.
// Both variants run exactly the same fault. The only difference is the
// opt-in prototype used by cmd/tigerstore's TIGERSTORE_DIAG_ASYNC_LOG.
func Test680DiagnosticWrite(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { diag680Run(t, async, false) })
		})
	}
}

// A long WAL fsync by itself does not stop the loop. The existing core
// may eventually deliberately suppress messages for a persist stall;
// raise that separate safety threshold here to isolate loop liveness.
func Test680WALSyncAlone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { diag680Run(t, false, true) })
}

func diag680Run(t *testing.T, async, walOnly bool) {
	net := &memNet{queues: map[uint64]chan *raftpb.Message{}, nodes: map[uint64]*Node{}}
	release := make(chan struct{})
	released := false
	unblock := func() {
		if !released {
			close(release)
			released = true
		}
	}
	var nodes [3]*Node
	var peers [3]*diag680Peer
	var sinks [3]*diag680Sink
	var queues []*diaglog.Queue
	var turns [3]atomic.Int64
	var terms [3]atomic.Uint64
	var slow [3]atomic.Bool
	var stops []context.CancelFunc
	var runs []chan error
	defer func() {
		unblock()
		for _, stop := range stops {
			stop()
		}
		for _, run := range runs {
			if err := <-run; err != nil {
				t.Error(err)
			}
		}
		for _, q := range queues {
			q.Close()
		}
	}()
	for i := range nodes {
		id := uint64(i + 1)
		sink := &diag680Sink{prefix: "NEVER MATCH", entered: make(chan time.Time, 1), release: release}
		sinks[i] = sink
		lg := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.Lock(zapcore.AddSync(sink)), zap.InfoLevel))
		logf := lg.Sugar().Infof
		if async {
			q := diaglog.New(func(line string) { lg.Info(line) }, 8)
			queues = append(queues, q)
			logf = q.Logf
		}
		cfg := Config{Core: core.DefaultConfig(id, []uint64{1, 2, 3}), Tick: 100 * time.Millisecond, Logf: logf,
			Turn: func(c *core.Node, _ core.Effects, _ error) { turns[i].Add(1); terms[i].Store(c.Term()) },
			BeforeSync: func() {
				if slow[i].CompareAndSwap(true, false) {
					if walOnly {
						<-release
					} else {
						time.Sleep(200 * time.Millisecond)
					}
				}
			},
		}
		// Keep every follower's randomized election below 1.6s, leaving
		// over 1s of the 2.7s fault for the queued PreVote to wait.
		cfg.Core.ElectionTicks = 8
		if walOnly {
			cfg.Core.StallElections = 100
		}
		n, err := NewOnFS(cfg, &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		ctx, cancel := context.WithCancel(context.Background())
		stops = append(stops, cancel)
		net.join(ctx, n)
		p := &diag680Peer{memPeer: memPeer{net: net, id: id}, n: n}
		peers[i] = p
		ran := make(chan error, 1)
		runs = append(runs, ran)
		go func() { ran <- n.Run(ctx, p) }()
	}
	leader := -1
	for range 60 {
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		l := nodes[0].Leader()
		if l > 0 && nodes[1].Leader() == l && nodes[2].Leader() == l {
			leader = int(l - 1)
			break
		}
	}
	if leader < 0 {
		t.Fatal("no initial leader")
	}
	// Let the initial no-op persist and every startup log finish.
	time.Sleep(300 * time.Millisecond)
	synctest.Wait()
	term := terms[leader].Load()
	if !walOnly {
		for i, sink := range sinks {
			sink.prefix = "leader is 0"
			if i == leader {
				sink.prefix = "persist timing:"
				sink.tail = 25 * time.Millisecond
			}
			sink.armed.Store(true)
		}
	}
	slow[leader].Store(true)
	put := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := nodes[leader].Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
		put <- err
	}()
	if walOnly {
		time.Sleep(300 * time.Millisecond)
	} else {
		select {
		case <-sinks[leader].entered:
		case <-time.After(time.Second):
			t.Fatal("no log write fault reached")
		}
	}
	synctest.Wait()
	if async {
		// A full queue must drop rather than put backpressure on the loop.
		for range 64 {
			nodes[leader].cfg.Logf("diagnostic burst")
		}
		if queues[leader].Dropped.Load() == 0 {
			t.Fatal("queue did not exercise its overflow policy")
		}
		t.Logf("bounded prototype dropped %d lines while sink was stalled", queues[leader].Dropped.Load())
	}
	before, beats := turns[leader].Load(), peers[leader].beats.Load()
	time.Sleep(2700 * time.Millisecond)
	synctest.Wait()
	delta, sent := turns[leader].Load()-before, peers[leader].beats.Load()-beats
	t.Logf("during 2.7s fault: leader loop turns=%d, heartbeats flushed=%d", delta, sent)
	if async || walOnly {
		if delta < 20 || sent < 40 {
			t.Fatalf("loop stopped despite isolation: turns=%d beats=%d", delta, sent)
		}
		for i, n := range nodes {
			if n.Leader() != uint64(leader+1) || terms[i].Load() != term {
				t.Fatal("leadership changed with isolated fault")
			}
		}
	} else {
		if delta != 0 || sent != 0 {
			t.Fatal("fault did not stop the loop")
		}
		stack := make([]byte, 1<<20)
		stack = stack[:runtime.Stack(stack, true)]
		for _, goroutine := range strings.Split(string(stack), "\n\n") {
			if strings.Contains(goroutine, "(*diag680Sink).Write") && strings.Contains(goroutine, "(*Node).commit") {
				t.Logf("blocked committer stack:\n%s", goroutine)
			}
		}
		for i, sink := range sinks {
			if i == leader {
				continue
			}
			select {
			case at := <-sink.entered:
				t.Logf("follower %d queued %d messages before blocking in log Write for %v", i+1, len(peers[i].pending), time.Since(at))
			default:
				t.Fatal("follower did not block in its log write")
			}
			if len(peers[i].pending) == 0 {
				t.Fatal("no follower peer queue")
			}
		}
	}
	unblock()
	time.Sleep(100 * time.Millisecond)
	synctest.Wait()
	for i, p := range peers {
		wait := time.Duration(p.maxVoteWait.Load())
		if wait > 0 {
			t.Logf("node %d longest PreVote queue wait=%v", i+1, wait)
		}
		if !async && !walOnly && i != leader && wait < time.Second {
			t.Fatal("follower PreVote did not wait over 1s")
		}
	}
	// Let an election finish (and do not turn the retry into a long run).
	time.Sleep(3 * time.Second)
	synctest.Wait()
	if !async && !walOnly {
		if terms[leader].Load() <= term {
			t.Fatal("blocked leader kept its term")
		}
		t.Logf("old leader term %d -> %d", term, terms[leader].Load())
	}
	select {
	case <-put:
	default:
	}
}
