package node

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// voteWatch is a transport to nobody that notes when the node first asks
// for a vote or a pre-vote.
type voteWatch struct {
	noPeers
	once  sync.Once
	asked chan struct{}
}

func (v *voteWatch) Send(m *raftpb.Message) {
	if t := m.GetType(); t == raftpb.MsgPreVote || t == raftpb.MsgVote {
		v.once.Do(func() { close(v.asked) })
	}
}

// TestElectionDespiteInputs runs node 1 of three with no peers, so it
// never hears from a leader, while an input reaches it on every tick, at
// the same moment as its own ticks fall due: a peer's message that Raft
// takes without starting its election clock again, or a client's write.
// The committer steps a peer's message before the due tick, in case it
// is a heartbeat (turn), but the tick must still follow: the node asks
// for votes within its largest election timeout. Which of the two comes
// first is the select's choice, so each case runs 10 times.
func TestElectionDespiteInputs(t *testing.T) {
	inputs := map[string]func(n *Node){
		"peer message": func(n *Node) {
			n.Deliver(&raftpb.Message{Type: raftpb.MsgHeartbeatResp.Enum(), From: new(uint64(2)), To: new(uint64(1))})
		},
		"client write": func(n *Node) {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()
				// A write with no leader only times out; the test watches
				// the votes.
				_, _ = n.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
			}()
		},
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			for range 10 {
				simrun.Bubble(t, func(t *testing.T) { electAmidInputs(t, input) })
			}
		})
	}
}

// electAmidInputs starts the node, feeds it input on every tick, and
// waits for it to ask for votes.
func electAmidInputs(t *testing.T, input func(*Node)) {
	cfg := Config{Core: core.DefaultConfig(1, []uint64{1, 2, 3}), Tick: 10 * time.Millisecond}
	n, err := newOnDisk(cfg, "", &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}})
	if err != nil {
		t.Fatal(err)
	}
	v := &voteWatch{asked: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, v) }()
	defer func() {
		cancel()
		if err := <-ran; err != nil {
			t.Error(err)
		}
	}()
	stop := make(chan struct{})
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		for {
			select {
			case <-stop:
				return
			case <-time.After(cfg.Tick):
			}
			input(n)
		}
	}()
	defer func() {
		close(stop)
		<-fed
	}()
	// Raft draws its timeout below twice ElectionTicks.
	limit := 2*time.Duration(cfg.Core.ElectionTicks)*cfg.Tick + 2*cfg.Tick
	select {
	case <-v.asked:
	case <-time.After(limit):
		t.Fatalf("no vote asked for within %v while inputs came on every tick", limit)
	}
}
