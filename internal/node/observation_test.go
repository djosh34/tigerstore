package node

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/peer"
)

type observedNoPeers struct{ noPeers }

func (observedNoPeers) ObserveQueues() []peer.QueueObservation {
	return []peer.QueueObservation{{ID: 2, DataBytes: 123, DataMessages: 1}}
}

func observationNode(t *testing.T, interval time.Duration) *Node {
	t.Helper()
	n, err := New(Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir(), Tick: 50 * time.Millisecond, ObserveInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestObservationDisabledAndSnapshotOnly(t *testing.T) {
	for _, interval := range []time.Duration{0, 100 * time.Millisecond} {
		n := observationNode(t, interval)
		o := n.Observation()
		if interval == 0 {
			if o != nil || n.observation != nil {
				t.Fatal("off mode created observation state")
			}
		} else {
			if o == nil || o.Node != nil || o.Peers != nil || o.NodeSampleAgeNS != -1 || o.PeersSampleAgeNS != -1 {
				t.Fatalf("initial snapshot: %+v", o)
			}
			n.core.Submit(core.Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
			n.publishObservation(5)
			o = n.Observation()
			if o.Node == nil || o.Node.Sequence != 1 || o.Node.PersistBacklogBatches != 5 || o.Node.PendingProposals != 1 || o.Node.QueuedWrites != 1 || o.Node.Revision != 1 {
				t.Fatalf("owner snapshot: %+v", o.Node)
			}
			sequence := o.Node.Sequence
			o.Node.Sequence = 999
			for range 4 {
				if got := n.Observation(); got.Node.Sequence != sequence || len(n.inbox) != 0 || len(n.observation.request) != 0 || got.Node.Applied != 0 {
					t.Fatal("scrape called the core, queued work or shared the published copy")
				}
			}
		}
		if err := n.wal.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestObservationSamplerCoalescesWithoutInbox(t *testing.T) {
	n := observationNode(t, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		n.sampleObservation(ctx, observedNoPeers{})
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		o := n.Observation()
		if len(n.observation.request) == 1 && len(o.Peers) == 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("sampler did not request a snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	// No owner consumes the request. The sampler must still stop and
	// publish peer samples without a blocked send or an inbox event.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler blocked on its request queue")
	}
	o := n.Observation()
	if o.Node != nil || len(n.inbox) != 0 || len(n.observation.request) != 1 || o.Peers[0].DataBytes != 123 || o.PeersSampleAgeNS < 0 {
		t.Fatalf("sampler snapshot: %+v", o)
	}
	o.Peers[0].DataBytes = -1
	if n.Observation().Peers[0].DataBytes != 123 {
		t.Fatal("snapshot shares published peer data")
	}
	if err := n.wal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestObservationPersistenceFenceAccountsActiveGroup(t *testing.T) {
	n := observationNode(t, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	// No disk goroutine. The test takes a group off persistQ and holds
	// it like an active disk worker, even though every queue is empty.
	go func() { ran <- n.commit(ctx, noPeers{}) }()
	defer func() {
		cancel()
		if err := <-ran; err != nil {
			t.Error(err)
		}
		if err := n.wal.Close(); err != nil {
			t.Error(err)
		}
	}()
	var jobs []core.PersistJob
	select {
	case jobs = <-n.persistQ:
	case <-time.After(5 * time.Second):
		t.Fatal("node did not issue initial persistence")
	}
	sample := func() *NodeObservation {
		t.Helper()
		var before uint64
		if o := n.Observation().Node; o != nil {
			before = o.Sequence
		}
		n.observation.request <- struct{}{}
		deadline := time.Now().Add(time.Second)
		for {
			if o := n.Observation().Node; o != nil && o.Sequence > before {
				return o
			}
			if time.Now().After(deadline) {
				t.Fatal("owner did not publish requested sample")
			}
			time.Sleep(time.Millisecond)
		}
	}
	first := sample()
	if first.PersistJobsIssued < uint64(len(jobs)) || first.PersistJobsCompleted != 0 || first.PersistQueueBatches != 0 || first.PersistBacklogBatches != 0 || first.PersistedGroups != 0 {
		t.Fatalf("active group disappeared from job accounting: %+v", first)
	}
	var completed uint64
	for range 10 {
		for _, j := range jobs {
			n.wal.Append(j.HardState, j.Entries)
		}
		if err := n.wal.Sync(); err != nil {
			t.Fatal(err)
		}
		completed += uint64(len(jobs))
		n.persisted <- persistDone{jobs: jobs}
		// An observation may be selected before the completion input,
		// but never between Persisted and its following Process.
		var o *NodeObservation
		for {
			o = sample()
			if o.PersistJobsCompleted == completed {
				break
			}
		}
		if o.PersistJobsCompleted > o.PersistJobsIssued {
			t.Fatalf("completion-generated jobs were not issued before sampling: %+v", o)
		}
		if o.PersistJobsIssued == o.PersistJobsCompleted {
			if o.PersistJobsIssued <= first.PersistJobsIssued {
				t.Fatal("fixture did not cover persistence generated by a completion")
			}
			if o.PersistQueueBatches != 0 || o.PersistBacklogBatches != 0 || o.PersistedGroups != 0 || o.PersistJobsIssued < first.PersistJobsIssued {
				t.Fatalf("equal job counters despite queued work: %+v", o)
			}
			return
		}
		select {
		case jobs = <-n.persistQ:
		case <-time.After(time.Second):
			t.Fatal("outstanding issued jobs were not handed to disk")
		}
	}
	t.Fatal("persistence did not reach an equal-counter fence")
}

func TestObservationRunningNode(t *testing.T) {
	n := observationNode(t, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, observedNoPeers{}) }()
	defer func() {
		cancel()
		select {
		case err := <-ran:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("node did not stop")
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	var rev int64
	for {
		call, stop := context.WithTimeout(ctx, time.Second)
		reply, err := n.Do(call, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
		stop()
		if err == nil {
			rev = reply.(*pb.PutResponse).Header.Revision
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node did not accept a put: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for {
		o := n.Observation()
		if o.Node != nil && o.Node.Leader == 1 && o.Node.Revision == rev && o.Node.PendingProposals == 0 && o.Node.PersistJobsIssued == o.Node.PersistJobsCompleted {
			if o.Node.Applied == 0 || o.Node.Term == 0 || o.Node.Sequence == 0 || o.NodeSampleAgeNS < 0 || o.Files.Write.Calls == 0 || o.Files.Fsync.Calls == 0 || o.Files.Write.Errors != 0 || o.Files.Fsync.Errors != 0 || len(o.Node.RaftProgress) != 1 || o.Node.RaftProgress[0].ID != 1 || o.Node.PersistJobsIssued == 0 {
				t.Fatalf("running snapshot: %+v", o)
			}
			o.Node.RaftProgress[0].ID = 999
			if n.Observation().Node.RaftProgress[0].ID != 1 {
				t.Fatal("scrape shares the published progress slice")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("applied write was not sampled: %+v", o)
		}
		time.Sleep(time.Millisecond)
	}
	call, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	reply, err := n.Do(call, &pb.RangeRequest{Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	rangeReply := reply.(*pb.RangeResponse)
	if rangeReply.Header.Revision != rev || len(rangeReply.Kvs) != 1 || string(rangeReply.Kvs[0].Value) != "v" {
		t.Fatalf("read after observed put: %+v", rangeReply)
	}
}
