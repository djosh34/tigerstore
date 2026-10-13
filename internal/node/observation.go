package node

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/peer"
)

type observationState struct {
	files   osfs.FileObservation
	request chan struct{}
	node    atomic.Pointer[NodeObservation]
	peers   atomic.Pointer[peerObservation]
	// Only the committer reads or writes these. A job is issued when its
	// effects enter the backlog and completed after its durable callback.
	persistJobsIssued, persistJobsCompleted uint64
}

// NodeObservation is an immutable copy published by the committer between
// normal turns. An observation request never calls Process, Tick or the
// Maintenance.Status RPC. It clones RawNode.Status for Raft progress.
type NodeObservation struct {
	Sequence              uint64 `json:"sequence"`
	SampledUnixNS         int64  `json:"sampled_unix_ns"`
	InboxEvents           int    `json:"inbox_events"`
	PersistQueueBatches   int    `json:"persist_queue_batches"`
	PersistBacklogBatches int    `json:"persist_backlog_batches"`
	PersistedGroups       int    `json:"persisted_groups"`
	PersistJobsIssued     uint64 `json:"persist_jobs_issued"`
	PersistJobsCompleted  uint64 `json:"persist_jobs_completed"`
	core.Observation
	sampledAt time.Time
}

type peerObservation struct {
	at     time.Time
	queues []peer.QueueObservation
}

// ObservationSnapshot has process-lifetime file counters and the most
// recent asynchronous gauges. Node is nil before the first owner sample;
// Peers is nil if the transport has no ObserveQueues method. Ages use Go's
// monotonic clock and are -1 when the corresponding sample is unavailable.
type ObservationSnapshot struct {
	Files              osfs.FileSnapshot       `json:"files"`
	Node               *NodeObservation        `json:"node"`
	NodeSampleAgeNS    int64                   `json:"node_sample_age_ns"`
	Peers              []peer.QueueObservation `json:"peers"`
	PeersSampledUnixNS int64                   `json:"peers_sampled_unix_ns"`
	PeersSampleAgeNS   int64                   `json:"peers_sample_age_ns"`
}

// Observation reads published copies and counters, never live core state.
// It returns nil when observation was disabled at construction.
func (n *Node) Observation() *ObservationSnapshot {
	if n.observation == nil {
		return nil
	}
	o := &ObservationSnapshot{
		Files: n.observation.files.Snapshot(), NodeSampleAgeNS: -1, PeersSampleAgeNS: -1,
	}
	if s := n.observation.node.Load(); s != nil {
		copy := *s
		copy.RaftProgress = slices.Clone(s.RaftProgress)
		o.Node = &copy
		o.NodeSampleAgeNS = int64(time.Since(s.sampledAt))
	}
	if p := n.observation.peers.Load(); p != nil {
		o.Peers = slices.Clone(p.queues)
		o.PeersSampledUnixNS = p.at.UnixNano()
		o.PeersSampleAgeNS = int64(time.Since(p.at))
	}
	return o
}

// sampleObservation runs outside the state owner. Requests use a separate
// one-slot channel, coalesce while the owner is busy, and never compete for
// inbox capacity. Sampling stops with Run, even if persistence is stalled.
func (n *Node) sampleObservation(ctx context.Context, t peer.Transport) {
	ticker := time.NewTicker(n.cfg.ObserveInterval)
	defer ticker.Stop()
	queues, hasQueues := t.(interface {
		ObserveQueues() []peer.QueueObservation
	})
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if hasQueues {
				n.observation.peers.Store(&peerObservation{at: time.Now(), queues: queues.ObserveQueues()})
			}
			select {
			case n.observation.request <- struct{}{}:
			default:
			}
		}
	}
}

// publishObservation is called only by the core owner. It reads lengths,
// copies gauges and publishes an immutable snapshot. Core.Observe clones
// Raft status/progress, but does not inspect persistence jobs, request
// bodies, index keys or transport state.
func (n *Node) publishObservation(backlog int) {
	sequence := uint64(1)
	if last := n.observation.node.Load(); last != nil {
		sequence = last.Sequence + 1
	}
	now := time.Now()
	n.observation.node.Store(&NodeObservation{
		Sequence: sequence, SampledUnixNS: now.UnixNano(), sampledAt: now,
		InboxEvents: len(n.inbox), PersistQueueBatches: len(n.persistQ),
		PersistBacklogBatches: backlog, PersistedGroups: len(n.persisted),
		PersistJobsIssued: n.observation.persistJobsIssued, PersistJobsCompleted: n.observation.persistJobsCompleted,
		Observation: n.core.Observe(),
	})
}
