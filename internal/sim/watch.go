package sim

import (
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
)

// watchClient keeps one logical watch open across node crashes, the way
// Kubernetes does: when its stream breaks, it opens a new one on a random
// node from the next revision it has not seen.
type watchClient struct {
	id       int
	sim      *Sim
	key, end []byte
	prevKV   bool
	progress bool
	// watchID is the ID of the current stream's watcher.
	watchID int64
	// start is the first revision of the watch, known once the first
	// stream is created. next is the first revision not yet seen.
	start, next int64
	events      []*mvccpb.Event
	responses   []WatchRecord
	// ended holds the logical watches that a compaction canceled; the
	// client then started a new one from the current revision.
	ended []segment

	// The current stream, if any.
	open   bool
	node   int
	gen    uint64
	stream uint64
	lastAt int64 // delivery time of the last response, for ordering
}

// WatchRecord is one watch response as the client got it.
type WatchRecord struct {
	Time int64
	Resp *pb.WatchResponse
}

// segment is a logical watch that a compaction ended.
type segment struct {
	start     int64
	events    []*mvccpb.Event
	responses []WatchRecord
}

// WatchResult is one logical watch at the end of a run.
type WatchResult struct {
	Key, End       []byte
	PrevKV         bool
	ProgressNotify bool
	Start          int64
	Responses      []WatchRecord
}

func (s *Sim) startWatchers() {
	specs := []struct {
		key, end         string
		prevKV, progress bool
	}{
		{"k", "l", true, false},
		{"k2", "k5", true, true},
		{"k3", "", true, false},
		{"k", "\x00", false, true},
	}
	for i, sp := range specs {
		w := &watchClient{id: i, sim: s, key: []byte(sp.key), prevKV: sp.prevKV, progress: sp.progress}
		if sp.end != "" {
			w.end = []byte(sp.end)
		}
		// Half the watches start from revision 1, the rest from now.
		if s.rng.IntN(2) == 0 {
			w.next = 1
		}
		s.watchers = append(s.watchers, w)
		s.push(&event{at: s.rng.Int64N(Second), kind: evWatchOpen, watch: w})
		s.push(&event{at: s.exp(3 * Second), kind: evWatchDrop, watch: w})
	}
}

// closeStream disconnects a watch client from its stream at once. The node
// learns of it in an event of its own, so a paused node handles it when it
// resumes.
func (s *Sim) closeStream(w *watchClient) {
	n := s.nodes[w.node]
	if n.up && n.gen == w.gen {
		delete(n.streams, w.stream)
		n.closing[w.stream] = true
		s.push(&event{at: s.now, kind: evStreamClose, node: n.idx, gen: n.gen, stream: w.stream})
	}
}

// drop closes the stream on purpose, as clients do on a timeout, and
// resumes up to 2 s later, so the next stream catches up on history.
func (w *watchClient) drop() {
	s := w.sim
	if s.now >= s.stopAt {
		// Watches stay open once the clients stop, so they reach the end.
		return
	}
	if w.open {
		s.closeStream(w)
		w.open = false
		s.push(&event{at: s.now + s.rng.Int64N(2*Second), kind: evWatchOpen, watch: w})
	}
	s.push(&event{at: s.exp(3 * Second), kind: evWatchDrop, watch: w})
}

// openWatch picks a node and sends the create request.
func (w *watchClient) openWatch() {
	s := w.sim
	n := s.nodes[s.rng.IntN(3)]
	if !n.up {
		s.push(&event{at: s.now + 100*Millisecond + s.rng.Int64N(400*Millisecond), kind: evWatchOpen, watch: w})
		return
	}
	s.push(&event{at: s.now + s.clientLatency(), kind: evWatchCreate, node: n.idx, gen: n.gen, watch: w})
}

func (w *watchClient) create(n *node, gen uint64) {
	s := w.sim
	if !n.up || n.gen != gen {
		w.openWatch()
		return
	}
	n.nextStream++
	n.streams[n.nextStream] = w
	w.open, w.node, w.gen, w.stream, w.lastAt, w.watchID = true, n.idx, n.gen, n.nextStream, 0, -1
	n.core.Watch(n.nextStream, core.Caller{}, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: w.key, RangeEnd: w.end, StartRevision: w.next, PrevKv: w.prevKV, ProgressNotify: w.progress,
	}}})
	s.process(n)
}

// watchResponse routes a response from a node to its client, in order.
func (s *Sim) watchResponse(n *node, r core.WatchResponse) {
	w, ok := n.streams[r.Stream]
	if !ok && (n.closing[r.Stream] || n.grayStreams[r.Stream]) {
		// Gone, or the gray client's, which never reads.
		return
	}
	if !ok {
		s.fail("node %d answered unknown watch stream %d", n.id, r.Stream)
		return
	}
	b, err := marshal.Marshal(r.Resp)
	if err != nil {
		s.fail("marshal watch response: %w", err)
		return
	}
	s.mixBytes(b)
	at := max(s.now+s.clientLatency(), w.lastAt)
	w.lastAt = at
	s.push(&event{at: at, kind: evWatchResponse, node: n.idx, gen: n.gen, from: int(r.Stream), watch: w, data: b})
}

func (w *watchClient) receive(e *event) {
	s := w.sim
	if !w.open || w.node != e.node || w.gen != e.gen || w.stream != uint64(e.from) {
		return // a response of a stream that already broke
	}
	r := &pb.WatchResponse{}
	if err := proto.Unmarshal(e.data, r); err != nil {
		s.fail("unmarshal watch response: %w", err)
		return
	}
	w.responses = append(w.responses, WatchRecord{Time: s.now, Resp: r})
	if r.Canceled && r.CompactRevision != 0 {
		// The revision it needed is compacted. Like Kubernetes, start a
		// new logical watch from now.
		s.stats.WatchCompacted++
		s.closeStream(w)
		w.ended = append(w.ended, segment{start: w.start, events: w.events, responses: w.responses})
		w.start, w.next, w.events, w.responses, w.open = 0, 0, nil, nil, false
		s.push(&event{at: s.now + s.clientLatency(), kind: evWatchOpen, watch: w})
		return
	}
	if r.Canceled {
		s.fail("watch %d was canceled: %v", w.id, r)
		return
	}
	if r.Created {
		if w.next == 0 {
			w.next = r.Header.Revision + 1
		}
		if w.start == 0 {
			w.start = w.next
		}
		w.watchID = r.WatchId
		return
	}
	if r.WatchId != w.watchID {
		s.fail("watch %d got a response for watch ID %d on a stream whose watcher is %d", w.id, r.WatchId, w.watchID)
		return
	}
	if len(r.Events) == 0 {
		// A progress notification: every event up to its revision was
		// delivered, so clientv3 resumes after it.
		w.next = max(w.next, r.Header.Revision+1)
		return
	}
	last := w.next
	for _, ev := range r.Events {
		rev := ev.Kv.ModRevision
		if rev < last {
			s.fail("watch %d got an event at revision %d after one at %d", w.id, rev, last)
			return
		}
		if rev > r.Header.Revision {
			s.fail("watch %d got an event at revision %d in a response with header revision %d", w.id, rev, r.Header.Revision)
			return
		}
		last = rev
	}
	w.events = append(w.events, r.Events...)
	// A revision is never split across responses, so the next stream can
	// start after the last event. A response without events says nothing.
	if len(r.Events) > 0 {
		w.next = last + 1
	}
}

// broken is a stream that died with its node. The client may have left
// that stream already and opened another, on the same node or another one
// whose stream and generation numbers can be the same; that one stays.
func (w *watchClient) broken(e *event) {
	if !w.open || w.node != e.node || w.stream != e.stream || w.gen != e.gen {
		return
	}
	w.open = false
	w.openWatch()
}

// checkWatches requires every logical watch to have delivered exactly the
// model's history of its range from its start, in order, without
// duplicates, with previous values where asked. The current one must reach
// the final revision; one that a compaction ended must be complete up to
// its last event.
func (s *Sim) checkWatches(model *mvcc.Store, res *Result) error {
	for _, w := range s.watchers {
		segs := append(slices.Clone(w.ended), segment{start: w.start, events: w.events, responses: w.responses})
		for i, sg := range segs {
			current := i == len(segs)-1
			res.Watches = append(res.Watches, WatchResult{
				Key: w.key, End: w.end, PrevKV: w.prevKV, ProgressNotify: w.progress, Start: sg.start, Responses: sg.responses,
			})
			if sg.start == 0 {
				if current {
					return fmt.Errorf("watch %d was never created", w.id)
				}
				return fmt.Errorf("watch %d was canceled before it was created", w.id)
			}
			want := model.History(w.key, w.end, sg.start)
			if !current {
				last := sg.start - 1
				if len(sg.events) > 0 {
					last = sg.events[len(sg.events)-1].Kv.ModRevision
				}
				want = slices.DeleteFunc(want, func(c mvcc.Change) bool { return c.ModRev() > last })
			}
			if len(sg.events) != len(want) {
				return fmt.Errorf("watch %d (part %d) on %q from %d got %d events, the history has %d", w.id, i, w.key, sg.start, len(sg.events), len(want))
			}
			for j, c := range want {
				ev, err := c.Event(w.prevKV)
				if err != nil {
					return err
				}
				if !proto.Equal(ev, sg.events[j]) {
					return fmt.Errorf("watch %d (part %d) event %d is %v, the history has %v", w.id, i, j, sg.events[j], ev)
				}
			}
		}
	}
	return nil
}
