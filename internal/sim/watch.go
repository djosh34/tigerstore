package sim

import (
	"fmt"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	tnode "github.com/djosh34/tigerstore/internal/node"
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
	// open is set while the client has a stream, on process proc.
	open bool
	proc *proc
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

// startWatchers starts the watch clients. The lock is held.
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
		wait, drop := s.rng.Int64N(Second), s.exp(3*Second)
		s.wg.Go(func() { w.run(wait, drop) })
	}
}

// run opens a stream on a random node and follows it, again whenever it
// breaks, until the run ends. Until the clients stop, it also closes the
// stream on purpose at random times, as clients do on a timeout, and
// resumes up to 2 s later, so the next stream catches up on history.
func (w *watchClient) run(wait, dropAt int64) {
	s := w.sim
	for s.sleep(wait) {
		s.mu.Lock()
		n := s.nodes[s.rng.IntN(3)]
		wait = 100*Millisecond + s.rng.Int64N(400*Millisecond)
		next := w.next
		s.mu.Unlock()
		ws, p, err := s.openStream(n, []int64{next}, func(start int64) *pb.WatchCreateRequest {
			return &pb.WatchCreateRequest{Key: w.key, RangeEnd: w.end, StartRevision: start, PrevKv: w.prevKV, ProgressNotify: w.progress}
		})
		if err != nil {
			continue
		}
		s.mu.Lock()
		w.open, w.proc, w.watchID = true, p, -1
		if dropAt < s.now() {
			dropAt = s.exp(3 * Second)
		}
		s.mu.Unlock()
		wait, dropAt = w.follow(ws, dropAt)
		s.mu.Lock()
		// Once the run ends, the stream stays open for the checks.
		w.open = w.open && s.ended
		s.mu.Unlock()
	}
}

// follow reads stream ws until it breaks or the client leaves it, and
// returns when to open the next one and when to drop that.
func (w *watchClient) follow(ws *tnode.WatchStream, dropAt int64) (int64, int64) {
	s := w.sim
	for {
		s.mu.Lock()
		p, untilDrop, dropping := w.proc, dropAt-s.now(), s.now() < s.stopAt
		s.mu.Unlock()
		t := time.NewTimer(time.Duration(max(untilDrop, 0)))
		drop := t.C
		if !dropping {
			t.Stop()
			drop = nil
		}
		select {
		case r := <-ws.C:
			t.Stop()
			ws.Taken(r)
			s.mu.Lock()
			lat := s.clientLatency()
			s.mu.Unlock()
			if !s.sleep(lat) {
				return 0, dropAt
			}
			s.mu.Lock()
			again := w.receive(r)
			lat = s.clientLatency()
			s.mu.Unlock()
			if again {
				ws.Close()
				return lat, dropAt
			}
		case <-ws.Done():
			t.Stop()
			return 0, dropAt
		case <-p.ctx.Done():
			t.Stop()
			return 0, dropAt
		case <-drop:
			ws.Close()
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.rng.Int64N(2 * Second), s.exp(3 * Second)
		case <-s.end:
			t.Stop()
			return 0, dropAt
		}
	}
}

// receive takes a response of the current stream, and reports whether the
// client must leave it and start a new logical watch. The lock is held.
func (w *watchClient) receive(r *pb.WatchResponse) bool {
	s := w.sim
	w.responses = append(w.responses, WatchRecord{Time: s.now(), Resp: r})
	if r.Canceled && r.CompactRevision != 0 {
		// The revision it needed is compacted. Like Kubernetes, start a
		// new logical watch from now.
		s.stats.WatchCompacted++
		w.ended = append(w.ended, segment{start: w.start, events: w.events, responses: w.responses})
		w.start, w.next, w.events, w.responses = 0, 0, nil, nil
		return true
	}
	if r.Canceled {
		s.fail("watch %d was canceled: %v", w.id, r)
		return false
	}
	if r.Created {
		if w.next == 0 {
			w.next = r.Header.Revision + 1
		}
		if w.start == 0 {
			w.start = w.next
		}
		w.watchID = r.WatchId
		return false
	}
	if r.WatchId != w.watchID {
		s.fail("watch %d got a response for watch ID %d on a stream whose watcher is %d", w.id, r.WatchId, w.watchID)
		return false
	}
	if len(r.Events) == 0 {
		// A progress notification: every event up to its revision was
		// delivered, so clientv3 resumes after it.
		w.next = max(w.next, r.Header.Revision+1)
		return false
	}
	last := w.next
	for _, ev := range r.Events {
		rev := ev.Kv.ModRevision
		if rev < last {
			s.fail("watch %d got an event at revision %d after one at %d", w.id, rev, last)
			return false
		}
		if rev > r.Header.Revision {
			s.fail("watch %d got an event at revision %d in a response with header revision %d", w.id, rev, r.Header.Revision)
			return false
		}
		last = rev
	}
	w.events = append(w.events, r.Events...)
	// A revision is never split across responses, so the next stream can
	// start after the last event.
	w.next = last + 1
	return false
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
