package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

// watches are one watch per resource prefix, as an apiserver keeps one
// per resource. Every change a worker made at a revision the watch covers
// must come in its events, and every event must be a change a worker made.
type watches struct {
	l   *load
	cl  *clientv3.Client
	per []*watched
}

// watched is one watch and what it delivered.
type watched struct {
	resource string
	prefix   string
	mu       sync.Mutex
	// start is the first revision the watch covers, and gaps the
	// revisions a compaction took from it before it got them.
	start int64
	gaps  []span
	// progress is a revision up to which every event arrived, and last
	// the revision of the last event.
	progress, last int64
	// events are the arrived events not yet matched to objects.
	events   []seen
	Events   int64 `json:"events"`
	Restarts int64 `json:"restarts"`
	Compacts int64 `json:"compacted"`
}

// span is the revisions from lo to hi.
type span struct{ lo, hi int64 }

type seen struct {
	key string
	c   change
}

func newWatches(l *load, cl *clientv3.Client) *watches {
	ws := &watches{l: l, cl: cl}
	for _, r := range k8ssize.Resources() {
		ws.per = append(ws.per, &watched{resource: r, prefix: "/registry/" + r + "/"})
	}
	return ws
}

// run watches every prefix from revision from until ctx ends, and asks
// for progress every few seconds, so that progress moves without events.
func (ws *watches) run(ctx context.Context, from int64) {
	wctx := clientv3.WithRequireLeader(ctx)
	var wg sync.WaitGroup
	for _, w := range ws.per {
		w.start, w.progress, w.last = from, from-1, from-1
		wg.Go(func() { ws.watch(wctx, w) })
	}
	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			cctx, cancel := context.WithTimeout(wctx, 5*time.Second)
			_ = ws.cl.RequestProgress(cctx)
			cancel()
		}
	})
	wg.Wait()
}

func (ws *watches) watch(ctx context.Context, w *watched) {
	for ctx.Err() == nil {
		w.mu.Lock()
		rev := max(w.progress, w.last) + 1
		w.mu.Unlock()
		ch := ws.cl.Watch(ctx, w.prefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
		for resp := range ch {
			if resp.CompactRevision != 0 {
				// The revisions up to the compaction are gone for this
				// watch; it starts again after them.
				w.mu.Lock()
				if resp.CompactRevision > rev {
					w.gaps = append(w.gaps, span{rev, resp.CompactRevision - 1})
				}
				w.progress = max(w.progress, resp.CompactRevision-1)
				w.Compacts++
				w.mu.Unlock()
				break
			}
			if resp.Err() != nil {
				break
			}
			w.mu.Lock()
			if resp.IsProgressNotify() {
				w.progress = max(w.progress, resp.Header.Revision)
				w.mu.Unlock()
				continue
			}
			for _, ev := range resp.Events {
				r := ev.Kv.ModRevision
				if r < w.last || r <= w.progress && r != w.last {
					w.mu.Unlock()
					ws.l.fail(&violation{fmt.Sprintf("the watch of %s sent an event at revision %d after revision %d", w.prefix, r, max(w.last, w.progress))})
					return
				}
				w.last = r
				c := change{lo: r, hi: r, del: ev.Type == mvccpb.DELETE}
				if !c.del {
					c.val = hash(ev.Kv.Value)
				}
				w.events = append(w.events, seen{key: string(ev.Kv.Key), c: c})
				w.Events++
			}
			w.progress = max(w.progress, w.last)
			w.mu.Unlock()
		}
		w.mu.Lock()
		w.Restarts++
		w.mu.Unlock()
		if ctx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// covered reports whether the watch covers every revision of c.
func (w *watched) covered(c change) bool {
	if c.lo < w.start {
		return false
	}
	for _, g := range w.gaps {
		if c.lo <= g.hi && g.lo <= c.hi {
			return false
		}
	}
	return true
}

// check matches the events that arrived against the changes the workers
// made. A change at a revision the watch covers and progressed past must
// have come; with final set, every event must be a change, and the
// workers' models must hold their final state.
func (ws *watches) check(final bool) error {
	for wi, w := range ws.per {
		w.mu.Lock()
		p := w.progress
		events := w.events
		w.events = nil
		w.mu.Unlock()
		for _, e := range events {
			o, ok := ws.l.byKey[e.key]
			if !ok {
				if strings.Contains(e.key, "/soak-w") {
					return &violation{fmt.Sprintf("the watch of %s sent a %s of %s, which no worker writes", w.prefix, e.c, e.key)}
				}
				continue
			}
			owner := ws.l.owner[e.key]
			owner.mu.Lock()
			o.seen = append(o.seen, e.c)
			owner.mu.Unlock()
		}
		for _, owner := range ws.l.workers {
			owner.mu.Lock()
			for _, o := range owner.groups[wi].objects {
				if err := w.match(o, p, final); err != nil {
					owner.mu.Unlock()
					return err
				}
			}
			owner.mu.Unlock()
		}
	}
	return nil
}

// match pairs o's changes with the events of them, in revision order.
func (w *watched) match(o *object, p int64, final bool) error {
	if o.pending != nil && !final {
		return nil
	}
	a, s := o.acked, o.seen
	var keep []change
	i, j := 0, 0
	for i < len(a) {
		c := a[i]
		for j < len(s) && s[j].lo < c.lo {
			keep = append(keep, s[j])
			j++
		}
		if j < len(s) && s[j].lo <= c.hi && s[j].del == c.del && (c.del || s[j].val == c.val) {
			i, j = i+1, j+1
			continue
		}
		if c.hi > p {
			break
		}
		if c.opt || !w.covered(c) {
			i++
			continue
		}
		return &violation{fmt.Sprintf("the watch of %s, from revision %d and past revision %d, did not send the %s of %s", w.prefix, w.start, p, c, o.key)}
	}
	keep = append(keep, s[j:]...)
	if final && o.present && o.leased {
		// The lease of an object left present may expire after the
		// final list, once.
		if k := slices.IndexFunc(keep, func(e change) bool { return e.del && e.lo > o.settled }); k >= 0 {
			keep = slices.Delete(keep, k, k+1)
		}
	}
	o.acked, o.seen = a[i:], keep
	if final && len(keep) > 0 {
		return &violation{fmt.Sprintf("the watch of %s sent a %s of %s, which no write of its worker made", w.prefix, keep[0], o.key)}
	}
	return nil
}

// caughtUp reports whether every watch progressed to rev.
func (ws *watches) caughtUp(rev int64) bool {
	for _, w := range ws.per {
		w.mu.Lock()
		p := w.progress
		w.mu.Unlock()
		if p < rev {
			return false
		}
	}
	return true
}
