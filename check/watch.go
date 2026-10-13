package check

import (
	"errors"
	"fmt"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

// maxErrors caps the errors one checker reports.
const maxErrors = 20

// Watches checks every watch in h against the replay of h.Log. It is
// stricter than etcd v3.7.2's watch validator, because Kubernetes rejects a
// non-create event without a previous value:
//
//   - A watch that asks for previous values gets one on every update and
//     delete event, equal to the key before the event, and none on creates.
//     A watch that does not ask gets none.
//   - Events match the filter, are in revision order, never repeat, and never
//     split one revision across responses.
//   - The events are exactly the committed ones from the first revision of
//     the watch on, with the exact key, value, revisions, version and lease.
//     Without a gap, the watch is complete up to h.Target if the server did
//     not cancel it, else up to the last revision it delivered.
//   - A progress notification at R comes after every event at or below R and
//     before every event above R.
//   - Nothing is delivered above the committed log.
//   - A compaction cancel names a compacted revision above the next revision
//     the watch needed, or equal to it if the watch asked for previous
//     values: etcd v3.7.2 sends such an event without one.
//
// A watch with no start revision must record its created response, so the
// checker knows where it starts. Every event must be at or below the header
// revision of its response.
func Watches(h *history.History) error {
	if err := h.Validate(); err != nil {
		return err
	}
	r, err := newReplay(h.Log)
	if err != nil {
		return err
	}
	if h.Target > r.rev {
		return fmt.Errorf("target revision %d is above the last committed revision %d", h.Target, r.rev)
	}
	var errs []error
	for i := range h.Watches {
		for _, e := range checkWatch(r, h.Target, &h.Watches[i]) {
			errs = append(errs, fmt.Errorf("watch %d (client %d, key %q, end %q, start %d): %w", i, h.Watches[i].Client, h.Watches[i].Req.Key, h.Watches[i].Req.End, h.Watches[i].Req.StartRevision, e))
		}
		if len(errs) >= maxErrors {
			break
		}
	}
	return errors.Join(errs...)
}

func checkWatch(r *replay, target int64, w *history.Watch) []error {
	var errs []error
	fail := func(format string, args ...any) {
		if len(errs) < maxErrors {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	type id struct {
		rev int64
		key string
		t   history.EventType
	}
	seen := map[id]bool{}
	var got []history.Event
	var lastEvent, lastProgress, delivered int64
	first := w.Req.StartRevision
	canceled := false
	for i, resp := range w.Responses {
		if canceled {
			fail("response %d arrives after the watch was canceled", i)
		}
		if resp.Created && first == 0 {
			first = resp.Revision + 1
		}
		for j, e := range resp.Events {
			rev := e.Revision()
			if j == 0 && rev <= lastEvent && lastEvent != 0 {
				if rev == lastEvent {
					fail("revision %d is split across responses", rev)
				} else {
					fail("event at revision %d comes after revision %d", rev, lastEvent)
				}
			} else if rev < lastEvent {
				fail("event at revision %d comes after revision %d", rev, lastEvent)
			}
			if rev <= lastProgress {
				fail("event at revision %d comes after a progress notification at %d", rev, lastProgress)
			}
			if !history.InRange(e.KV.Key, w.Req.Key, w.Req.End) {
				fail("event for key %q does not match the watch", e.KV.Key)
			}
			k := id{rev, e.KV.Key, e.Type}
			if seen[k] {
				fail("event %s of key %q at revision %d is delivered twice", typeName(e.Type), e.KV.Key, rev)
			}
			seen[k] = true
			if rev > r.rev {
				fail("event at revision %d is above the last committed revision %d", rev, r.rev)
			}
			if rev > resp.Revision {
				fail("event at revision %d is above its response's header revision %d", rev, resp.Revision)
			}
			lastEvent = max(lastEvent, rev)
			got = append(got, e)
		}
		if resp.Progress {
			if len(resp.Events) > 0 {
				fail("progress notification %d carries events", i)
			}
			if resp.Revision < lastProgress {
				fail("progress notification at %d comes after one at %d", resp.Revision, lastProgress)
			}
			if resp.Revision < lastEvent {
				fail("progress notification at %d comes after an event at %d", resp.Revision, lastEvent)
			}
			if resp.Revision > r.rev {
				fail("progress notification at %d is above the last committed revision %d", resp.Revision, r.rev)
			}
			lastProgress = max(lastProgress, resp.Revision)
		}
		delivered = max(lastEvent, lastProgress)
		if resp.Canceled {
			canceled = true
		}
		if resp.CompactRevision != 0 {
			if !resp.Canceled {
				fail("compaction response at %d does not cancel the watch", resp.CompactRevision)
			}
			// The watch may also be canceled at the revision it needs
			// next when it asked for previous values, because the
			// value before that revision is compacted.
			next := max(first, delivered+1)
			if next > resp.CompactRevision || next == resp.CompactRevision && !w.Req.PrevKV {
				fail("canceled as compacted at %d, but it only needed revision %d on", resp.CompactRevision, next)
			}
			if !slices.Contains(r.compactions, resp.CompactRevision) {
				fail("canceled as compacted at %d, which the log never compacted", resp.CompactRevision)
			}
		}
	}
	if first == 0 {
		// Guessing the start from what arrived would let a watch that
		// lost its first events pass.
		if len(w.Responses) > 0 || target > 0 {
			fail("has no start revision and no created response, so it cannot be checked")
		}
		return errs
	}
	last := delivered
	if !canceled && target > 0 {
		last = max(last, target)
	}

	var want []history.Event
	for _, e := range r.events {
		rev := e.Revision()
		if rev < first || rev > last || !history.InRange(e.KV.Key, w.Req.Key, w.Req.End) {
			continue
		}
		if !w.Req.PrevKV {
			e.PrevKV = nil
		}
		want = append(want, e)
	}
	for i := range max(len(want), len(got)) {
		switch {
		case i >= len(got):
			fail("misses %d events, the first is %s", len(want)-i, describeEvent(want[i]))
		case i >= len(want):
			fail("has %d events that were not committed, the first is %s", len(got)-i, describeEvent(got[i]))
		case got[i].Revision() > want[i].Revision():
			fail("misses %s", describeEvent(want[i]))
		case got[i].Revision() < want[i].Revision():
			fail("has %s, which was not committed", describeEvent(got[i]))
		case !sameEvent(want[i], got[i]):
			fail("%s", eventDiff(want[i], got[i], w.Req.PrevKV))
		default:
			continue
		}
		break
	}
	return errs
}

func sameEvent(a, b history.Event) bool {
	return a.Type == b.Type && a.KV == b.KV && samePtr(a.PrevKV, b.PrevKV)
}

func eventDiff(want, got history.Event, prevKV bool) string {
	if want.Type == got.Type && want.KV == got.KV {
		switch {
		case prevKV && want.PrevKV != nil && got.PrevKV == nil:
			return describeEvent(got) + " has no previous value, but the watch asked for it and the key existed"
		case got.PrevKV != nil && !prevKV:
			return describeEvent(got) + " has a previous value, but the watch did not ask for it"
		case got.PrevKV != nil && want.PrevKV == nil:
			return describeEvent(got) + " creates its key but has a previous value"
		default:
			return fmt.Sprintf("%s has previous value %+v, want %+v", describeEvent(got), *got.PrevKV, *want.PrevKV)
		}
	}
	return fmt.Sprintf("got %s, want %s", describeEvent(got), describeEvent(want))
}

func describeEvent(e history.Event) string {
	return fmt.Sprintf("%s %+v at revision %d", typeName(e.Type), e.KV, e.Revision())
}

func typeName(t history.EventType) string {
	if t == history.EventDelete {
		return "delete"
	}
	return "put"
}
