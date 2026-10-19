package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type watchDiagnostic struct {
	Client   int                   `json:"client"`
	Response history.WatchResponse `json:"response"`
	Error    string                `json:"error,omitempty"`
}

func transientWatch(err error) bool {
	code := status.Code(err)
	if e, ok := errors.AsType[rpctypes.EtcdError](err); ok {
		code = e.Code()
	}
	return errors.Is(err, rpctypes.ErrNoLeader) || code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled
}

// retryWatch keeps the logical watch across a learner replacement too.
// Learners correctly refuse Watch until promotion. No event or progress
// cursor is discarded; the resumed watch must still deliver every event.
func retryWatch(err error) bool {
	return transientWatch(err) || errors.Is(err, rpctypes.ErrGRPCNotSupportedForLearner) ||
		errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCNotSupportedForLearner))
}

func (r *recorder) watch(ctx context.Context, cl *clientv3.Client, i int, prefix string) {
	next := int64(1)
	for ctx.Err() == nil {
		ch := cl.Watch(clientv3.WithRequireLeader(ctx), prefix, clientv3.WithPrefix(), clientv3.WithRev(next), clientv3.WithPrevKV(), clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
		fatal := false
		for wr := range ch {
			hr := history.WatchResponse{Time: r.now(), Revision: wr.Header.Revision, Created: wr.Created, Canceled: wr.Canceled, CompactRevision: wr.CompactRevision, Progress: wr.IsProgressNotify()}
			e := wr.Err()
			if e != nil && wr.CompactRevision == 0 {
				hr.CancelReason = e.Error()
			}
			var bad error
			for _, ev := range wr.Events {
				he := history.Event{KV: frometcd.KeyValue(ev.Kv), PrevKV: frometcd.PrevKV(ev.PrevKv)}
				switch ev.Type {
				case mvccpb.Event_PUT:
					he.Type = history.EventPut
				case mvccpb.Event_DELETE:
					he.Type = history.EventDelete
				default:
					bad = fmt.Errorf("unknown watch event %d", ev.Type)
				}
				hr.Events = append(hr.Events, he)
			}
			diagnostic := watchDiagnostic{Client: 100 + i, Response: hr}
			if e != nil {
				diagnostic.Error = e.Error()
			}
			r.mu.Lock()
			r.diagnostics = append(r.diagnostics, diagnostic)
			// clientv3 turns a terminal no-leader/transport status into a canceled
			// response. That closes this transport, not the resumed logical watch.
			// Preserve it in diagnostics, but do not weaken logical completeness.
			if e != nil && retryWatch(e) && wr.CompactRevision == 0 && len(wr.Events) == 0 && bad == nil {
				r.mu.Unlock()
				continue
			}
			r.h.Watches[i].Responses = append(r.h.Watches[i].Responses, hr)
			if bad != nil {
				r.errs = append(r.errs, bad)
				fatal = true
			}
			if e != nil {
				r.errs = append(r.errs, fmt.Errorf("watch %d: %w", i, e))
				fatal = true
			}
			r.mu.Unlock()
			next = resumeRevision(next, hr)
		}
		if fatal || ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func resumeRevision(next int64, wr history.WatchResponse) int64 {
	for _, ev := range wr.Events {
		next = max(next, ev.KV.ModRevision+1)
	}
	if wr.Progress {
		next = max(next, wr.Revision+1)
	}
	return next
}

// throughTarget reports whether watch w got an event or a progress
// notification at or past the target. A watch resumed after the last write
// starts above the target, and neither tigerstore nor etcd sends progress
// to such a watcher, so an event at the target counts too; check.Watches
// still finds any event the watch missed.
func throughTarget(w history.Watch, target int64) bool {
	next := int64(1)
	for _, wr := range w.Responses {
		next = resumeRevision(next, wr)
	}
	return target > 0 && next-1 >= target
}
