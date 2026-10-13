package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"time"

	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

const listPrefix = "/correct-round1/list/"

type listEvidence struct {
	Endpoint     string `json:"endpoint"`
	CallUTC      int64  `json:"call_utc_ns"`
	ReturnUTC    int64  `json:"return_utc_ns"`
	EOFUTC       int64  `json:"stream_eof_utc_ns,omitempty"`
	callTime     time.Time
	eofTime      time.Time
	Serializable bool                `json:"serializable"`
	Chunks       []*pb.RangeResponse `json:"chunks"`
	Unary        *pb.RangeResponse   `json:"historical_unary,omitempty"`
	Passed       bool                `json:"passed"`
	Error        string              `json:"error,omitempty"`
}

// Retain every chunk and compare every KV field at the stream's pinned revision.
func checkedList(ctx context.Context, cl *clientv3.Client, serializable bool) (e listEvidence, err error) {
	e.callTime = time.Now()
	e.Endpoint, e.CallUTC, e.Serializable = cl.Endpoints()[0], e.callTime.UnixNano(), serializable
	defer func() {
		e.ReturnUTC = time.Now().UnixNano()
		e.Passed = err == nil
		if err != nil {
			e.Error = err.Error()
		}
	}()
	kv := pb.NewKVClient(cl.ActiveConnection())
	req := &pb.RangeRequest{Key: []byte(listPrefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(listPrefix)), Serializable: serializable}
	stream, err := kv.RangeStream(ctx, req)
	if err != nil {
		return e, err
	}
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			e.eofTime = time.Now()
			e.EOFUTC = e.eofTime.UnixNano()
			break
		}
		if recvErr != nil {
			return e, recvErr
		}
		if chunk.RangeResponse == nil {
			return e, errors.New("nil RangeResponse")
		}
		e.Chunks = append(e.Chunks, chunk.RangeResponse)
	}
	merged, err := mergeList(e.Chunks)
	if err != nil {
		return e, err
	}
	req.Revision = merged.Header.Revision
	// Historical reads need no quorum; the preceding stream was linearizable
	// unless explicitly marked serializable for a leaderless fault probe.
	req.Serializable = true
	e.Unary, err = kv.Range(ctx, req)
	if err != nil {
		return e, err
	}
	if merged.Count != e.Unary.Count || merged.More != e.Unary.More || len(merged.Kvs) != len(e.Unary.Kvs) {
		return e, errors.New("stream count/more/length differs from historical Range")
	}
	for i, value := range merged.Kvs {
		if !proto.Equal(value, e.Unary.Kvs[i]) {
			return e, fmt.Errorf("stream KV metadata mismatch at index %d", i)
		}
	}
	return e, nil
}

func mergeList(chunks []*pb.RangeResponse) (*pb.RangeResponse, error) {
	if len(chunks) < 2 || len(chunks[0].Kvs) != 10 {
		return nil, errors.New("LIST did not exercise first-10 multi-chunk path")
	}
	merged := &pb.RangeResponse{}
	for i, chunk := range chunks {
		if i < len(chunks)-1 {
			if chunk.Header != nil || chunk.Count != 0 || chunk.More {
				return nil, errors.New("nonfinal chunk carries header/count/more")
			}
		} else {
			if chunk.Header == nil || chunk.Header.Revision <= 1 {
				return nil, errors.New("missing final snapshot header")
			}
			merged.Header, merged.Count, merged.More = chunk.Header, chunk.Count, chunk.More
		}
		for _, value := range chunk.Kvs {
			if len(merged.Kvs) > 0 && bytes.Compare(merged.Kvs[len(merged.Kvs)-1].Key, value.Key) >= 0 {
				return nil, errors.New("unordered or duplicate stream key")
			}
			merged.Kvs = append(merged.Kvs, value)
		}
	}
	if merged.More || merged.Count != int64(len(merged.Kvs)) || len(merged.Kvs) != 32 {
		return nil, errors.New("unlimited LIST lost seeded keys or count")
	}
	return merged, nil
}

// The probe starts and finishes while the controller keeps the fault installed.
// A fresh non-require-leader watch replays the mutable LIST dataset from revision
// 1, then reports progress through the independently captured LIST revision.
func listWatchProbe(c config) (err error) {
	cs, err := clients(c)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeClients(cs)) }()
	if len(cs) != 1 {
		return errors.New("probe requires exactly one pinned member")
	}
	cl := cs[0]
	type evidence struct {
		CallUTC   int64                   `json:"call_utc_ns"`
		ReturnUTC int64                   `json:"return_utc_ns"`
		List      listEvidence            `json:"list"`
		Watches   []history.WatchResponse `json:"watch_responses"`
		Target    int64                   `json:"target"`
		Passed    bool                    `json:"passed"`
		Error     string                  `json:"error,omitempty"`
	}
	ev := evidence{CallUTC: time.Now().UnixNano()}
	defer func() {
		ev.ReturnUTC = time.Now().UnixNano()
		ev.Passed = err == nil
		if err != nil {
			ev.Error = err.Error()
		}
		err = errors.Join(err, save(c.out, ev))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := cl.Watch(ctx, listPrefix, clientv3.WithPrefix(), clientv3.WithRev(1), clientv3.WithPrevKV(), clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
	ev.List, err = checkedList(ctx, cl, c.serializable)
	if err != nil {
		return err
	}
	ev.Target = ev.List.Chunks[len(ev.List.Chunks)-1].Header.Revision
	state := map[string]*mvccpb.KeyValue{}
	last := int64(0)
	covered := false
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for !covered {
		select {
		case <-ctx.Done():
			return errors.New("fault watch did not report progress through LIST target")
		case <-tick.C:
			if err = cl.RequestProgress(ctx); err != nil {
				return err
			}
		case wr, ok := <-ch:
			if !ok {
				return errors.New("fault watch closed before target")
			}
			hr := history.WatchResponse{Time: time.Now().UnixNano(), Revision: wr.Header.Revision, Created: wr.Created, Progress: wr.IsProgressNotify(), Canceled: wr.Canceled}
			for _, event := range wr.Events {
				typ := history.EventPut
				if event.Type == mvccpb.DELETE {
					typ = history.EventDelete
				}
				hr.Events = append(hr.Events, history.Event{Type: typ, KV: keyValue(event.Kv), PrevKV: prevKV(event.PrevKv)})
			}
			ev.Watches = append(ev.Watches, hr)
			if err = wr.Err(); err != nil {
				return err
			}
			for _, event := range wr.Events {
				value := event.Kv
				if value.ModRevision <= last {
					return errors.New("fault watch duplicated or reordered dataset mutation")
				}
				last = value.ModRevision
				if value.ModRevision > ev.Target {
					continue
				}
				old := state[string(value.Key)]
				if event.Type != mvccpb.PUT {
					return errors.New("unexpected delete in update-only LIST dataset")
				}
				if old == nil {
					if event.PrevKv != nil || value.Version != 1 || value.CreateRevision != value.ModRevision {
						return errors.New("fault watch create metadata wrong")
					}
				} else if !proto.Equal(old, event.PrevKv) || value.Version != old.Version+1 || value.CreateRevision != old.CreateRevision {
					return errors.New("fault watch lost mutation or incorrect PrevKV/version")
				}
				state[string(value.Key)] = value
			}
			covered = wr.IsProgressNotify() && wr.Header.Revision >= ev.Target
		}
	}
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) != len(ev.List.Unary.Kvs) {
		return errors.New("fault watch incomplete at LIST target")
	}
	for i, key := range keys {
		if !proto.Equal(state[key], ev.List.Unary.Kvs[i]) {
			return errors.New("fault watch replay differs from independent historical Range")
		}
	}
	return nil
}

func (r *recorder) concurrentLists(ctx context.Context, cl *clientv3.Client, client int, timeout time.Duration) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), timeout)
		ev, err := checkedList(callCtx, cl, false)
		cancel()
		r.mu.Lock()
		r.lists = append(r.lists, ev)
		if !ev.eofTime.IsZero() {
			if merged, mergeErr := mergeList(ev.Chunks); mergeErr == nil {
				// Record the completed linearizable stream itself, before the
				// subsequent historical comparison. Monotonic invocation/EOF
				// bounds let the whole-history checker reject stale snapshots.
				r.h.Ops = append(r.h.Ops, history.Op{
					Client: client, Call: ev.callTime.Sub(r.origin).Nanoseconds(), Return: ev.eofTime.Sub(r.origin).Nanoseconds(),
					Req:  history.Request{Range: &history.RangeRequest{Key: listPrefix, End: clientv3.GetPrefixRangeEnd(listPrefix)}},
					Resp: &history.Response{Revision: merged.Header.Revision, Range: rangeResponse(merged)},
				})
			}
		}
		// Fault transport errors remain evidence, not correctness violations. Any
		// completed stream with a malformed chunk or wrong snapshot fails the case.
		if err != nil && !transientWatch(err) && ctx.Err() == nil {
			r.errs = append(r.errs, fmt.Errorf("LIST: %w", err))
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (r *recorder) seedLists(cs []*clientv3.Client, c config) error {
	for i := range 32 {
		q := history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("%sk%02d", listPrefix, i), Value: fmt.Sprintf("seed-%d-%02d", c.seed, i), PrevKV: true}}
		if op := r.call(cs[0], 50000, q, c.requestTimeout); op.Err != nil {
			return fmt.Errorf("LIST seed failed: %s", op.Err.Message)
		}
	}
	return nil
}

func (r *recorder) mutateLists(ctx context.Context, cs []*clientv3.Client, c config) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for seq := 0; ; seq++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// Rotate across member-pinned clients. All writes stay in the checked history.
		r.call(cs[seq%3], 50001, history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("%sk%02d", listPrefix, seq%32), Value: fmt.Sprintf("s%d-list-%d", c.seed, seq), PrevKV: true}}, c.requestTimeout)
	}
}

func listEvidencePath(out string) string { return filepath.Join(out, "list-responses.json") }
