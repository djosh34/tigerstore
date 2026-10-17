package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/k8ssize"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const proofPrefix = "/proof/"

// The large /registry/ population is immutable during the proof. All mixed
// mutations use /proof/, which starts empty at base. This lets every general
// checker run without copying two GiB of immutable values into its model.
// Bulk reads still check exact values, and final scans check every member.
type recordedLoad struct {
	mu               sync.Mutex
	h                history.History
	base             int64
	origin           time.Time
	mixedStart       time.Time
	mixedEnd         time.Time
	clients          []*clientv3.Client
	stop, stopW      context.CancelFunc
	workers, watches sync.WaitGroup
	errs             []error
}

func startRecorded(cl *clientv3.Client) (*recordedLoad, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	r, err := cl.Get(ctx, proofPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, err
	}
	if r.Count != 0 {
		return nil, errors.New("proof namespace is not empty")
	}
	s := &recordedLoad{base: r.Header.Revision - 1, origin: time.Now()}
	for _, endpoint := range cl.Endpoints() {
		one, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 10 * time.Second, MaxUnaryRetries: 1})
		if err != nil {
			return nil, errors.Join(err, s.close())
		}
		s.clients = append(s.clients, one)
	}
	wctx, stopW := context.WithCancel(context.Background())
	s.stopW = stopW
	for i := range s.clients {
		s.h.Watches = append(s.h.Watches, history.Watch{Client: 100 + i, Req: history.WatchRequest{Key: proofPrefix, End: "/proof0", StartRevision: s.base + 2, PrevKV: true, ProgressNotify: true}})
	}
	for i, one := range s.clients {
		s.watches.Go(func() { s.watch(wctx, one, i) })
	}
	return s, nil
}

func (s *recordedLoad) start() {
	s.mixedStart = time.Now()
	ctx, stop := context.WithCancel(context.Background())
	s.stop = stop
	for i, cl := range s.clients {
		s.workers.Go(func() {
			rng := rand.New(rand.NewPCG(148, uint64(i+1)))
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			for seq := 1; ; seq++ {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				key := []byte(fmt.Sprintf("%sk%02d", proofPrefix, rng.IntN(24)))
				identity := fmt.Sprintf("c%d-%d:", i, seq)
				size := k8ssize.Buckets[k8ssize.Draw(rng, "")].Size
				value := []byte(identity + strings.Repeat("x", max(0, size-len(identity))))
				var q proto.Message
				switch rng.IntN(5) {
				case 0:
					q = &pb.PutRequest{Key: key, Value: value, PrevKv: true}
				case 1:
					q = &pb.DeleteRangeRequest{Key: key, PrevKv: true}
				case 2:
					q = &pb.RangeRequest{Key: []byte(proofPrefix), RangeEnd: []byte("/proof0"), Limit: 8}
				case 3:
					q = &pb.RangeRequest{Key: key}
				case 4:
					q = &pb.TxnRequest{Compare: []*pb.Compare{{Key: key, Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: 0}}}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: value, PrevKv: true}}}}, Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}}}
				}
				s.call(cl, i, q)
			}
		})
	}
	// A separate client grants, uses and explicitly revokes leases. Their
	// 600s TTL excludes natural expiry during this 180s measurement.
	s.workers.Go(func() {
		for id := int64(10001); ; id++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			cl := s.clients[0]
			s.call(cl, 10, &pb.LeaseGrantRequest{ID: id, TTL: 600})
			s.call(cl, 10, &pb.PutRequest{Key: []byte(proofPrefix + "lease"), Value: []byte(strconv.FormatInt(id, 10)), Lease: id, PrevKv: true})
			s.call(cl, 10, &pb.LeaseRevokeRequest{ID: id})
		}
	})
}

func (s *recordedLoad) now() int64 { return time.Since(s.origin).Nanoseconds() }
func (s *recordedLoad) call(cl *clientv3.Client, client int, q proto.Message) history.Op {
	req, err := frometcd.Request(q)
	op := history.Op{Client: client, Call: s.now(), Req: req}
	var response proto.Message
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		kv, lease := pb.NewKVClient(cl.ActiveConnection()), pb.NewLeaseClient(cl.ActiveConnection())
		switch q := q.(type) {
		case *pb.PutRequest:
			response, err = kv.Put(ctx, q)
		case *pb.RangeRequest:
			response, err = kv.Range(ctx, q)
		case *pb.DeleteRangeRequest:
			response, err = kv.DeleteRange(ctx, q)
		case *pb.TxnRequest:
			response, err = kv.Txn(ctx, q)
		case *pb.CompactionRequest:
			response, err = kv.Compact(ctx, q)
		case *pb.LeaseGrantRequest:
			response, err = lease.LeaseGrant(ctx, q)
		case *pb.LeaseRevokeRequest:
			response, err = lease.LeaseRevoke(ctx, q)
		default:
			err = fmt.Errorf("unsupported request %T", q)
		}
		cancel()
		if err == nil {
			op.Resp, err = frometcd.Response(response)
		}
	}
	op.Return = s.now()
	if err != nil {
		op.Err = &history.Error{Code: status.Code(err).String(), Message: err.Error(), Ambiguous: true}
	}
	s.mu.Lock()
	s.h.Ops = append(s.h.Ops, op)
	s.mu.Unlock()
	return op
}

func (s *recordedLoad) watch(ctx context.Context, cl *clientv3.Client, i int) {
	next := s.base + 2
	for ctx.Err() == nil {
		ch := cl.Watch(ctx, proofPrefix, clientv3.WithPrefix(), clientv3.WithRev(next), clientv3.WithPrevKV(), clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
		for wr := range ch {
			if ctx.Err() != nil {
				return
			}
			if err := wr.Err(); err != nil {
				s.mu.Lock()
				s.errs = append(s.errs, fmt.Errorf("watch %d: %w", i, err))
				s.mu.Unlock()
				return
			}
			r := history.WatchResponse{Time: s.now(), Revision: wr.Header.Revision, Created: wr.Created, Progress: wr.IsProgressNotify(), Canceled: wr.Canceled, CompactRevision: wr.CompactRevision}
			for _, e := range wr.Events {
				if e.Type != mvccpb.Event_PUT && e.Type != mvccpb.Event_DELETE {
					s.mu.Lock()
					s.errs = append(s.errs, fmt.Errorf("watch %d: unknown event type %d", i, e.Type))
					s.mu.Unlock()
					return
				}
				typ := history.EventPut
				if e.Type == mvccpb.Event_DELETE {
					typ = history.EventDelete
				}
				r.Events = append(r.Events, history.Event{Type: typ, KV: frometcd.KeyValue(e.Kv), PrevKV: frometcd.PrevKV(e.PrevKv)})
				next = max(next, e.Kv.ModRevision+1)
			}
			if r.Progress {
				next = max(next, r.Revision+1)
			}
			s.mu.Lock()
			s.h.Watches[i].Responses = append(s.h.Watches[i].Responses, r)
			s.mu.Unlock()
		}
		if ctx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (s *recordedLoad) stopTraffic() {
	s.stop()
	s.workers.Wait()
	if s.mixedEnd.IsZero() {
		s.mixedEnd = time.Now()
	}
}

func (s *recordedLoad) finish(out string) (err error) {
	s.stopTraffic()
	s.mu.Lock()
	var latencies []int64
	var failed int
	for _, op := range s.h.Ops {
		if op.Client == 50 {
			continue // The start marker precedes the measured interval.
		}
		if op.Err != nil {
			failed++
		}
		latencies = append(latencies, (op.Return-op.Call)/1000)
	}
	s.mu.Unlock()
	if len(latencies) == 0 {
		return errors.New("mixed interval completed no recorded requests")
	}
	slices.Sort(latencies)
	seconds := s.mixedEnd.Sub(s.mixedStart).Seconds()
	stats, statsErr := json.MarshalIndent(struct {
		Calls, Failed         int
		Seconds, OpsPerSecond float64
		P50Us, P99Us          int64
	}{len(latencies), failed, seconds, float64(len(latencies)) / seconds, latencies[len(latencies)/2], latencies[(len(latencies)-1)*99/100]}, "", "  ")
	if statsErr == nil {
		statsErr = os.WriteFile(filepath.Join(out, "recorded-load.json"), stats, 0o644)
	}
	err = errors.Join(err, statsErr)
	// Failed calls remain ambiguous. Wait beyond the server timeout before
	// the terminal reads, without retrying any mutation.
	time.Sleep(8 * time.Second)
	for i, cl := range s.clients {
		s.call(cl, 20+i, proofFence())
		op := s.call(cl, 20+i, &pb.RangeRequest{Key: []byte(proofPrefix), RangeEnd: []byte("/proof0")})
		if op.Resp == nil {
			err = errors.Join(err, fmt.Errorf("terminal read %d: %v", i, op.Err))
			continue
		}
		if s.h.Target != 0 && s.h.Target != op.Resp.Revision {
			err = errors.Join(err, errors.New("terminal revisions disagree"))
		}
		s.h.Target = op.Resp.Revision
	}
	for deadline := time.Now().Add(30 * time.Second); err == nil; {
		complete := true
		s.mu.Lock()
		for _, w := range s.h.Watches {
			var reached int64
			for _, r := range w.Responses {
				if r.Progress {
					reached = max(reached, r.Revision)
				}
				for _, e := range r.Events {
					reached = max(reached, e.Revision())
				}
			}
			complete = complete && reached >= s.h.Target
		}
		s.mu.Unlock()
		if complete {
			break
		}
		if time.Now().After(deadline) {
			err = errors.New("watch completeness timeout")
			break
		}
		for _, cl := range s.clients {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			e := cl.RequestProgress(ctx)
			cancel()
			if e != nil {
				err = errors.Join(err, e)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Compact only after all watches delivered the target. This exercises
	// compaction without declaring a canceled watch a complete stream.
	if err == nil {
		op := s.call(s.clients[0], 30, &pb.CompactionRequest{Revision: s.h.Target})
		if op.Err != nil {
			err = fmt.Errorf("compaction: %v", op.Err)
		}
		for i, cl := range s.clients {
			op := s.call(cl, 40+i, proofFence())
			if op.Resp == nil || op.Resp.Txn.Succeeded {
				err = errors.Join(err, fmt.Errorf("post-compaction fence %d: %v", i, op.Err))
			}
		}
	}
	s.stopW()
	s.watches.Wait()
	err = errors.Join(err, errors.Join(s.errs...))
	return errors.Join(err, s.save(out))
}

func (s *recordedLoad) save(out string) error {
	b, err := json.Marshal(struct {
		Base    int64           `json:"base"`
		History history.History `json:"history"`
	}{s.base, s.h})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "recorded.json"), b, 0o644)
}

// A failed CAS with a mutating branch appends a Raft entry without changing
// the KV revision. An empty Txn is read-only on etcd and cannot fence the WAL's
// durable commit hint after the last compaction.
func proofFence() *pb.TxnRequest {
	key := []byte(proofPrefix + "start")
	return &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: key, Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: -1}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: []byte("unreachable")}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}},
	}
}

func (s *recordedLoad) close() error {
	if s.stop != nil {
		s.stop()
		s.workers.Wait()
	}
	if s.stopW != nil {
		s.stopW()
		s.watches.Wait()
	}
	var err error
	for _, cl := range s.clients {
		err = errors.Join(err, cl.Close())
	}
	return err
}
