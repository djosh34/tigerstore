// Throwaway protocol probe. Not product code and not a Kubernetes integration test.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func emit(label string, value any) {
	b, err := json.Marshal(value)
	must(err)
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(string(b)))
	decoder.UseNumber()
	must(decoder.Decode(&decoded))
	scrub(decoded)
	b, err = json.Marshal(decoded)
	must(err)
	fmt.Printf("%s %s\n", label, b)
}

// Keep small exact values. Large base64 value fields are replaced only in logs.
func scrub(v any) {
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			if text, ok := value.(string); ok && key == "value" && len(text) > 128 {
				x[key] = fmt.Sprintf("<base64 value, %d characters>", len(text))
			} else {
				scrub(value)
			}
		}
	case []any:
		for _, value := range x {
			scrub(value)
		}
	}
}
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func check(ok bool, msg string) {
	if !ok {
		must(fmt.Errorf("assertion failed: %s", msg))
	}
}
func trace(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	emit("request", map[string]any{"method": method, "body": req})
	err := invoke(ctx, method, req, reply, conn, opts...)
	if err != nil {
		emit("rpc-error", map[string]any{"method": method, "code": status.Code(err).String(), "message": status.Convert(err).Message()})
	}
	return err
}
func streamTrace(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	emit("stream", map[string]any{"method": method, "metadata": md})
	s, err := streamer(ctx, desc, conn, method, opts...)
	if err != nil {
		return nil, err
	}
	return &loggedStream{ClientStream: s, method: method}, nil
}

type loggedStream struct {
	grpc.ClientStream
	method string
}

func (s *loggedStream) SendMsg(m any) error {
	emit("stream-request", map[string]any{"method": s.method, "body": m})
	return s.ClientStream.SendMsg(m)
}

func watch(c *clientv3.Client, key string, rev int64, progress, fragment bool) (pb.Watch_WatchClient, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(context.Background()), 5*time.Second)
	s, err := pb.NewWatchClient(c.ActiveConnection()).Watch(ctx)
	must(err)
	must(s.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte(key), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(key)), StartRevision: rev, PrevKv: true, ProgressNotify: progress, Fragment: fragment}}}))
	r, err := s.Recv()
	must(err)
	check(r.Created && !r.Canceled, "watch created")
	emit("watch-created", r)
	return s, cancel
}
func recv(s pb.Watch_WatchClient) *pb.WatchResponse { r, err := s.Recv(); must(err); return r }
func brief(r *pb.WatchResponse) any {
	events := []any{}
	for _, e := range r.Events {
		events = append(events, map[string]any{"type": e.Type.String(), "key": string(e.Kv.Key), "mod": e.Kv.ModRevision, "create": e.Kv.CreateRevision, "version": e.Kv.Version, "value_bytes": len(e.Kv.Value), "prev": e.PrevKv != nil})
	}
	return map[string]any{"header": r.Header, "events": events, "canceled": r.Canceled, "compact_revision": r.CompactRevision, "cancel_reason": r.CancelReason, "fragment": r.Fragment, "watch_id": r.WatchId}
}
func probeNoLeader(c *clientv3.Client) {
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(context.Background()), 3*time.Second)
	defer cancel()
	_, err := pb.NewKVClient(c.ActiveConnection()).Range(ctx, &pb.RangeRequest{Key: []byte("health")})
	check(status.Code(err).String() == "Unavailable", "require-leader unary error")
	emit("no-leader-range", status.Convert(err).Message())
	stream, err := pb.NewWatchClient(c.ActiveConnection()).Watch(ctx)
	must(err)
	must(stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("health")}}}))
	_, err = stream.Recv()
	check(status.Code(err).String() == "Unavailable", "require-leader stream error")
	emit("no-leader-watch", status.Convert(err).Message())
}

func main() {
	cert, err := tls.LoadX509KeyPair(os.Getenv("CERT"), os.Getenv("KEY"))
	must(err)
	ca, err := os.ReadFile(os.Getenv("CA"))
	must(err)
	pool := x509.NewCertPool()
	check(pool.AppendCertsFromPEM(ca), "CA")
	endpoint := os.Getenv("ENDPOINT")
	if endpoint == "" {
		endpoint = "https://127.0.0.1:32379"
	}
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, TLS: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12}, DialTimeout: 3 * time.Second, DialKeepAliveTime: 30 * time.Second, DialKeepAliveTimeout: 10 * time.Second, DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(trace), grpc.WithChainStreamInterceptor(streamTrace)}})
	must(err)
	defer c.Close()
	if os.Getenv("MODE") == "no-leader" {
		probeNoLeader(c)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := pb.NewKVClient(c.ActiveConnection())
	l := pb.NewLeaseClient(c.ActiveConnection())
	// Grant does not change the MVCC revision. Nonpositive TTLs are clamped, not rejected.
	for _, ttl := range []int64{-1, 0, 1, 2} {
		r, e := l.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: ttl})
		must(e)
		check(r.TTL == 2, "default TTL minimum 2")
		emit("grant-min", r)
		_, e = l.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: r.ID})
		must(e)
	}
	_, err = l.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 9000000001})
	check(status.Code(err).String() == "OutOfRange", "max TTL")
	emit("grant-too-large", status.Convert(err).Message())
	grant, err := l.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 10, ID: 777})
	must(err)
	_, err = l.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 10, ID: 777})
	check(status.Code(err).String() == "FailedPrecondition", "duplicate ID")
	_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("missing-lease"), Value: []byte("x"), Lease: 999999})
	check(status.Code(err).String() == "NotFound", "put missing lease")
	// These are the exact etcd Kubernetes helper calls in all three inspected minors.
	kc := kubernetes.Client{Client: c}
	kc.Kubernetes = &kc
	_, err = kc.Kubernetes.Get(ctx, "/registry/p/k", kubernetes.GetOptions{})
	must(err)
	p, err := kc.Kubernetes.OptimisticPut(ctx, "/registry/p/k", []byte("a"), 0, kubernetes.PutOptions{LeaseID: clientv3.LeaseID(grant.ID)})
	must(err)
	_, err = kc.Kubernetes.OptimisticPut(ctx, "/registry/p/k", []byte("bad"), 0, kubernetes.PutOptions{GetOnFailure: true})
	must(err)
	_, err = kc.Kubernetes.List(ctx, "/registry/p/", kubernetes.ListOptions{Limit: 1, Revision: p.Revision})
	must(err)
	_, err = kc.Kubernetes.Count(ctx, "/registry/p/", kubernetes.CountOptions{})
	must(err)
	_, err = c.Get(ctx, "/registry/p/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	must(err)
	_, err = kc.Kubernetes.OptimisticDelete(ctx, "/registry/p/k", p.Revision-1, kubernetes.DeleteOptions{GetOnFailure: true})
	must(err)
	_, err = kc.Kubernetes.OptimisticDelete(ctx, "/registry/p/k", p.Revision, kubernetes.DeleteOptions{GetOnFailure: true})
	must(err)
	// Compactor Txn with VERSION, and no Physical option.
	tr, err := c.Txn(ctx).If(clientv3.Compare(clientv3.Version("compact_rev_key"), "=", 0)).Then(clientv3.OpPut("compact_rev_key", "0")).Else(clientv3.OpGet("compact_rev_key")).Commit()
	must(err)
	// Historical reads have a current header, not a header pinned to the requested revision.
	_, err = c.Put(ctx, "historical", "a")
	must(err)
	first, err := c.Get(ctx, "historical")
	must(err)
	_, err = c.Put(ctx, "historical", "b")
	must(err)
	old, err := c.Get(ctx, "historical", clientv3.WithRev(first.Header.Revision))
	must(err)
	check(string(old.Kvs[0].Value) == "a" && old.Header.Revision > first.Header.Revision, "historical header")
	emit("historical", old)
	readOnly, err := c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("historical"), ">", 0)).Then(clientv3.OpGet("historical")).Commit()
	must(err)
	check(readOnly.Header.Revision == old.Header.Revision, "read-only txn no bump")
	failedWrite, err := c.Txn(ctx).If(clientv3.Compare(clientv3.Version("historical"), "=", 99)).Else(clientv3.OpPut("failed-branch", "yes")).Commit()
	must(err)
	check(!failedWrite.Succeeded && failedWrite.Header.Revision == old.Header.Revision+1, "failed compare mutating branch bump")
	emit("revision-shapes", map[string]any{"read_only": readOnly.Header.Revision, "failed_mutation": failedWrite.Header.Revision, "compactor": tr.Header.Revision})
	// Create, update and delete prev_kv rules; periodic and requested progress.
	s, stop := watch(c, "events/", 0, true, false)
	_, err = c.Put(ctx, "events/a", "a")
	must(err)
	r := recv(s)
	emit("create", brief(r))
	check(len(r.Events) == 1 && r.Events[0].PrevKv == nil, "create absent prev_kv")
	_, err = c.Put(ctx, "events/a", "b")
	must(err)
	r = recv(s)
	emit("update", brief(r))
	check(len(r.Events) == 1 && string(r.Events[0].PrevKv.Value) == "a", "update prev")
	_, err = c.Delete(ctx, "events/a")
	must(err)
	r = recv(s)
	emit("delete", brief(r))
	check(len(r.Events) == 1 && string(r.Events[0].PrevKv.Value) == "b", "delete prev")
	r = recv(s)
	emit("periodic-progress", brief(r))
	check(len(r.Events) == 0 && !r.Created && !r.Canceled, "periodic progress")
	must(s.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}))
	r = recv(s)
	emit("requested-progress", brief(r))
	check(r.WatchId == -1 && len(r.Events) == 0, "requested stream-wide progress")
	must(s.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{CancelRequest: &pb.WatchCancelRequest{WatchId: 0}}}))
	r = recv(s)
	emit("explicit-cancel", brief(r))
	check(r.Canceled && r.CancelReason == "", "cancel reason")
	stop()
	// Public client sends the same watch options and RequestProgress message.
	wctx, wcancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), 2*time.Second)
	wch := c.Watch(wctx, "client/", clientv3.WithRev(1), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
	<-wch
	must(c.RequestProgress(wctx))
	wr := <-wch
	check(wr.IsProgressNotify(), "client progress")
	emit("client-progress", wr)
	wcancel()
	// Revoke removes only currently attached keys, in one revision.
	a, err := c.Grant(ctx, 10)
	must(err)
	b, err := c.Grant(ctx, 10)
	must(err)
	for _, key := range []string{"leases/a", "leases/b", "leases/detach", "leases/move"} {
		_, err = c.Put(ctx, key, "x", clientv3.WithLease(a.ID))
		must(err)
	}
	_, err = c.Put(ctx, "leases/detach", "free")
	must(err)
	_, err = c.Put(ctx, "leases/move", "other", clientv3.WithLease(b.ID))
	must(err)
	ls, lstop := watch(c, "leases/", 0, false, false)
	_, err = c.Revoke(ctx, a.ID)
	must(err)
	r = recv(ls)
	emit("revoke-events", brief(r))
	check(len(r.Events) == 2 && r.Events[0].Kv.ModRevision == r.Events[1].Kv.ModRevision, "single revoke revision")
	lstop()
	left, err := c.Get(ctx, "leases/", clientv3.WithPrefix())
	must(err)
	check(len(left.Kvs) == 2, "detached keys survive")
	ka, err := l.LeaseKeepAlive(ctx)
	must(err)
	must(ka.Send(&pb.LeaseKeepAliveRequest{ID: int64(a.ID)}))
	kr, err := ka.Recv()
	must(err)
	check(kr.TTL == 0, "missing keepalive TTL 0")
	emit("missing-keepalive", kr)
	must(ka.CloseSend())
	ttr, err := l.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: int64(a.ID), Keys: true})
	must(err)
	check(ttr.TTL == -1, "missing time-to-live TTL -1")
	emit("missing-ttl", ttr)
	// Healthy-clock expiry of two keys, plus keepalive extending a separate lease.
	start := time.Now()
	exp, err := c.Grant(ctx, 1)
	must(err)
	_, err = c.Put(ctx, "expire/a", "a", clientv3.WithLease(exp.ID))
	must(err)
	_, err = c.Put(ctx, "expire/b", "b", clientv3.WithLease(exp.ID))
	must(err)
	es, estop := watch(c, "expire/", 0, false, false)
	r = recv(es)
	elapsed := time.Since(start)
	emit("expiry", map[string]any{"elapsed_ms": elapsed.Milliseconds(), "response": brief(r)})
	check(elapsed >= 2*time.Second && len(r.Events) == 2 && r.Events[0].Kv.ModRevision == r.Events[1].Kv.ModRevision, "expiry TTL and single revision")
	estop()
	keep, err := c.Grant(ctx, 1)
	must(err)
	_, err = c.Put(ctx, "keepalive", "x", clientv3.WithLease(keep.ID))
	must(err)
	time.Sleep(1500 * time.Millisecond)
	renewed, err := c.KeepAliveOnce(ctx, keep.ID)
	must(err)
	check(renewed.TTL == 2, "keepalive resets granted TTL")
	time.Sleep(1100 * time.Millisecond)
	still, err := c.Get(ctx, "keepalive")
	must(err)
	check(len(still.Kvs) == 1, "renewed survives original deadline")
	emit("keepalive-survived", map[string]any{"after_grant_ms": 2600, "renewed_ttl": renewed.TTL})
	_, err = c.Revoke(ctx, keep.ID)
	must(err)
	// Fragment request needs enough response bytes, not merely many events.
	ops := []clientv3.Op{}
	for i := 0; i < 24; i++ {
		ops = append(ops, clientv3.OpPut(fmt.Sprintf("fragment/%02d", i), strings.Repeat("a", 65536)))
	}
	_, err = c.Txn(ctx).Then(ops...).Commit()
	must(err)
	fs, fstop := watch(c, "fragment/", 0, false, true)
	ns, nstop := watch(c, "fragment/", 0, false, false)
	ops = nil
	for i := 0; i < 24; i++ {
		ops = append(ops, clientv3.OpPut(fmt.Sprintf("fragment/%02d", i), strings.Repeat("b", 65536)))
	}
	_, err = c.Txn(ctx).Then(ops...).Commit()
	must(err)
	chunks, total := 0, 0
	for {
		r = recv(fs)
		chunks++
		total += len(r.Events)
		emit("fragment-chunk", brief(r))
		if !r.Fragment {
			break
		}
	}
	check(chunks > 1 && total == 24, "fragment splits and ends false")
	fstop()
	r = recv(ns)
	emit("no-fragment", map[string]any{"event_count": len(r.Events), "fragment": r.Fragment})
	check(len(r.Events) == 24 && !r.Fragment, "unfragmented whole revision")
	nstop()
	// Physically compact at the revision of a delete. The boundary itself remains watchable.
	_, err = c.Put(ctx, "boundary", "x")
	must(err)
	dr, err := c.Delete(ctx, "boundary")
	must(err)
	C := dr.Header.Revision
	beforeBoundary, err := kv.Compact(ctx, &pb.CompactionRequest{Revision: C - 1, Physical: true})
	must(err)
	emit("compact-R-minus-1", beforeBoundary)
	comp, err := kv.Compact(ctx, &pb.CompactionRequest{Revision: C, Physical: true})
	must(err)
	emit("compact", comp)
	for _, rev := range []int64{C - 1, C, C + 1} {
		rr, e := kv.Range(ctx, &pb.RangeRequest{Key: []byte("boundary"), Revision: rev})
		emit("range-boundary", map[string]any{"requested": rev, "response": rr, "code": status.Code(e).String(), "message": status.Convert(e).Message()})
		if rev != C {
			check(status.Code(e).String() == "OutOfRange", "range boundary error")
		} else {
			must(e)
		}
	}
	// Raw watch error travels inside a normal response; equality is allowed and includes the delete.
	for _, rev := range []int64{C - 1, C} {
		bs, bstop := watch(c, "boundary", rev, false, false)
		r = recv(bs)
		emit("watch-boundary", map[string]any{"requested": rev, "response": brief(r)})
		if rev < C {
			check(r.Canceled && r.CompactRevision == C && r.CancelReason == "", "compacted watch cancel")
		} else {
			check(len(r.Events) == 1 && r.Events[0].Type.String() == "DELETE", "watch at C retains delete")
		}
		bstop()
	}
	_, err = c.Get(ctx, "boundary", clientv3.WithRev(C-1))
	check(errors.Is(err, rpctypes.ErrCompacted), "public Range mapped compaction error")
	emit("client-compacted-range", map[string]any{"mapped_code": err.(rpctypes.EtcdError).Code().String(), "grpc_status_code": status.Code(err).String(), "message": err.Error()})
	compactedCtx, compactedStop := context.WithTimeout(ctx, 2*time.Second)
	compactedResponse := <-c.Watch(compactedCtx, "boundary", clientv3.WithRev(C-1))
	check(compactedResponse.Canceled && errors.Is(compactedResponse.Err(), rpctypes.ErrCompacted), "public Watch mapped compaction error")
	emit("client-compacted-watch", map[string]any{"mapped_code": compactedResponse.Err().(rpctypes.EtcdError).Code().String(), "grpc_status_code": status.Code(compactedResponse.Err()).String(), "message": compactedResponse.Err().Error()})
	compactedStop()
	_, err = kv.Compact(ctx, &pb.CompactionRequest{Revision: C})
	check(status.Code(err).String() == "OutOfRange", "repeat compact error")
	_, err = kv.Compact(ctx, &pb.CompactionRequest{Revision: C + 1})
	check(status.Code(err).String() == "OutOfRange", "future compact error")
	futureCtx, futureStop := context.WithTimeout(ctx, 2*time.Second)
	futureWatch := c.Watch(futureCtx, "boundary", clientv3.WithRev(C+2), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	futureCreated := <-futureWatch
	check(futureCreated.Created, "future watch is created, not rejected")
	_, err = c.Put(ctx, "boundary", "before-target")
	must(err)
	select {
	case premature := <-futureWatch:
		check(false, fmt.Sprintf("future watch returned early: %+v", premature))
	case <-time.After(50 * time.Millisecond):
	}
	_, err = c.Put(ctx, "boundary", "at-target")
	must(err)
	futureResponse := <-futureWatch
	check(len(futureResponse.Events) == 1 && futureResponse.Events[0].Kv.ModRevision == C+2, "future watch starts inclusively at its target")
	emit("future-watch", map[string]any{"requested": C + 2, "delivered_revision": futureResponse.Events[0].Kv.ModRevision, "value": string(futureResponse.Events[0].Kv.Value)})
	futureStop()
	statusResp, err := c.Status(ctx, endpoint)
	must(err)
	emit("status", statusResp)
	members, err := c.MemberList(ctx)
	must(err)
	emit("members", members)
	emit("PASS", map[string]any{"compact_revision": C, "fragment_chunks": chunks, "expiry_ms": elapsed.Milliseconds()})
}
