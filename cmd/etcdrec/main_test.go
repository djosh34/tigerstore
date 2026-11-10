package main

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/k8strace"
	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/djosh34/tigerstore/internal/testport"
)

// rig is a proxy in front of a node that answers a Range with a
// key-value at revision 7 and creates a Watch, which stays open, and a
// client of the proxy.
type rig struct {
	cc   *grpc.ClientConn
	stop chan os.Signal
	done chan error // run's result
	out  string     // the log
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caFile := write("ca.pem", ca.PEM)
	serverKP, certPEM, keyPEM, err := ca.Issue("server", 2)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := write("server.pem", certPEM), write("server-key.pem", keyPEM)
	clientKP, clientPEM, clientKeyPEM, err := ca.Issue("client", 3)
	if err != nil {
		t.Fatal(err)
	}
	clientFile, clientKeyFile := write("client.pem", clientPEM), write("client-key.pem", clientKeyPEM)

	node := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverKP}, ClientCAs: ca.Pool, ClientAuth: tls.RequireAndVerifyClientCert})),
		grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(func(_ any, ss grpc.ServerStream) error {
			method, _ := grpc.MethodFromServerStream(ss)
			var req []byte
			if err := ss.RecvMsg(&req); err != nil {
				return err
			}
			var resp proto.Message = &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 7}, Count: 1}
			if method == k8strace.MethodWatch {
				resp = &pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 7}, Created: true}
			}
			b, err := proto.Marshal(resp)
			if err != nil {
				return err
			}
			if err := ss.SendMsg(&b); err != nil {
				return err
			}
			if method == k8strace.MethodWatch {
				<-ss.Context().Done()
			}
			return nil
		}))
	nodeLn, _, err := testport.Listen()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = node.Serve(nodeLn) }()
	t.Cleanup(node.Stop)

	proxyLn, port, err := testport.Listen()
	if err != nil {
		t.Fatal(err)
	}
	if err := proxyLn.Close(); err != nil {
		t.Fatal(err)
	}
	r := &rig{stop: make(chan os.Signal, 1), done: make(chan error, 1), out: filepath.Join(dir, "record.jsonl.gz")}
	go func() {
		r.done <- run([]string{fmt.Sprintf("127.0.0.1:%d", port)}, []string{nodeLn.Addr().String()}, caFile, certFile, keyFile, clientFile, clientKeyFile, r.out, r.stop)
	}()
	r.cc, err = grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{clientKP}})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.cc.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

// call makes a Range through the proxy, and opens a Watch that it leaves
// open.
func (r *rig) call(t *testing.T, ctx context.Context) grpc.ClientStream {
	t.Helper()
	var rangeResp pb.RangeResponse
	if err := r.cc.Invoke(ctx, k8strace.MethodRange, &pb.RangeRequest{Key: []byte("/registry/pods/a")}, &rangeResp, grpc.WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	ws, err := r.cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, k8strace.MethodWatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.SendMsg(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("/registry/pods/")}}}); err != nil {
		t.Fatal(err)
	}
	var created pb.WatchResponse
	if err := ws.RecvMsg(&created); err != nil || !created.Created {
		t.Fatalf("watch: %v, created %v", err, created.Created)
	}
	return ws
}

// stopped stops the proxy and waits for run to end without error.
func (r *rig) stopped(t *testing.T) {
	t.Helper()
	r.stop <- os.Interrupt
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the proxy did not stop within 10s")
	}
}

// records reads the log.
func (r *rig) records(t *testing.T) []k8strace.Record {
	t.Helper()
	f, err := os.Open(r.out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []k8strace.Record
	dec := json.NewDecoder(gz)
	for {
		var rec k8strace.Record
		err := dec.Decode(&rec)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
}

// TestRecord passes a Range and a Watch through the proxy, stops the proxy
// while the watch is still open, and reads the log: each call opens,
// sends, is answered and ends, the Range's key and revision are there, and
// the log closes cleanly.
func TestRecord(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.call(t, ctx)
	r.stopped(t)
	events := map[int64][]string{}
	for _, rec := range r.records(t) {
		if rec.Event == k8strace.EventOpen {
			events[rec.Call] = append(events[rec.Call], rec.Method)
		}
		events[rec.Call] = append(events[rec.Call], rec.Event)
		if rec.Req != nil && rec.Req.Range != nil && rec.Req.Range.Key != "/registry/pods/a" {
			t.Errorf("range key %q", rec.Req.Range.Key)
		}
		if rec.Resp != nil && rec.Resp.Rev != 7 {
			t.Errorf("answer at revision %d, want 7", rec.Resp.Rev)
		}
	}
	want := map[int64][]string{
		1: {k8strace.MethodRange, "open", "req", "resp", "end"},
		2: {k8strace.MethodWatch, "open", "req", "resp", "end"},
	}
	for call, w := range want {
		if fmt.Sprint(events[call]) != fmt.Sprint(w) {
			t.Errorf("call %d: %v, want %v", call, events[call], w)
		}
	}
}

// TestRecordHeldRequest stops the proxy while a request it has received
// is not logged yet: the log stays open until that request is in it.
func TestRecordHeldRequest(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ws := r.call(t, ctx)

	held, release := make(chan struct{}), make(chan struct{})
	forwarded = func() {
		close(held)
		<-release
	}
	defer func() { forwarded = nil }()
	if err := ws.SendMsg(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}); err != nil {
		t.Fatal(err)
	}
	<-held
	r.stop <- os.Interrupt
	// Long enough for the proxy to stop its servers, and, if it did not
	// wait for its forwarders, to close the log.
	time.Sleep(300 * time.Millisecond)
	close(release)
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the proxy did not stop within 10s")
	}
	for _, rec := range r.records(t) {
		if rec.Req != nil && rec.Req.Watch != nil && rec.Req.Watch.Progress {
			return
		}
	}
	t.Error("the progress request the proxy held at its stop is not in the log")
}
