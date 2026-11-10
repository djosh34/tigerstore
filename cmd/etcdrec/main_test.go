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

// TestRecord passes a Range and a Watch through the proxy to a node that
// answers them, stops the proxy while the watch is still open, and reads
// the log: each call opens, sends, is answered and ends, the Range's key
// and revision are there, and the log closes cleanly.
func TestRecord(t *testing.T) {
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

	// The node: a Range gets a key-value; a Watch is created and stays.
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
	defer node.Stop()

	proxyLn, port, err := testport.Listen()
	if err != nil {
		t.Fatal(err)
	}
	if err := proxyLn.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "record.jsonl.gz")
	stop := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() {
		done <- run([]string{fmt.Sprintf("127.0.0.1:%d", port)}, []string{nodeLn.Addr().String()}, caFile, certFile, keyFile, clientFile, clientKeyFile, out, stop)
	}()

	cc, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{clientKP}})))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cc.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var rangeResp pb.RangeResponse
	if err := cc.Invoke(ctx, k8strace.MethodRange, &pb.RangeRequest{Key: []byte("/registry/pods/a")}, &rangeResp, grpc.WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	ws, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, k8strace.MethodWatch)
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

	// Stop with the watch still open: the log must close after every
	// call and forwarder has ended.
	stop <- os.Interrupt
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the proxy did not stop within 10s")
	}

	f, err := os.Open(out)
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
	events := map[int64][]string{}
	dec := json.NewDecoder(gz)
	for {
		var r k8strace.Record
		err := dec.Decode(&r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if r.Event == k8strace.EventOpen {
			events[r.Call] = append(events[r.Call], r.Method)
		}
		events[r.Call] = append(events[r.Call], r.Event)
		if r.Req != nil && r.Req.Range != nil && r.Req.Range.Key != "/registry/pods/a" {
			t.Errorf("range key %q", r.Req.Range.Key)
		}
		if r.Resp != nil && r.Resp.Rev != 7 {
			t.Errorf("answer at revision %d, want 7", r.Resp.Rev)
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
