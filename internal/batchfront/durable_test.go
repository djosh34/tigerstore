package batchfront_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/djosh34/tigerstore/internal/batchfront"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestBatchColdQuorum covers both follower ingress and cold recovery with any
// one member absent. It is not a crash/fault campaign or a clientv3 test.
func TestBatchColdQuorum(t *testing.T) {
	for lost := uint64(1); lost <= 3; lost++ {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			ca, err := testcert.NewCA()
			if err != nil {
				t.Fatal(err)
			}
			clientCert, _, _, err := ca.Issue("client", 99)
			if err != nil {
				t.Fatal(err)
			}
			peerAddrs := map[uint64]string{}
			dirs := map[uint64]string{}
			for id := uint64(1); id <= 3; id++ {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				peerAddrs[id] = ln.Addr().String()
				if err := ln.Close(); err != nil {
					t.Fatal(err)
				}
				dirs[id] = t.TempDir()
			}
			type running struct {
				kv   pb.KVClient
				stop func()
			}
			live := map[uint64]running{}
			start := func(id uint64) running {
				cert, _, _, err := ca.Issue("node", int64(id+10))
				if err != nil {
					t.Fatal(err)
				}
				n, err := node.New(node.Config{Core: core.DefaultConfig(id, []uint64{1, 2, 3}), DataDir: dirs[id], Tick: 20 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				peers := map[uint64]string{}
				for pid, addr := range peerAddrs {
					if pid != id {
						peers[pid] = addr
					}
				}
				tr, err := peer.Start(peer.Config{ID: id, Listen: peerAddrs[id], Peers: peers, Certificate: cert, CA: ca.Pool, Deliver: n.Deliver, Unreachable: n.Unreachable})
				if err != nil {
					t.Fatal(err)
				}
				front, err := batchfront.New(n, server.Config{TLS: &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: ca.Pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}}, "full")
				if err != nil {
					t.Fatal(err)
				}
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				ran, served := make(chan error, 1), make(chan error, 1)
				go func() { ran <- n.Run(ctx, tr) }()
				go func() { served <- front.Serve(ln) }()
				cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: ca.Pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})))
				if err != nil {
					t.Fatal(err)
				}
				return running{kv: pb.NewKVClient(cc), stop: func() {
					cancel()
					if err := <-ran; err != nil {
						t.Errorf("node %d: %v", id, err)
					}
					if err := errors.Join(cc.Close(), front.Stop(), <-served, tr.Close()); err != nil {
						t.Errorf("stop %d: %v", id, err)
					}
				}}
			}
			stopAll := func() {
				for id, r := range live {
					r.stop()
					delete(live, id)
				}
			}
			t.Cleanup(stopAll)
			for id := uint64(1); id <= 3; id++ {
				live[id] = start(id)
			}
			// Retry only the startup probe. Every checked write below runs once.
			until := time.Now().Add(10 * time.Second)
			for {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := live[1].kv.Range(ctx, &pb.RangeRequest{Key: []byte("probe")})
				cancel()
				if err == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal(err)
				}
				time.Sleep(20 * time.Millisecond)
			}
			want := map[string]string{}
			var last int64
			for i := range 12 {
				id := uint64(i%3 + 1)
				key, value := fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%02d", i)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				r, err := live[id].kv.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if r.Header.MemberId != id || r.Header.Revision <= last {
					t.Fatalf("bad Put header: %v", r.Header)
				}
				last = r.Header.Revision
				want[key] = value
			}
			cas := &pb.TxnRequest{
				Compare: []*pb.Compare{{Key: []byte("key-11"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: last}}},
				Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("key-11"), Value: []byte("cas"), PrevKv: true}}}},
				Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("key-11")}}}},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, err := live[2].kv.Txn(ctx, cas)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if !r.Succeeded || r.Header.Revision != last+1 || len(r.Responses) != 1 || string(r.Responses[0].GetResponsePut().GetPrevKv().GetValue()) != "value-11" {
				t.Fatalf("CAS: %v", r)
			}
			last = r.Header.Revision
			want["key-11"] = "cas"
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			r, err = live[3].kv.Txn(ctx, cas)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if r.Succeeded || r.Header.Revision != last || len(r.Responses) != 1 || len(r.Responses[0].GetResponseRange().GetKvs()) != 1 || string(r.Responses[0].GetResponseRange().GetKvs()[0].Value) != "cas" {
				t.Fatalf("failed CAS: %v", r)
			}
			stopAll()
			for id := uint64(1); id <= 3; id++ {
				if id != lost {
					live[id] = start(id)
				}
			}
			readID := lost%3 + 1
			for key, value := range want {
				until = time.Now().Add(10 * time.Second)
				for {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					r, err := live[readID].kv.Range(ctx, &pb.RangeRequest{Key: []byte(key)})
					cancel()
					if err != nil {
						if time.Now().After(until) {
							t.Fatal(err)
						}
						time.Sleep(20 * time.Millisecond)
						continue
					}
					if r.Header.Revision < last || len(r.Kvs) != 1 || string(r.Kvs[0].Value) != value {
						t.Fatalf("lost node %d: %s=%v, want %q, revision >=%d", lost, key, r, value, last)
					}
					break
				}
			}
		})
	}
}
