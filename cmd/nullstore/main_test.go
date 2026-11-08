package main

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestEtcdClient drives nullstore with etcd's own client, which decodes
// every answer in full: each must be a well-formed etcd answer.
func TestEtcdClient(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	kp, _, _, err := ca.IssueFor("node", 1, []net.IP{net.IPv4(127, 0, 0, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{value: make([]byte, 10), listKeys: 3, watchers: make(map[*watcher]struct{})}
	srv, _ := s.serve(kp, []net.Listener{ln})
	defer srv.Stop()
	c, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ln.Addr().String()}, DialTimeout: 5 * time.Second,
		TLS: &tls.Config{RootCAs: ca.Pool, MinVersion: tls.VersionTLS13}, Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	watch := c.Watch(ctx, "/registry/pods/", clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	if created := <-watch; !created.Created {
		t.Fatalf("watch not created: %+v", created)
	}
	put, err := c.Put(ctx, "/registry/pods/a", "v1")
	if err != nil || put.Header.Revision != 1 {
		t.Fatalf("put: %v %v", put, err)
	}
	txn, err := c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("/registry/pods/a"), "=", 1)).
		Then(clientv3.OpPut("/registry/pods/a", "v2")).Else(clientv3.OpGet("/registry/pods/a")).Commit()
	if err != nil || !txn.Succeeded || txn.Header.Revision != 2 || len(txn.Responses) != 1 {
		t.Fatalf("txn: %v %v", txn, err)
	}
	del, err := c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("/registry/pods/a"), "=", 2)).
		Then(clientv3.OpDelete("/registry/pods/a")).Commit()
	if err != nil || !del.Succeeded || del.Responses[0].GetResponseDeleteRange().Deleted != 1 {
		t.Fatalf("delete: %v %v", del, err)
	}
	var events []*clientv3.Event
	for len(events) < 3 {
		w := <-watch
		if err := w.Err(); err != nil {
			t.Fatal(err)
		}
		events = append(events, w.Events...)
	}
	if string(events[1].Kv.Value) != "v2" || events[1].PrevKv == nil || events[0].PrevKv != nil ||
		events[2].Type != clientv3.EventTypeDelete || events[2].PrevKv == nil || len(events[2].PrevKv.Value) != 10 {
		t.Fatalf("events %v", events)
	}
	get, err := c.Get(ctx, "/registry/pods/a")
	if err != nil || len(get.Kvs) != 1 || len(get.Kvs[0].Value) != 10 {
		t.Fatalf("get: %v %v", get, err)
	}
	rev, err := c.Get(ctx, "/registry/pods", clientv3.WithLimit(1))
	if err != nil || len(rev.Kvs) != 0 || rev.Header.Revision != 3 {
		t.Fatalf("revision read: %v %v", rev, err)
	}
	list, err := c.Get(ctx, "/registry/pods/", clientv3.WithPrefix(), clientv3.WithLimit(2))
	if err != nil || len(list.Kvs) != 2 || !list.More {
		t.Fatalf("list: %v %v", list, err)
	}
	lease, err := c.Grant(ctx, 3600)
	if err != nil || lease.ID == 0 || lease.TTL != 3600 {
		t.Fatalf("lease: %v %v", lease, err)
	}
}
