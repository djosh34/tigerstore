package wl

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/djosh34/tigerstore/internal/testport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// portPair finds a free peer port whose port below is free too: the loop
// takes a member's client port to be its peer port less one.
func portPair(t *testing.T) int {
	t.Helper()
	for range 100 {
		ln, p, err := testport.Listen()
		if err != nil {
			t.Fatal(err)
		}
		below, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p-1))
		_ = ln.Close()
		if err == nil {
			_ = below.Close()
			return p
		}
	}
	t.Fatal("no free pair of ports")
	return 0
}

// startCluster runs a 3-node cluster in this process and returns a
// client of it. The nodes run until the test binary ends.
func startCluster(t *testing.T) *clientv3.Client {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, certPEM, keyPEM, err := ca.Issue("node", 1)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"ca.pem": ca.PEM, "cert.pem": certPEM, "key.pem": keyPEM}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	certFile, keyFile, caFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
	var peers, initial, endpoints []string
	for i := range 3 {
		p := portPair(t)
		peers = append(peers, fmt.Sprintf("https://127.0.0.1:%d", p))
		initial = append(initial, fmt.Sprintf("n%d=%s", i+1, peers[i]))
		endpoints = append(endpoints, fmt.Sprintf("127.0.0.1:%d", p-1))
	}
	logger := log.New(io.Discard, "", 0)
	stopped := make(chan error, 3)
	for i := range 3 {
		client := "https://" + endpoints[i]
		go func() {
			err := run(logger, fmt.Sprintf("n%d", i+1), filepath.Join(dir, fmt.Sprintf("n%d", i+1)), client, client, peers[i],
				strings.Join(initial, ","), certFile, keyFile, caFile, certFile, keyFile, caFile)
			stopped <- fmt.Errorf("node %d stopped: %w", i+1, err)
		}()
	}
	t.Cleanup(func() {
		select {
		case err := <-stopped:
			t.Error(err)
		default:
		}
	})
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: ca.Pool, MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if _, err := c.Put(ctx, "/ready", "1"); err == nil {
			return c
		}
		if ctx.Err() != nil {
			t.Fatal("the cluster took no write in 30 s")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestClusterServesTheEtcdAPI(t *testing.T) {
	c := startCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("put, get and delete", func(t *testing.T) {
		if _, err := c.Put(ctx, "/kv/a", "1"); err != nil {
			t.Fatal(err)
		}
		r, err := c.Get(ctx, "/kv/a")
		if err != nil || len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "1" {
			t.Fatalf("get /kv/a: %v %v", r, err)
		}
		d, err := c.Delete(ctx, "/kv/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		if err != nil || d.Deleted != 1 || len(d.PrevKvs) != 1 {
			t.Fatalf("delete /kv/: %v %v", d, err)
		}
	})

	t.Run("txn", func(t *testing.T) {
		r, err := c.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision("/txn/k"), "=", 0)).
			Then(clientv3.OpPut("/txn/k", "v")).Else(clientv3.OpGet("/txn/k")).Commit()
		if err != nil || !r.Succeeded {
			t.Fatalf("first txn: %v %v", r, err)
		}
		r, err = c.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision("/txn/k"), "=", 0)).
			Then(clientv3.OpPut("/txn/k", "w")).Else(clientv3.OpGet("/txn/k")).Commit()
		if err != nil || r.Succeeded || string(r.Responses[0].GetResponseRange().Kvs[0].Value) != "v" {
			t.Fatalf("second txn: %v %v", r, err)
		}
		if _, err := c.Txn(ctx).Then(clientv3.OpPut("/txn/d", "1"), clientv3.OpPut("/txn/d", "2")).Commit(); err == nil {
			t.Fatal("a txn putting one key twice was taken")
		}
	})

	t.Run("watch from a past revision", func(t *testing.T) {
		p, err := c.Put(ctx, "/w/a", "1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/w/b", "2"); err != nil {
			t.Fatal(err)
		}
		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()
		ch := c.Watch(wctx, "/w/", clientv3.WithPrefix(), clientv3.WithRev(p.Header.Revision))
		if _, err := c.Put(ctx, "/w/c", "3"); err != nil {
			t.Fatal(err)
		}
		var got []string
		for len(got) < 3 {
			select {
			case r := <-ch:
				for _, ev := range r.Events {
					got = append(got, string(ev.Kv.Key))
				}
			case <-ctx.Done():
				t.Fatalf("events %q before the deadline", got)
			}
		}
		if strings.Join(got, " ") != "/w/a /w/b /w/c" {
			t.Fatalf("events %q", got)
		}
	})

	t.Run("leases", func(t *testing.T) {
		g, err := c.Grant(ctx, 60)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/lease/k", "v", clientv3.WithLease(g.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.KeepAliveOnce(ctx, g.ID); err != nil {
			t.Fatal(err)
		}
		ttl, err := c.TimeToLive(ctx, g.ID, clientv3.WithAttachedKeys())
		if err != nil || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != "/lease/k" {
			t.Fatalf("time to live: %v %v", ttl, err)
		}
		if _, err := c.Revoke(ctx, g.ID); err != nil {
			t.Fatal(err)
		}
		r, err := c.Get(ctx, "/lease/k")
		if err != nil || len(r.Kvs) != 0 {
			t.Fatalf("the leased key after the revoke: %v %v", r, err)
		}
		if _, err := c.Put(ctx, "/lease/k", "v", clientv3.WithLease(g.ID)); err == nil {
			t.Fatal("a put to a revoked lease was taken")
		}
	})

	t.Run("large range", func(t *testing.T) {
		v := strings.Repeat("x", 1000)
		for i := range 300 {
			if _, err := c.Put(ctx, fmt.Sprintf("/big/%04d", i), v); err != nil {
				t.Fatal(err)
			}
		}
		r, err := c.Get(ctx, "/big/", clientv3.WithPrefix())
		if err != nil || len(r.Kvs) != 300 {
			t.Fatalf("a 300 KB range: %d keys, %v", len(r.Kvs), err)
		}
	})

	t.Run("compaction", func(t *testing.T) {
		p, err := c.Put(ctx, "/c/k", "1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/c/k", "2"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Compact(ctx, p.Header.Revision+1); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(ctx, "/c/k", clientv3.WithRev(p.Header.Revision)); err == nil {
			t.Fatal("a read before the compacted revision was answered")
		}
	})

	t.Run("status and members", func(t *testing.T) {
		m, err := c.MemberList(ctx)
		if err != nil || len(m.Members) != 3 {
			t.Fatalf("member list: %v %v", m, err)
		}
		s, err := c.Status(ctx, c.Endpoints()[0])
		if err != nil || s.Leader == 0 {
			t.Fatalf("status: %v %v", s, err)
		}
	})
}
