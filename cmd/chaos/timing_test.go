package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/chaos/proxy"
)

// A client reconnects after the entire old fault hold would have ended.
// The step must wait for it, then still require a hit during the hold.
func TestNetworkWaitsForClient(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
	})
	p, err := proxy.Listen("127.0.0.1:0", ln.Addr().String(), 224)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := &runner{start: time.Now()}
	l := &link{Name: "client-node1", From: -1, Proxy: p}
	rec := &record{}
	done := make(chan error, 1)
	go func() {
		done <- r.network(ctx, step{Hold: 100 * time.Millisecond}, []linkFault{{l, proxy.Fault{Kind: proxy.Stall, Dir: proxy.Backward}}}, rec)
	}()
	// No client exists yet. Old network returned with zero hits here.
	select {
	case err := <-done:
		t.Fatalf("fault ended before the client reconnected: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	if st := p.Stats(); st.Fault.Kind != proxy.None {
		t.Fatalf("fault installed on an empty client link: %+v", st)
	}
	client, err := net.DialTimeout("tcp", p.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := server.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// Traffic before Set proves readiness; traffic after Set hits the
	// backward stall. No TLS or product process is needed for this race.
	for {
		if _, err := client.Write([]byte("q")); err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		if _, err := io.ReadFull(server, b[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := server.Write(b[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(client, b[:]); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil || !rec.Installed || !rec.Effect || !rec.Healed {
				t.Fatalf("reconnected client step: %+v, %v", rec, err)
			}
			return
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestNetworkReadinessCanceled(t *testing.T) {
	p, err := proxy.Listen("127.0.0.1:0", "127.0.0.1:1", 224)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l := &link{Name: "client-node1", From: -1, Proxy: p}
	r := &runner{start: time.Now()}
	rec := &record{}
	err = r.network(ctx, step{}, []linkFault{{l, proxy.Fault{Kind: proxy.Reset}}}, rec)
	if !errors.Is(err, context.Canceled) || rec.Installed || rec.Effect || p.Stats().Generation != 0 {
		t.Fatalf("unready client changed the fault: %+v, %v, %+v", rec, err, p.Stats())
	}
}

// The small snapshot can install within one log-poll interval. The step
// must hold its replies until SIGKILL, then remove the holds for recovery.
func TestCrashInInstall(t *testing.T) {
	bin := os.Getenv("CHAOS_TIGERSTORE")
	if bin == "" {
		t.Skip("set CHAOS_TIGERSTORE to the workflow's tigerstore build")
	}
	c, err := newCluster(t.TempDir(), bin, 223, [3]string{}, nodeArgs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.closeProxies(); err != nil {
			t.Error(err)
		}
		for _, n := range c.nodes {
			if err := n.log.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	t.Cleanup(func() {
		if err := c.stop(); err != nil {
			t.Error(err)
		}
	})
	for _, n := range c.nodes {
		if err := c.start(n); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := c.ready(ctx, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	cs, err := dialClients(c, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeClients(cs); err != nil {
			t.Error(err)
		}
	}()
	lead, err := c.leader(ctx, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	target := others(lead)[0]
	r := &runner{c: c, start: time.Now(), t: &traffic{
		cs: cs, r: &recorder{origin: time.Now()},
		cfg: workloadConfig{keys: 6, requestTimeout: time.Second},
	}}
	rec := &record{Env: &envRecord{}}
	if err := r.crashInInstall(ctx, lead, target, rec); err != nil {
		t.Fatal(err)
	}
	if !rec.Installed || !rec.Effect || !rec.Healed || rec.Env.Fetching == "" {
		t.Fatalf("snapshot crash missed the fetch: %+v", rec)
	}
	var held bool
	for _, i := range others(target) {
		p := c.peers[target][i].Proxy
		events, _ := p.Events()
		for _, e := range events {
			held = held || e.Action == proxy.Stall && e.Dir == proxy.Backward
		}
		if p.Stats().Fault.Kind != proxy.None {
			t.Fatal("snapshot reply hold survived the kill")
		}
	}
	if !held {
		t.Fatal("snapshot crash did not hold a reply before the kill")
	}
	if err := c.converged(ctx, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	for k := range 6 {
		resp, err := cs[target].Get(ctx, fmt.Sprintf("%sk%d", prefix, k))
		want := fmt.Sprintf("bulk-%d-%s", 294+k, strings.Repeat("b", 1000))
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != want {
			t.Fatalf("recovered k%d differs from the acknowledged bulk write", k)
		}
	}
}
