package server

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc/stats"
)

func TestPre47ActiveCounts(t *testing.T) {
	h := &pre47Stats{}
	ctx := h.TagConn(context.Background(), &stats.ConnTagInfo{})
	conn := ctx.Value(pre47ConnKey{}).(*pre47Conn)
	other := h.TagConn(context.Background(), &stats.ConnTagInfo{})
	otherConn := other.Value(pre47ConnKey{}).(*pre47Conn)
	if conn.id == otherConn.id {
		t.Fatal("connection IDs reused")
	}
	const count = 256
	var started, ended sync.WaitGroup
	release := make(chan struct{})
	for range count {
		started.Add(1)
		ended.Go(func() {
			rpcCtx := h.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/etcdserverpb.KV/Put"})
			if rpcCtx.Value(pre47RPCKey{}).(*pre47RPC).conn != conn {
				t.Error("RPC lost its connection tag")
			}
			h.HandleRPC(rpcCtx, &stats.Begin{})
			started.Done()
			<-release
			h.HandleRPC(rpcCtx, &stats.End{})
		})
	}
	started.Wait()
	global, local, idle := h.active.Load(), conn.active.Load(), otherConn.active.Load()
	// Release the goroutines before any failing assertion.
	close(release)
	ended.Wait()
	if global != count || local != count || idle != 0 {
		t.Fatalf("active counts = %d, %d, %d; want %d, %d, 0", global, local, idle, count, count)
	}
	if h.active.Load() != 0 || conn.active.Load() != 0 || h.nextRPC.Load() != count {
		t.Fatal("RPC IDs or final active counts are incorrect")
	}
}

func TestPre47Options(t *testing.T) {
	for _, tc := range []struct {
		window, streams string
		want            int
	}{
		{"", "", 1},
		{"65535", "1", 4},
		{"1048576", "1048576", 4},
		{"2147483647", "4294967295", 4},
		{"65534", "0", 1},
		{"2147483648", "4294967296", 1},
		{"bad", "bad", 1},
	} {
		t.Run(tc.window+"/"+tc.streams, func(t *testing.T) {
			t.Setenv("PRE47_GRPC_WINDOW", tc.window)
			t.Setenv("PRE47_GRPC_STREAMS", tc.streams)
			if got := len(pre47Options()); got != tc.want {
				t.Fatalf("option count = %d, want %d", got, tc.want)
			}
		})
	}
}
