package server

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/expclock"
)

// pre47Options installs the diagnostic hooks even before capture starts, so
// existing connections and RPCs retain their IDs and active counts. Event is
// a no-op outside capture. These hooks still have a cost outside capture;
// compare an uninstrumented binary before attributing a performance change.
func pre47Options() []grpc.ServerOption {
	var opts []grpc.ServerOption
	if os.Getenv("PRE47_TRACE") == "1" {
		opts = append(opts, grpc.StatsHandler(&pre47Stats{}))
	}
	if text := os.Getenv("PRE47_GRPC_WINDOW"); text != "" {
		n, err := strconv.ParseUint(text, 10, 31)
		if err != nil || n < 65535 {
			log.Printf("PRE47_GRPC_WINDOW=%q ignored: expected 65535..2147483647", text)
		} else {
			opts = append(opts, grpc.StaticConnWindowSize(int32(n)), grpc.StaticStreamWindowSize(int32(n)))
		}
	}
	if text := os.Getenv("PRE47_GRPC_STREAMS"); text != "" {
		n, err := strconv.ParseUint(text, 10, 32)
		if err != nil || n == 0 {
			log.Printf("PRE47_GRPC_STREAMS=%q ignored: expected 1..4294967295", text)
		} else {
			opts = append(opts, grpc.MaxConcurrentStreams(uint32(n)))
		}
	}
	return opts
}

type pre47ConnKey struct{}
type pre47RPCKey struct{}

type pre47Conn struct {
	id     uint64
	active atomic.Int64
}

type pre47RPC struct {
	id   uint64
	conn *pre47Conn
}

type pre47Stats struct {
	nextConn atomic.Uint64
	nextRPC  atomic.Uint64
	active   atomic.Int64
}

var _ stats.Handler = (*pre47Stats)(nil)

func (h *pre47Stats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	conn := &pre47Conn{id: h.nextConn.Add(1)}
	expclock.Event("grpc_tag_conn", conn.id, 0, 0, 0)
	return context.WithValue(ctx, pre47ConnKey{}, conn)
}

func (h *pre47Stats) HandleConn(ctx context.Context, s stats.ConnStats) {
	if !expclock.Enabled() {
		return
	}
	conn, ok := ctx.Value(pre47ConnKey{}).(*pre47Conn)
	if !ok {
		return
	}
	switch s.(type) {
	case *stats.ConnBegin:
		expclock.Event("grpc_conn_begin", conn.id, 0, 0, 0)
	case *stats.ConnEnd:
		expclock.Event("grpc_conn_end", conn.id, uint64(conn.active.Load()), 0, 0)
	}
}

func (h *pre47Stats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	conn, _ := ctx.Value(pre47ConnKey{}).(*pre47Conn)
	rpc := &pre47RPC{id: h.nextRPC.Add(1), conn: conn}
	var connID uint64
	if conn != nil {
		connID = conn.id
	}
	// Method IDs distinguish measured Puts from probes and other traffic.
	var method uint64
	switch info.FullMethodName {
	case "/etcdserverpb.KV/Put":
		method = 1
	case "/etcdserverpb.KV/Range":
		method = 2
	case "/etcdserverpb.KV/Txn":
		method = 3
	case "/etcdserverpb.KV/DeleteRange":
		method = 4
	}
	expclock.Event("grpc_tag_rpc", rpc.id, connID, method, 0)
	return context.WithValue(ctx, pre47RPCKey{}, rpc)
}

// RPC event fields are RPC ID, connection ID, c, d. Begin and End use c
// and d for active RPC counts across the server and on this connection.
// Counts change outside capture too, so capture can start mid-cohort.
// Concurrent callbacks can interleave the two atomic count updates.
// Payloads use wire bytes and uncompressed bytes. Headers and trailers use
// wire bytes and zero. OutHeader has no wire size in grpc-go; OutTrailer's
// deprecated WireLength is never set. Both use all-one bits for unknown.
// Event timestamps are callback times, not socket flush times. InHeader
// precedes Begin; OutPayload and OutTrailer only describe queued output.
func (h *pre47Stats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	rpc, ok := ctx.Value(pre47RPCKey{}).(*pre47RPC)
	if !ok {
		return
	}
	var connID uint64
	if rpc.conn != nil {
		connID = rpc.conn.id
	}
	switch s.(type) {
	case *stats.Begin:
		global := h.active.Add(1)
		var local int64
		if rpc.conn != nil {
			local = rpc.conn.active.Add(1)
		}
		expclock.Event("grpc_begin", rpc.id, connID, uint64(global), uint64(local))
		return
	case *stats.End:
		global := h.active.Add(-1)
		var local int64
		if rpc.conn != nil {
			local = rpc.conn.active.Add(-1)
		}
		expclock.Event("grpc_end", rpc.id, connID, uint64(global), uint64(local))
	}
	if !expclock.Enabled() {
		return
	}
	switch s := s.(type) {
	case *stats.InHeader:
		expclock.Event("grpc_in_header", rpc.id, connID, uint64(s.WireLength), 0)
	case *stats.InPayload:
		expclock.Event("grpc_in_payload", rpc.id, connID, uint64(s.WireLength), uint64(s.Length))
	case *stats.OutPayload:
		expclock.Event("grpc_out_payload", rpc.id, connID, uint64(s.WireLength), uint64(s.Length))
	case *stats.OutHeader:
		expclock.Event("grpc_out_header", rpc.id, connID, ^uint64(0), 0)
	case *stats.OutTrailer:
		expclock.Event("grpc_out_trailer", rpc.id, connID, ^uint64(0), 0)
	case *stats.End:
		// Keep duration and status separate from the active-count event.
		expclock.Event("grpc_end_result", rpc.id, connID, uint64(s.EndTime.Sub(s.BeginTime)), uint64(status.Code(s.Error)))
	}
}
