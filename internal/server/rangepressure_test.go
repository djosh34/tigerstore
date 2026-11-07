package server_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

// TestRangeReplyClients reads pages through real TLS clients, the node
// driver and the socket writer, on a node whose process uses more than its
// memory budget, so its value cache stays empty. Every page must finish,
// gzip replies on separate connections included.
func TestRangeReplyClients(t *testing.T) {
	var mu sync.Mutex
	var trace [4096]string
	var pos int
	var losses []string
	started := time.Now()
	t.Cleanup(func() {
		for _, loss := range losses {
			t.Logf("DIAGNOSTIC LEADER LOSS:\n%s", loss)
		}
	})
	c := newClusterWith(t, func(c *cluster) {
		if os.Getenv("TIGERSTORE_DIAG_539") != "" {
			c.nodeConfig = func(cfg *node.Config) {
				id := cfg.Core.ID
				cfg.Core.Tracef = func(format string, args ...any) {
					line := fmt.Sprintf("%9.3fms node=%d %s", float64(time.Since(started))/float64(time.Millisecond), id, fmt.Sprintf(format, args...))
					mu.Lock()
					trace[pos%len(trace)] = strings.TrimSpace(line)
					pos++
					mu.Unlock()
				}
				var previous uint64
				cfg.Turn = func(n *core.Node, _ core.Effects, _ error) {
					if previous == id && n.Leader() != id {
						mu.Lock()
						var lines []string
						for j := max(0, pos-600); j < pos; j++ {
							lines = append(lines, trace[j%len(trace)])
						}
						losses = append(losses, strings.Join(lines, "\n"))
						mu.Unlock()
					}
					previous = n.Leader()
				}
			}
		}
		c.coreConfig = func(cfg *core.Config) {
			cfg.MemoryBytes = 12 << 20
			cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
		}
	})
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	value := bytes.Repeat([]byte("v"), 1024)
	for batch := range 30 {
		var ops []*pb.RequestOp
		for j := range 100 {
			ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{
				Key: fmt.Appendf(nil, "range/%04d", batch*100+j), Value: value,
			}}})
		}
		retry(t, func(ctx context.Context) (*pb.TxnResponse, error) {
			return kv.Txn(ctx, &pb.TxnRequest{Success: ops})
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		client := c.client(1, cert)
		wg.Go(func() {
			<-start
			var options []grpc.CallOption
			if i%2 == 0 {
				options = append(options, grpc.UseCompressor(gzip.Name))
			}
			key := []byte("range/")
			var revision int64
			for page := range 3 {
				r, err := client.Range(ctx, &pb.RangeRequest{Key: key, RangeEnd: []byte("range0"), Limit: 1000, Revision: revision}, options...)
				if err != nil {
					errs <- fmt.Errorf("client %d page %d: %w", i, page, err)
					return
				}
				if len(r.Kvs) != 1000 || r.Count != int64(3000-page*1000) || r.More != (page < 2) {
					errs <- fmt.Errorf("client %d page %d: keys=%d count=%d more=%v", i, page, len(r.Kvs), r.Count, r.More)
					return
				}
				revision = r.Header.Revision
				for j, kv := range r.Kvs {
					if string(kv.Key) != fmt.Sprintf("range/%04d", page*1000+j) || !bytes.Equal(kv.Value, value) {
						errs <- fmt.Errorf("client %d page %d: wrong key or value at %d", i, page, j)
						return
					}
				}
				key = append(bytes.Clone(r.Kvs[len(r.Kvs)-1].Key), 0)
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
