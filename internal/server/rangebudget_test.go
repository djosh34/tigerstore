package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestRangeReplyClients checks the accounting through real TLS clients,
// the node driver and the socket writer. More pages than fit in memory
// must all finish, including gzip replies on separate connections.
func TestRangeReplyClients(t *testing.T) {
	for _, kind := range []string{"range", "txn", "stream"} {
		t.Run(kind, func(t *testing.T) { testAnswerClients(t, kind) })
	}
}

func answerPage(ctx context.Context, client pb.KVClient, kind string, req *pb.RangeRequest, options ...grpc.CallOption) (*pb.RangeResponse, error) {
	switch kind {
	case "txn":
		r, err := client.Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: req}}}}, options...)
		if err != nil {
			return nil, err
		}
		return r.Responses[0].GetResponseRange(), nil
	case "stream":
		stream, err := client.RangeStream(ctx, req, options...)
		if err != nil {
			return nil, err
		}
		out := &pb.RangeResponse{}
		for {
			r, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return nil, err
			}
			out.Kvs = append(out.Kvs, r.RangeResponse.Kvs...)
			if r.RangeResponse.Header != nil {
				out.Header, out.Count, out.More = r.RangeResponse.Header, r.RangeResponse.Count, r.RangeResponse.More
			}
		}
	default:
		return client.Range(ctx, req, options...)
	}
}

func testAnswerClients(t *testing.T, kind string) {
	c := newClusterWith(t, func(c *cluster) {
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
				r, err := answerPage(ctx, client, kind, &pb.RangeRequest{Key: key, RangeEnd: []byte("range0"), Limit: 1000, Revision: revision}, options...)
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
	// A history response must release its reservation while the watch
	// stream remains open, not only when the client closes it.
	watch, err := pb.NewWatchClient(c.dial(1, cert)).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("range/0000"), RangeEnd: []byte("range/1000"), StartRevision: 1, PrevKv: true}}}); err != nil {
		t.Fatal(err)
	}
	events := 0
	for events < 1000 {
		r, err := watch.Recv()
		if err != nil || r.Canceled {
			t.Fatalf("watch: %v, %v", r, err)
		}
		for _, ev := range r.Events {
			if string(ev.Kv.Key) != fmt.Sprintf("range/%04d", events) || !bytes.Equal(ev.Kv.Value, value) {
				t.Fatalf("wrong watch event %d: %v", events, ev)
			}
			events++
		}
	}
	for {
		m, err := c.nodes[1].Metrics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if m.RangeReplyBytes == 0 {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("completed clients still reserve %d bytes", m.RangeReplyBytes)
		}
	}
}
