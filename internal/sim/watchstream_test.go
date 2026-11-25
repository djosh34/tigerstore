package sim

import (
	"bytes"
	"context"
	"net"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// catchUpWatch answers a watch's create request with one response that
// catches it up on events of large values, as wl's catch-up does with up
// to 256 events a turn (#820, seed 1248 of TestScenarios: values of
// 37,555 bytes made responses of 4.9 and 5.1 MB).
type catchUpWatch struct {
	pb.UnimplementedWatchServer
	resp *pb.WatchResponse
}

func (w catchUpWatch) Watch(stream pb.Watch_WatchServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(w.resp); err != nil {
		return err
	}
	// Hold the stream until the client ends it.
	<-stream.Context().Done()
	return stream.Context().Err()
}

// TestACatchUpOverFourMiBArrives is #820: a catch-up response over gRPC's
// 4 MiB default reaches the simulator's watch, as it reaches a clientv3
// watch, which takes any size.
func TestACatchUpOverFourMiBArrives(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	value := bytes.Repeat([]byte{'v'}, 37555)
	resp := &pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 600}}
	for rev := int64(422); rev < 422+140; rev++ {
		resp.Events = append(resp.Events, &mvccpb.Event{Kv: &mvccpb.KeyValue{
			Key: []byte("k"), Value: value, CreateRevision: 2, ModRevision: rev, Version: rev,
		}})
	}
	size := proto.Size(resp)
	if size <= 4<<20 {
		t.Fatalf("the catch-up is %d bytes, not over 4 MiB", size)
	}

	lis := bufconn.Listen(1 << 16)
	srv := grpc.NewServer(grpc.MaxSendMsgSize(1 << 30))
	pb.RegisterWatchServer(srv, catchUpWatch{resp: resp})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///watch",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ws, err := openWLStream(&proc{ctx: ctx, wl: &wlProc{conn: conn}})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("k"), StartRevision: 422},
	}}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-ws.Responses():
		if len(r.Events) != len(resp.Events) || proto.Size(r) != size {
			t.Fatalf("got %d events in %d bytes, want %d in %d", len(r.Events), proto.Size(r), len(resp.Events), size)
		}
	case <-ws.Done():
		t.Fatalf("the stream ended before the catch-up arrived: %v", ws.Err())
	}
}
