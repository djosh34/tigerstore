//go:build difftest

package difftest

import (
	"bytes"
	"io"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
)

// plain is a compressor that neither server knows.
type plain struct{}

func (plain) Compress(w io.Writer) (io.WriteCloser, error) { return nopCloser{w}, nil }
func (plain) Decompress(r io.Reader) (io.Reader, error)    { return r, nil }
func (plain) Name() string                                 { return "plain" }

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func init() { encoding.RegisterCompressor(plain{}) }

// compression sends gzip requests on unary and streaming calls and one
// with an encoding the server does not know.
func compression(r *run) {
	k := r.key("g")
	big := bytes.Repeat([]byte("compressible "), 10_000)
	resp, err := r.c.kv.Put(r.ctx, &pb.PutRequest{Key: k, Value: big}, grpc.UseCompressor(gzip.Name))
	var res any
	if err == nil {
		res = r.rel(resp.Header.Revision)
	}
	r.record("gzip put", res, err)
	got, err := r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: k}, grpc.UseCompressor(gzip.Name))
	res = nil
	if err == nil {
		res = len(got.Kvs) == 1 && bytes.Equal(got.Kvs[0].Value, big)
	}
	r.record("gzip range reads the value", res, err)
	_, err = r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: k}, grpc.UseCompressor("plain"))
	r.record("range with an unknown encoding", nil, err)
	w, err := r.c.watch.Watch(r.ctx, grpc.UseCompressor(gzip.Name))
	if err == nil {
		err = w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: k, StartRevision: resp.GetHeader().GetRevision()}}})
	}
	res = nil
	for err == nil {
		var wr *pb.WatchResponse
		if wr, err = w.Recv(); err == nil && len(wr.Events) > 0 {
			res = bytes.Equal(wr.Events[0].Kv.Value, big)
			break
		}
	}
	r.record("gzip watch sends the value", res, err)
}

// fragments watches three values of 1 MiB from the first, once with
// fragment set and once without, and records how the events came:
// "e" per event, and "+" after a response marked as a fragment.
func fragments(r *run) {
	k := r.key("f")
	var first int64
	for i := range 3 {
		p := r.put("put 1 MiB", &pb.PutRequest{Key: k, Value: bytes.Repeat([]byte{byte('a' + i)}, 1<<20)})
		if i == 0 {
			first = p.GetHeader().GetRevision()
		}
	}
	for _, fragment := range []bool{true, false} {
		w, err := r.c.watch.Watch(r.ctx)
		if err == nil {
			err = w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: k, StartRevision: first, Fragment: fragment}}})
		}
		var parts []string
		for events := 0; err == nil && events < 3; {
			var wr *pb.WatchResponse
			if wr, err = w.Recv(); err != nil || len(wr.Events) == 0 {
				continue
			}
			events += len(wr.Events)
			part := strings.Repeat("e", len(wr.Events))
			if wr.Fragment {
				part += "+"
			}
			parts = append(parts, part)
		}
		var res any
		if err == nil {
			res = strings.Join(parts, " ")
		}
		do := "watch without fragment"
		if fragment {
			do = "watch with fragment"
		}
		r.record(do, res, err)
	}
}
