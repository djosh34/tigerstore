// Question: what exact chunks, revisions and errors does stock etcd 3.7.2
// return on the RangeStream shapes used by Kubernetes 1.37 storage?
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func emit(kind string, data any) {
	b, err := json.Marshal(map[string]any{"kind": kind, "data": data})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	fmt.Println(string(b))
}

func run() error {
	cmd := exec.Command(os.Getenv("STORAGE_TEST_FACTORY"))
	cmd.Stderr = os.Stderr
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() { _ = cmd.Process.Signal(os.Interrupt); _ = cmd.Wait() }()
	r := bufio.NewReader(pipe)
	ep, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	go func() { _, _ = io.Copy(io.Discard, r) }()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{strings.TrimSpace(ep)}, DialTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ready, err := c.Get(ctx, "\x00", clientv3.WithFromKey())
	if err != nil {
		return err
	}
	emit("initial_revision", ready.Header.Revision)
	s, err := c.Status(ctx, strings.TrimSpace(ep))
	if err != nil {
		return err
	}
	emit("status", s)
	kv := pb.NewKVClient(c.ActiveConnection())
	for i := range 25 {
		key := fmt.Sprintf("/probe/%02d", i)
		p, err := c.Put(ctx, key, "small-value")
		if err != nil {
			return err
		}
		emit("put", map[string]any{"key": key, "revision": p.Header.Revision})
	}
	end := []byte(clientv3.GetPrefixRangeEnd("/probe/"))
	base := func() *pb.RangeRequest { return &pb.RangeRequest{Key: []byte("/probe/"), RangeEnd: end} }
	probe := func(name string, req *pb.RangeRequest, want codes.Code) error {
		emit("request", map[string]any{"name": name, "request": req})
		stream, err := kv.RangeStream(ctx, req)
		merged := &pb.RangeResponse{}
		chunks := 0
		if err == nil {
			for {
				resp, recvErr := stream.Recv()
				if recvErr != nil {
					err = recvErr
					break
				}
				wire, marshalErr := proto.Marshal(resp)
				if marshalErr != nil {
					return marshalErr
				}
				file := fmt.Sprintf("%s-%02d.pb", name, chunks)
				if err := os.WriteFile(filepath.Join(os.Getenv("PROBE_EVIDENCE"), file), wire, 0600); err != nil {
					return err
				}
				hash := sha256.Sum256(wire)
				rr := resp.GetRangeResponse()
				keys := []map[string]any{}
				for _, v := range rr.Kvs {
					keys = append(keys, map[string]any{"key": string(v.Key), "value_length": len(v.Value), "create_revision": v.CreateRevision, "mod_revision": v.ModRevision, "version": v.Version, "lease": v.Lease})
				}
				emit("chunk", map[string]any{"name": name, "index": chunks, "header": rr.Header, "count": rr.Count, "more": rr.More, "kvs": keys, "wire_file": file, "wire_bytes": len(wire), "sha256": hex.EncodeToString(hash[:])})
				proto.Merge(merged, rr)
				chunks++
			}
		}
		code := status.Code(err)
		if err == io.EOF {
			code = codes.OK
		}
		emit("terminal", map[string]any{"name": name, "code": code.String(), "error": fmt.Sprint(err), "chunks": chunks, "merged_key_count": len(merged.Kvs), "header": merged.Header, "count": merged.Count, "more": merged.More})
		if code != want {
			return fmt.Errorf("%s returned %s, want %s", name, code, want)
		}
		if code == codes.OK {
			unary, err := kv.Range(ctx, req)
			if err != nil {
				return err
			}
			equal := proto.Equal(merged, unary)
			emit("merge_equals_unary", map[string]any{"name": name, "equal": equal})
			if !equal {
				return fmt.Errorf("%s merged stream differs from unary Range", name)
			}
		}
		return nil
	}
	if err := probe("latest25", base(), codes.OK); err != nil {
		return err
	}
	p, err := c.Put(ctx, "/probe/25", "after-snapshot")
	if err != nil {
		return err
	}
	emit("put", map[string]any{"key": "/probe/25", "revision": p.Header.Revision})
	for _, limit := range []int64{1, 5, 10, 15, 25, 26, 30} {
		q := base()
		q.Limit = limit
		if err := probe(fmt.Sprintf("limit%d", limit), q, codes.OK); err != nil {
			return err
		}
	}
	q := base()
	q.Revision = 26
	if err := probe("pinned26_after_write27", q, codes.OK); err != nil {
		return err
	}
	q = base()
	q.KeysOnly = true
	if err := probe("keys_only", q, codes.OK); err != nil {
		return err
	}
	q = base()
	q.CountOnly = true
	q.Limit = 5
	if err := probe("count_only_limit5", q, codes.OK); err != nil {
		return err
	}
	q = base()
	q.Serializable = true
	if err := probe("serializable", q, codes.OK); err != nil {
		return err
	}
	q = base()
	q.SortOrder = pb.RangeRequest_ASCEND
	if err := probe("key_ascending", q, codes.OK); err != nil {
		return err
	}
	q = base()
	q.SortOrder = pb.RangeRequest_DESCEND
	if err := probe("key_descending", q, codes.Unimplemented); err != nil {
		return err
	}
	q = base()
	q.SortTarget = pb.RangeRequest_VALUE
	q.SortOrder = pb.RangeRequest_ASCEND
	if err := probe("value_ascending", q, codes.Unimplemented); err != nil {
		return err
	}
	q = base()
	q.MinModRevision = 2
	if err := probe("revision_filter", q, codes.Unimplemented); err != nil {
		return err
	}
	q = base()
	q.SortOrder = pb.RangeRequest_SortOrder(99)
	if err := probe("invalid_sort", q, codes.InvalidArgument); err != nil {
		return err
	}
	if err := probe("empty_key", &pb.RangeRequest{}, codes.InvalidArgument); err != nil {
		return err
	}
	if err := probe("missing_prefix", &pb.RangeRequest{Key: []byte("/missing/"), RangeEnd: []byte("/missing0")}, codes.OK); err != nil {
		return err
	}
	q = base()
	q.Revision = 1000
	if err := probe("future_revision", q, codes.OutOfRange); err != nil {
		return err
	}
	q = base()
	q.Revision = -1
	if err := probe("negative_revision", q, codes.OK); err != nil {
		return err
	}
	if _, err := c.Compact(ctx, 26, clientv3.WithCompactPhysical()); err != nil {
		return err
	}
	for _, rev := range []int64{25, 26, 27} {
		q = base()
		q.Revision = rev
		want := codes.OK
		if rev < 26 {
			want = codes.OutOfRange
		}
		if err := probe(fmt.Sprintf("compacted_at26_read%d", rev), q, want); err != nil {
			return err
		}
	}
	// Large values make the first 10-key chunk larger than twice the target.
	// This records the adaptive halving rather than assuming every chunk has 10 keys.
	for i := range 25 {
		if _, err := c.Put(ctx, fmt.Sprintf("/large/%02d", i), strings.Repeat("x", 400*1024)); err != nil {
			return err
		}
	}
	if err := probe("large_values", &pb.RangeRequest{Key: []byte("/large/"), RangeEnd: []byte("/large0")}, codes.OK); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
