//go:build difftest

package difftest

import (
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// TestAutoCompaction runs on a server started with --auto-compaction-mode
// periodic --auto-compaction-retention 2s (scripts/difftest.sh). Where
// the compactor compacts depends on when it noted revisions, so the steps
// record whether the first and the newest revision of the case are
// compacted.
func TestAutoCompaction(t *testing.T) {
	runCases(t, "etcd-v3.7.2-autocompact.json", []testCase{{"periodic", periodicCompaction}})
}

func periodicCompaction(r *run) {
	k := r.key("k")
	first := r.put("put", &pb.PutRequest{Key: k, Value: []byte("0")})
	compacted := func(do string, rev int64) {
		_, err := r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: k, Revision: rev})
		var res any
		if err == nil || errors.Is(err, rpctypes.ErrGRPCCompacted) {
			res, err = struct {
				Compacted bool `json:"compacted"`
			}{err != nil}, nil
		}
		r.record(do, res, err)
	}
	compacted("the first revision at once", first.Header.Revision)
	var last *pb.PutResponse
	// Writes for 4 s, twice the retention.
	for i := range 40 {
		last = r.put("", &pb.PutRequest{Key: k, Value: []byte{byte('a' + i%26)}})
		time.Sleep(100 * time.Millisecond)
	}
	r.steps = r.steps[:len(r.steps)-40]
	compacted("the first revision after 4 s", first.Header.Revision)
	compacted("the newest revision after 4 s", last.Header.Revision)
}
