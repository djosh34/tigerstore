//go:build difftest

package difftest

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// hashKV records a Maintenance.HashKV: its revisions, and whether its hash
// is the one in same, since the hash itself depends on everything the
// endpoint holds. It returns the hash.
func (r *run) hashKV(do string, rev int64, same *uint32) uint32 {
	r.t.Helper()
	resp, err := r.c.maint.HashKV(r.ctx, &pb.HashKVRequest{Revision: rev})
	var res any
	if err == nil {
		res = struct {
			Revision        int64 `json:"revision"`
			HashRevision    int64 `json:"hashRevision"`
			CompactRevision int64 `json:"compactRevision"`
			Same            bool  `json:"same,omitempty"`
		}{r.rel(resp.Header.Revision), r.rel(resp.HashRevision), r.rel(resp.CompactRevision), same != nil && *same == resp.Hash}
	}
	r.record(do, res, err)
	return resp.GetHash()
}

// hashes checks Maintenance.HashKV and Hash: the revisions they report,
// that a write after the hash revision leaves the hash alone and a
// compaction changes it, and the errors for compacted and future
// revisions.
func hashes(r *run) {
	r.rebase()
	r.put("put a", &pb.PutRequest{Key: r.key("a"), Value: []byte("1")})
	r.put("put b", &pb.PutRequest{Key: r.key("b"), Value: []byte("1")})
	r.put("put a again", &pb.PutRequest{Key: r.key("a"), Value: []byte("2")})
	r.del("delete b", &pb.DeleteRangeRequest{Key: r.key("b")})
	at4 := r.hashKV("hashkv at 4", r.abs(4), nil)
	r.hashKV("hashkv at 4 again", r.abs(4), &at4)
	now := r.hashKV("hashkv at revision 0", 0, &at4)
	r.put("put c", &pb.PutRequest{Key: r.key("c"), Value: []byte("1")})
	r.hashKV("hashkv at 4 after a put", r.abs(4), &at4)
	r.hashKV("hashkv at revision 0 after a put", 0, &now)
	r.hashKV("hashkv at a future revision", r.abs(100), nil)
	r.compact("compact at 4", r.abs(4), false)
	r.hashKV("hashkv at 3 after compacting at 4", r.abs(3), nil)
	r.hashKV("hashkv at 4 after compacting at 4", r.abs(4), &at4)
	r.hashKV("hashkv at 5 after compacting at 4", r.abs(5), nil)
	r.compact("compact at 5", r.abs(5), false)
	r.hashKV("hashkv at 5 after compacting at 5", r.abs(5), nil)
	resp, err := r.c.maint.Hash(r.ctx, &pb.HashRequest{})
	var res any
	if err == nil {
		res = struct {
			Revision int64 `json:"revision"`
		}{r.rel(resp.Header.Revision)}
	}
	r.record("hash", res, err)
}
