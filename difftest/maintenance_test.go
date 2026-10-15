//go:build difftest

package difftest

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"errors"
	"io"
	"slices"

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
	// A physical compaction returns once etcd took its hash, which
	// HashKV of that revision then answers.
	r.compact("compact at 4", r.abs(4), true)
	r.hashKV("hashkv at 3 after compacting at 4", r.abs(3), nil)
	r.hashKV("hashkv at 4 after compacting at 4", r.abs(4), &at4)
	r.hashKV("hashkv at 5 after compacting at 4", r.abs(5), nil)
	r.compact("compact at 5", r.abs(5), true)
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

// alarm records a Maintenance.Alarm, with its alarms in type and member
// order: etcd lists them in map order.
func (r *run) alarm(do string, req *pb.AlarmRequest) {
	r.t.Helper()
	resp, err := r.c.maint.Alarm(r.ctx, req)
	var res any
	if err == nil {
		type alarm struct {
			Member uint64 `json:"member"`
			Alarm  string `json:"alarm"`
		}
		var alarms []alarm
		for _, a := range resp.Alarms {
			alarms = append(alarms, alarm{a.MemberID, a.Alarm.String()})
		}
		slices.SortFunc(alarms, func(a, b alarm) int {
			return cmp.Or(cmp.Compare(a.Alarm, b.Alarm), cmp.Compare(a.Member, b.Member))
		})
		res = struct {
			Revision int64   `json:"revision"`
			Alarms   []alarm `json:"alarms,omitempty"`
		}{r.rel(resp.Header.Revision), alarms}
	}
	r.record(do, res, err)
}

// statusErrors records the errors Maintenance.Status lists, sorted.
func (r *run) statusErrors(do string) {
	r.t.Helper()
	resp, err := r.c.maint.Status(r.ctx, &pb.StatusRequest{})
	var res any
	if err == nil {
		res = slices.Sorted(slices.Values(resp.Errors))
	}
	r.record(do, res, err)
}

// alarms activates, lists and deactivates alarms, and checks which
// requests NOSPACE and CORRUPT refuse. It leaves no alarm active.
func alarms(r *run) {
	r.rebase()
	k := r.key("k")
	put := &pb.PutRequest{Key: k, Value: []byte("v")}
	putInFailure := &pb.TxnRequest{Failure: []*pb.RequestOp{putOp(k, "w")}}
	act := func(typ pb.AlarmType, member uint64) *pb.AlarmRequest {
		return &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: typ, MemberID: member}
	}
	deact := func(typ pb.AlarmType, member uint64) *pb.AlarmRequest {
		return &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, Alarm: typ, MemberID: member}
	}
	r.put("put k", put)
	r.alarm("list with no alarm", &pb.AlarmRequest{})
	r.alarm("activate NONE", act(pb.AlarmType_NONE, 7))
	r.alarm("activate NOSPACE for 7", act(pb.AlarmType_NOSPACE, 7))
	r.alarm("activate NOSPACE for 7 again", act(pb.AlarmType_NOSPACE, 7))
	r.alarm("activate NOSPACE for 8", act(pb.AlarmType_NOSPACE, 8))
	r.alarm("activate alarm type 9 for 3", act(9, 3))
	r.alarm("list", &pb.AlarmRequest{})
	r.alarm("list NOSPACE", &pb.AlarmRequest{Alarm: pb.AlarmType_NOSPACE})
	r.alarm("list CORRUPT", &pb.AlarmRequest{Alarm: pb.AlarmType_CORRUPT})
	r.statusErrors("status with alarms")
	r.put("put under NOSPACE", put)
	r.txn("txn with a put in its failure branch under NOSPACE", putInFailure)
	r.txn("txn with a delete under NOSPACE", &pb.TxnRequest{Success: []*pb.RequestOp{deleteOp(r.key("missing"))}})
	r.grant("grant under NOSPACE", 0, 60)
	r.get("range under NOSPACE", &pb.RangeRequest{Key: k})
	r.compact("compact under NOSPACE", r.abs(1), false)
	r.alarm("deactivate NOSPACE for 7", deact(pb.AlarmType_NOSPACE, 7))
	r.alarm("deactivate NOSPACE for 7 again", deact(pb.AlarmType_NOSPACE, 7))
	r.put("put with NOSPACE still active for 8", put)
	r.alarm("deactivate NOSPACE for 8", deact(pb.AlarmType_NOSPACE, 8))
	r.alarm("deactivate alarm type 9 for 3", deact(9, 3))
	r.put("put with no alarm", put)
	r.alarm("activate CORRUPT for 7", act(pb.AlarmType_CORRUPT, 7))
	r.put("put under CORRUPT", put)
	r.del("delete under CORRUPT", &pb.DeleteRangeRequest{Key: k})
	r.txn("txn with a delete under CORRUPT", &pb.TxnRequest{Success: []*pb.RequestOp{deleteOp(k)}})
	r.txn("read-only txn under CORRUPT", &pb.TxnRequest{Success: []*pb.RequestOp{getOp(k)}})
	r.get("range under CORRUPT", &pb.RangeRequest{Key: k})
	r.compact("compact under CORRUPT", r.abs(2), false)
	r.grant("grant under CORRUPT", 0, 60)
	r.revoke("revoke under CORRUPT", 0x7a11)
	r.alarm("deactivate CORRUPT for 7", deact(pb.AlarmType_CORRUPT, 7))
	r.alarm("list after deactivating", &pb.AlarmRequest{})
	r.put("put after deactivating", put)
}

// snapshot records what etcdctl snapshot save checks of a
// Maintenance.Snapshot stream, since the file's format is each server's
// own: the size after a multiple of 512 bytes, whether the last 32 bytes
// are the SHA-256 of the rest, the storage version, and the bytes still
// to come in the last two messages.
func snapshot(r *run) {
	s, err := r.c.maint.Snapshot(r.ctx, &pb.SnapshotRequest{})
	var file []byte
	var versions []string
	var remaining []uint64
	for err == nil {
		var m *pb.SnapshotResponse
		if m, err = s.Recv(); err == nil {
			file = append(file, m.Blob...)
			remaining = append(remaining, m.RemainingBytes)
			if !slices.Contains(versions, m.Version) {
				versions = append(versions, m.Version)
			}
		}
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	var res any
	if err == nil && len(file) >= sha256.Size && len(remaining) >= 2 {
		body := len(file) - sha256.Size
		sum := sha256.Sum256(file[:body])
		res = struct {
			Mod512    int      `json:"mod512"`
			Digest    bool     `json:"digest"`
			Versions  []string `json:"versions"`
			Remaining []uint64 `json:"remaining"`
		}{len(file) % 512, bytes.Equal(sum[:], file[body:]), versions, remaining[len(remaining)-2:]}
	}
	r.record("snapshot", res, err)
}

// defragment records Defragment's answer and whether it has a header,
// and that the data reads back after it.
func defragment(r *run) {
	r.put("put before", &pb.PutRequest{Key: r.key("a"), Value: []byte("1")})
	resp, err := r.c.maint.Defragment(r.ctx, &pb.DefragmentRequest{})
	var res any
	if err == nil {
		res = struct {
			Header bool `json:"header"`
		}{resp.Header != nil}
	}
	r.record("defragment", res, err)
	r.get("get after", &pb.RangeRequest{Key: r.key("a")})
}

// downgrade records a Downgrade's answer: its version, unless version is
// false, and whether it has a header.
func (r *run) downgrade(do string, action pb.DowngradeRequest_DowngradeAction, v string, version bool) {
	r.t.Helper()
	resp, err := r.c.maint.Downgrade(r.ctx, &pb.DowngradeRequest{Action: action, Version: v})
	var res any
	if err == nil {
		out := struct {
			Version string `json:"version,omitempty"`
			Header  bool   `json:"header"`
		}{Header: resp.Header != nil}
		if version {
			out.Version = resp.Version
		}
		res = out
	}
	r.record(do, res, err)
}

// downgradeInfo records the downgrade Status reports.
func (r *run) downgradeInfo(do string) {
	r.t.Helper()
	st, err := r.c.maint.Status(r.ctx, &pb.StatusRequest{})
	var res any
	if err == nil {
		res = struct {
			Enabled bool   `json:"enabled"`
			Target  string `json:"target,omitempty"`
		}{st.DowngradeInfo.GetEnabled(), st.DowngradeInfo.GetTargetVersion()}
	}
	r.record(do, res, err)
}

// downgradeCase checks Downgrade's version formats and targets, one
// downgrade at a time, and CANCEL with and without one. The leader's
// version monitor may lower the cluster version at any moment once a
// downgrade is on, so CANCEL's version is not recorded.
func downgradeCase(r *run) {
	V, E, C := pb.DowngradeRequest_VALIDATE, pb.DowngradeRequest_ENABLE, pb.DowngradeRequest_CANCEL
	for _, v := range []string{"", "x", "v3.6", "3.6.x", "3.5", "3.7", "4.6", "3.6", "3.6.0", "3.6.9"} {
		r.downgrade("validate "+v, V, v, true)
	}
	r.downgrade("cancel without a downgrade", C, "", true)
	r.downgrade("unknown action", 7, "3.6", true)
	r.downgradeInfo("status before")
	r.downgrade("enable 3.7", E, "3.7", true)
	r.downgrade("enable x", E, "x", true)
	r.downgrade("enable 3.6", E, "3.6", true)
	r.downgrade("enable 3.6 again", E, "3.6", true)
	r.downgrade("validate 3.6 during the downgrade", V, "3.6", true)
	r.downgradeInfo("status during the downgrade")
	r.downgrade("cancel", C, "", false)
	r.downgradeInfo("status after the cancel")
	r.downgrade("cancel again", C, "", false)
}
