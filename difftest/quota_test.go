//go:build difftest

package difftest

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestQuota runs on a server started with --quota-backend-bytes 1048576
// (scripts/difftest.sh).
func TestQuota(t *testing.T) {
	runCases(t, "etcd-v3.7.2-quota.json", []testCase{{"quota", quota}})
}

// quota writes values of 100 KiB until the quota refuses one. Where that
// happens depends on how each server counts its data, so the step
// records only the refusal. Then it checks the alarm the server raised
// for itself, what NOSPACE refuses and lets through, and Status.
func quota(r *run) {
	value := bytes.Repeat([]byte("q"), 100<<10)
	var err error
	// etcd sees its database grow only when its backend commits, every
	// 100 ms, so the writes are paced.
	for i := 0; i < 200 && err == nil; i++ {
		_, err = r.c.kv.Put(r.ctx, &pb.PutRequest{Key: r.key(fmt.Sprintf("big/%03d", i)), Value: value})
		time.Sleep(20 * time.Millisecond)
	}
	r.record("put 100 KiB values until one is refused", nil, err)
	r.rebase()
	member := r.ownAlarms("alarms after the refusal", &pb.AlarmRequest{})
	st, err := r.c.maint.Status(r.ctx, &pb.StatusRequest{})
	var res any
	if err == nil {
		res = struct {
			Quota  int64 `json:"quota"`
			Errors int   `json:"errors"`
		}{st.DbSizeQuota, len(st.Errors)}
	}
	r.record("status", res, err)
	k := r.key("small")
	r.put("small put under NOSPACE", &pb.PutRequest{Key: k, Value: []byte("v")})
	r.txn("txn with a put under NOSPACE", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(k, "v")}})
	r.grant("grant under NOSPACE", 0, 60)
	r.del("delete under NOSPACE", &pb.DeleteRangeRequest{Key: r.key("big/000")})
	r.get("get the deleted key under NOSPACE", &pb.RangeRequest{Key: r.key("big/000")})
	r.compact("compact under NOSPACE", r.abs(1), false)
	r.ownAlarms("deactivate the alarm", &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: member})
	r.alarm("list after deactivating", &pb.AlarmRequest{})
}

// ownAlarms records the types of the alarms an alarm request answers,
// and whether the answering member raised each for itself, since member
// IDs differ between servers. It returns the answering member's ID.
func (r *run) ownAlarms(do string, req *pb.AlarmRequest) uint64 {
	r.t.Helper()
	resp, err := r.c.maint.Alarm(r.ctx, req)
	var res any
	if err == nil {
		type alarm struct {
			Alarm string `json:"alarm"`
			Own   bool   `json:"own"`
		}
		var alarms []alarm
		for _, a := range resp.Alarms {
			alarms = append(alarms, alarm{a.Alarm.String(), a.MemberID == resp.Header.MemberId})
		}
		res = alarms
	}
	r.record(do, res, err)
	return resp.GetHeader().GetMemberId()
}
