package core

import (
	"cmp"
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"
)

// Alarms follow etcd v3.7.2's AlarmStore and its appliers
// (server/etcdserver/apply): an alarm is a type raised for a member ID,
// replicated through the log. While a NOSPACE alarm is active, puts,
// transactions with a put in either branch and lease grants fail with
// ErrNoSpace; while a CORRUPT alarm is active, every write but keepalives
// and alarms fails with ErrCorrupt, compactions and lease revokes
// included, so leases do not expire either.

// alarm runs an ACTIVATE or DEACTIVATE. Like etcd, it answers with the
// alarm it found or added, or with none.
func (st *State) alarm(r *pb.AlarmRequest) *pb.AlarmResponse {
	resp := &pb.AlarmResponse{Header: header(st.kv.Rev())}
	i, found := slices.BinarySearchFunc(st.alarms, r, compareAlarm)
	switch r.Action {
	case pb.AlarmRequest_ACTIVATE:
		if r.Alarm == pb.AlarmType_NONE {
			break
		}
		if !found {
			st.alarms = slices.Insert(st.alarms, i, &pb.AlarmMember{MemberID: r.MemberID, Alarm: r.Alarm})
			st.alarmsChanged()
		}
		resp.Alarms = []*pb.AlarmMember{st.alarms[i]}
	case pb.AlarmRequest_DEACTIVATE:
		if !found {
			break
		}
		resp.Alarms = []*pb.AlarmMember{st.alarms[i]}
		st.alarms = slices.Delete(st.alarms, i, i+1)
		st.alarmsChanged()
	case pb.AlarmRequest_GET:
		// A list never goes through the log (isAlarmChange).
	}
	return resp
}

// isAlarmChange reports whether an alarm request activates or
// deactivates, which goes through the log.
func isAlarmChange(r *pb.AlarmRequest) bool {
	return r.Action == pb.AlarmRequest_ACTIVATE || r.Action == pb.AlarmRequest_DEACTIVATE
}

// alarmList answers a GET: every active alarm of a type, or of every
// type for NONE. etcd v3.7.2 crashes on an action it does not know; this
// answers it with no alarms.
func (st *State) alarmList(r *pb.AlarmRequest) *pb.AlarmResponse {
	resp := &pb.AlarmResponse{Header: header(st.kv.Rev())}
	if r.Action != pb.AlarmRequest_GET {
		return resp
	}
	for _, a := range st.alarms {
		if r.Alarm == pb.AlarmType_NONE || a.Alarm == r.Alarm {
			resp.Alarms = append(resp.Alarms, proto.CloneOf(a))
		}
	}
	return resp
}

// alarmStrings are the active alarms as etcd's Status lists them in
// Errors.
func (st *State) alarmStrings() []string {
	var out []string
	for _, a := range st.alarms {
		out = append(out, fmt.Sprintf("memberID:%d alarm:%s", a.MemberID, a.Alarm))
	}
	return out
}

func compareAlarm(a *pb.AlarmMember, r *pb.AlarmRequest) int {
	return cmp.Or(cmp.Compare(a.Alarm, r.Alarm), cmp.Compare(a.MemberID, r.MemberID))
}

// alarmsChanged sets noSpace and corrupt from the alarms.
func (st *State) alarmsChanged() {
	st.noSpace, st.corrupt = false, false
	for _, a := range st.alarms {
		st.noSpace = st.noSpace || a.Alarm == pb.AlarmType_NOSPACE
		st.corrupt = st.corrupt || a.Alarm == pb.AlarmType_CORRUPT
	}
}

// refused is the error an active alarm answers a write with: CORRUPT any
// write it covers, NOSPACE one that puts.
func (st *State) refused(puts bool) error {
	switch {
	case st.corrupt:
		return rpctypes.ErrGRPCCorrupt
	case st.noSpace && puts:
		return rpctypes.ErrGRPCNoSpace
	}
	return nil
}

// refusedOp is refused for a write request.
func (st *State) refusedOp(op proto.Message) error {
	if !st.corrupt && !st.noSpace {
		return nil
	}
	switch op := op.(type) {
	case *pb.PutRequest, *pb.LeaseGrantRequest:
		return st.refused(true)
	case *pb.TxnRequest:
		return st.refused(txnPuts(op))
	case *pb.DeleteRangeRequest, *pb.CompactionRequest, *pb.LeaseRevokeRequest, *pb.LeaseCheckpoint:
		return st.refused(false)
	}
	return nil
}

// txnPuts reports whether either branch of a transaction, nested ones
// included, holds a put: etcd's quota counts the larger branch.
func txnPuts(t *pb.TxnRequest) bool {
	for _, ops := range [][]*pb.RequestOp{t.Success, t.Failure} {
		for _, o := range ops {
			if o.GetRequestPut() != nil {
				return true
			}
			if n := o.GetRequestTxn(); n != nil && txnPuts(n) {
				return true
			}
		}
	}
	return false
}
