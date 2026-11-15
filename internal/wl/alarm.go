package wl

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Alarms, as etcd's: a member's NOSPACE or CORRUPT, or any other type,
// raised and cleared through Raft, kept with the member list in
// checkpoints. While NOSPACE is raised, the apply refuses puts, Txns with
// a put and lease grants; while CORRUPT is, every write but an alarm's
// (etcd's applierV3Capped and applierV3Corrupt, the latter checked
// first).

// alarm is one raised alarm.
type alarm struct{ member, typ uint64 }

const (
	alarmNone    = uint64(etcdpb.AlarmType_NONE)
	alarmNoSpace = uint64(etcdpb.AlarmType_NOSPACE)
	alarmCorrupt = uint64(etcdpb.AlarmType_CORRUPT)

	alarmGet        = uint64(etcdpb.AlarmRequest_GET)
	alarmActivate   = uint64(etcdpb.AlarmRequest_ACTIVATE)
	alarmDeactivate = uint64(etcdpb.AlarmRequest_DEACTIVATE)

	codeResourceExhausted = 8
	codeDataLoss          = 15
	msgNoSpace            = "etcdserver: mvcc: database space exceeded"
	msgCorrupt            = "etcdserver: corrupt cluster"
)

// compareAlarms orders alarms by type, then member.
func compareAlarms(a, b alarm) int {
	return cmp.Or(cmp.Compare(a.typ, b.typ), cmp.Compare(a.member, b.member))
}

// String is etcd's AlarmMember's text.
func (a alarm) String() string {
	name, ok := etcdpb.AlarmType_name[int32(a.typ)]
	if !ok {
		name = strconv.FormatUint(a.typ, 10)
	}
	return fmt.Sprintf("memberID:%d alarm:%s", a.member, name)
}

type alarmReq struct{ action, member, typ uint64 }

func parseAlarm(m []byte) (alarmReq, error) {
	var r alarmReq
	for len(m) > 0 {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return r, err
		}
		switch num {
		case 1:
			r.action = v
		case 2:
			r.member = v
		case 3:
			r.typ = v
		}
		m = rest
	}
	return r, nil
}

// alarmRequest answers a GET at once and proposes the rest.
func (n *node) alarmRequest(c *client, s *stream, msg []byte) error {
	r, err := parseAlarm(msg)
	if err != nil {
		n.reject(c, s, badRequest(err))
		return nil
	}
	if r.action != alarmActivate && r.action != alarmDeactivate {
		var list []alarm
		for _, a := range n.alarms {
			if r.typ == alarmNone || a.typ == r.typ {
				list = append(list, a)
			}
		}
		n.replyAlarms(c, s, list)
		return nil
	}
	n.addOp(mkHandle(c, s.id), kAlarm, msg)
	return nil
}

// applyAlarm raises or clears an alarm, and answers with it if that
// changed it or found it raised.
func (n *node) applyAlarm(req []byte, c *client, s *stream) error {
	r, err := parseAlarm(req)
	if err != nil {
		return badRequest(err)
	}
	a := alarm{member: r.member, typ: r.typ}
	i, found := slices.BinarySearchFunc(n.alarms, a, compareAlarms)
	var out []alarm
	switch {
	case r.action == alarmActivate && r.typ != alarmNone:
		if !found {
			n.alarms = slices.Insert(n.alarms, i, a)
		}
		n.alarmGauge(a, 1)
		out = []alarm{a}
	case r.action == alarmDeactivate && found:
		n.alarms = slices.Delete(n.alarms, i, i+1)
		n.alarmGauge(a, -1)
		out = []alarm{a}
	}
	n.alarmsChanged()
	if c != nil {
		n.replyAlarms(c, s, out)
	}
	return nil
}

// alarmsChanged keeps which alarms are raised where the apply and the
// HTTP side read them.
func (n *node) alarmsChanged() {
	n.noSpace, n.corrupt = false, false
	strs := make([]string, 0, len(n.alarms))
	for _, a := range n.alarms {
		n.noSpace = n.noSpace || a.typ == alarmNoSpace
		n.corrupt = n.corrupt || a.typ == alarmCorrupt
		strs = append(strs, a.String())
	}
	if n.metrics != nil {
		n.metrics.alarms.Store(&strs)
	}
}

func (n *node) replyAlarms(c *client, s *stream, list []alarm) {
	var body []byte
	for _, a := range list {
		var m []byte
		m = appendVarintField(m, 1, a.member)
		m = appendVarintField(m, 2, a.typ)
		body = appendBytesField(body, 2, m)
	}
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize())+len(body))
	n.writeHdr(&w)
	w.write(body)
	n.endReply(c, s, &w)
}

// alarmGate refuses a write of kind that a raised alarm forbids.
func (n *node) alarmGate(kind uint8, req []byte) error {
	if n.corrupt {
		switch kind {
		case kPut, kDel, kCompact, kLeaseGrant, kLeaseRevoke:
			return requestErr(codeDataLoss, msgCorrupt)
		case kTxn:
			if txnWrites(req, false) {
				return requestErr(codeDataLoss, msgCorrupt)
			}
		}
	}
	if n.noSpace {
		switch kind {
		case kPut, kLeaseGrant:
			return requestErr(codeResourceExhausted, msgNoSpace)
		case kTxn:
			if txnWrites(req, true) {
				return requestErr(codeResourceExhausted, msgNoSpace)
			}
		}
	}
	return nil
}

// txnWrites reports whether a Txn's branches, nested ones too, put, or
// with deletes also delete: etcd's isTxnPut and !IsTxnReadonly.
func txnWrites(txn []byte, putsOnly bool) bool {
	for m := txn; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return false
		}
		m = rest
		if num != 2 && num != 3 {
			continue
		}
		onum, _, _, op, _, err := field(p)
		if err != nil {
			continue
		}
		switch {
		case onum == 2, onum == 3 && !putsOnly:
			return true
		case onum == 4 && txnWrites(op, putsOnly):
			return true
		}
	}
	return false
}
