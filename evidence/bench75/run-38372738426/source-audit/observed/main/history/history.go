// Package history is the plain record of a test run that every checker reads.
//
// The simulator and the real-process harness both write it. A History holds
// what clients saw (Ops and Watches) and, separately, the committed log the
// cluster ended with (Log). Checkers never trust the log alone: the client
// observations must agree with it.
//
// Times are nanoseconds from one clock: the simulator's clock, or one
// monotonic clock of the load generator. A server's "response queued" is not
// a client receipt; record times at the client.
//
// Keys and values are Go strings holding raw bytes. A range end follows etcd:
// "" means the single key, "\x00" means every key from Key on, anything else
// is the exclusive end.
package history

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

// AllKeys is the range end that selects every key from the start key on.
const AllKeys = "\x00"

// History is one checked run.
type History struct {
	// Ops are the completed and failed client calls, in any order.
	Ops []Op `json:"ops"`
	// Watches are the client watch streams.
	Watches []Watch `json:"watches,omitempty"`
	// Log is the committed log in commit order: every mutating request the
	// cluster applied, including lease expiry as LeaseRevoke and Compact.
	// Read-only requests may be left out. Checkers that need it say so.
	Log []Request `json:"log,omitempty"`
	// Target is a revision the harness obtained independently after a
	// healthy suffix with mutations stopped. Every watch that was not
	// canceled by the server must have delivered all its events up to it.
	// Zero disables the completeness check.
	Target int64 `json:"target,omitempty"`
}

// Op is one client call.
type Op struct {
	// Client identifies the client. One client never has two calls in
	// flight at the same time.
	Client int `json:"client"`
	// Call is the time the client sent the request.
	Call int64 `json:"call"`
	// Return is the time the client got the response or gave up.
	Return int64 `json:"return"`
	// Req is the request as the client sent it.
	Req Request `json:"req"`
	// Resp is the response. It is nil exactly when Err is set.
	Resp *Response `json:"resp,omitempty"`
	// Err is the error the client got.
	Err *Error `json:"err,omitempty"`
}

// Error is a failed call.
type Error struct {
	// Code is the gRPC status code name, for example "OutOfRange".
	Code string `json:"code"`
	// Message is the status message, for example
	// "etcdserver: mvcc: required revision has been compacted".
	Message string `json:"message"`
	// Ambiguous is true when the request may or may not have taken effect:
	// a timeout, a cancellation, a lost connection or a lost leader. Such a
	// mutation may take effect at any time after Call, even after Return.
	// False means the server rejected the request and it had no effect.
	Ambiguous bool `json:"ambiguous,omitempty"`
}

// Request is one etcd request. Exactly one field is set.
type Request struct {
	Range       *RangeRequest       `json:"range,omitempty"`
	Put         *PutRequest         `json:"put,omitempty"`
	Delete      *DeleteRequest      `json:"delete,omitempty"`
	Txn         *TxnRequest         `json:"txn,omitempty"`
	LeaseGrant  *LeaseGrantRequest  `json:"leaseGrant,omitempty"`
	LeaseRevoke *LeaseRevokeRequest `json:"leaseRevoke,omitempty"`
	Compact     *CompactRequest     `json:"compact,omitempty"`
}

// Response is the response to the Request of the same Op. The field that
// matches the request is set.
type Response struct {
	// Revision is the header revision.
	Revision    int64                `json:"revision"`
	Range       *RangeResponse       `json:"range,omitempty"`
	Put         *PutResponse         `json:"put,omitempty"`
	Delete      *DeleteResponse      `json:"delete,omitempty"`
	Txn         *TxnResponse         `json:"txn,omitempty"`
	LeaseGrant  *LeaseGrantResponse  `json:"leaseGrant,omitempty"`
	LeaseRevoke *LeaseRevokeResponse `json:"leaseRevoke,omitempty"`
	Compact     *CompactResponse     `json:"compact,omitempty"`
}

// KeyValue is a stored key as etcd returns it.
type KeyValue struct {
	Key            string `json:"key"`
	Value          string `json:"value,omitempty"`
	CreateRevision int64  `json:"createRevision"`
	ModRevision    int64  `json:"modRevision"`
	Version        int64  `json:"version"`
	Lease          int64  `json:"lease,omitempty"`
}

// RangeRequest reads keys.
type RangeRequest struct {
	Key string `json:"key"`
	End string `json:"end,omitempty"`
	// Revision is the revision to read at. Zero or less means the current
	// one.
	Revision  int64 `json:"revision,omitempty"`
	Limit     int64 `json:"limit,omitempty"`
	CountOnly bool  `json:"countOnly,omitempty"`
	KeysOnly  bool  `json:"keysOnly,omitempty"`
	// Serializable reads may be served from a stale node. They are not
	// linearizable and only the revision checks look at them.
	Serializable bool `json:"serializable,omitempty"`
}

// RangeResponse is what a range read returned. With CountOnly, KVs is empty.
// With KeysOnly, values are empty.
type RangeResponse struct {
	KVs   []KeyValue `json:"kvs,omitempty"`
	Count int64      `json:"count"`
	More  bool       `json:"more,omitempty"`
}

// PutRequest writes one key.
type PutRequest struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Lease  int64  `json:"lease,omitempty"`
	PrevKV bool   `json:"prevKV,omitempty"`
}

// PutResponse is what a put returned.
type PutResponse struct {
	PrevKV *KeyValue `json:"prevKV,omitempty"`
}

// DeleteRequest deletes one key or a range.
type DeleteRequest struct {
	Key    string `json:"key"`
	End    string `json:"end,omitempty"`
	PrevKV bool   `json:"prevKV,omitempty"`
}

// DeleteResponse is what a delete returned.
type DeleteResponse struct {
	Deleted int64      `json:"deleted"`
	PrevKVs []KeyValue `json:"prevKVs,omitempty"`
}

// Target is the field a Compare reads.
type Target uint8

// Compare targets, as in etcd's Compare.CompareTarget.
const (
	TargetVersion Target = iota + 1
	TargetCreate
	TargetMod
	TargetValue
	TargetLease
)

// Result is the comparison a Compare makes.
type Result uint8

// Compare results, as in etcd's Compare.CompareResult.
const (
	Equal Result = iota + 1
	Greater
	Less
	NotEqual
)

// Compare is one txn condition on a single key. A missing key has version,
// create revision, mod revision and lease 0, and every value compare on it
// fails, as in etcd.
type Compare struct {
	Key    string `json:"key"`
	Target Target `json:"target"`
	Result Result `json:"result"`
	// Number is compared for every target except TargetValue.
	Number int64 `json:"number,omitempty"`
	// Value is compared for TargetValue.
	Value string `json:"value,omitempty"`
}

// TxnOp is one operation of a txn branch. Exactly one field is set. Nested
// txns are not supported.
type TxnOp struct {
	Range  *RangeRequest  `json:"range,omitempty"`
	Put    *PutRequest    `json:"put,omitempty"`
	Delete *DeleteRequest `json:"delete,omitempty"`
}

// TxnRequest is a compare-and-swap.
type TxnRequest struct {
	Compare []Compare `json:"compare,omitempty"`
	Success []TxnOp   `json:"success,omitempty"`
	Failure []TxnOp   `json:"failure,omitempty"`
}

// TxnOpResponse is the result of one TxnOp. The field that matches the
// operation is set.
type TxnOpResponse struct {
	Range  *RangeResponse  `json:"range,omitempty"`
	Put    *PutResponse    `json:"put,omitempty"`
	Delete *DeleteResponse `json:"delete,omitempty"`
}

// TxnResponse is what a txn returned. Results has one entry per operation
// of the branch that ran.
type TxnResponse struct {
	Succeeded bool            `json:"succeeded"`
	Results   []TxnOpResponse `json:"results,omitempty"`
}

// LeaseGrantRequest grants a lease. ID zero lets the server pick it.
type LeaseGrantRequest struct {
	ID  int64 `json:"id,omitempty"`
	TTL int64 `json:"ttl"`
}

// LeaseGrantResponse is the granted lease.
type LeaseGrantResponse struct {
	ID  int64 `json:"id"`
	TTL int64 `json:"ttl"`
}

// LeaseRevokeRequest revokes a lease and deletes its keys. In Log it also
// records a lease expiry.
type LeaseRevokeRequest struct {
	ID int64 `json:"id"`
}

// LeaseRevokeResponse is the response to a revoke.
type LeaseRevokeResponse struct{}

// CompactRequest compacts history below Revision.
type CompactRequest struct {
	Revision int64 `json:"revision"`
	Physical bool  `json:"physical,omitempty"`
}

// CompactResponse is the response to a compaction.
type CompactResponse struct{}

// Watch is one client watch stream as the client saw it, across reconnects
// that the client resumed by itself.
type Watch struct {
	Client int          `json:"client"`
	Req    WatchRequest `json:"req"`
	// Responses are in the order the client received them.
	Responses []WatchResponse `json:"responses"`
}

// WatchRequest opens a watch.
type WatchRequest struct {
	Key string `json:"key"`
	End string `json:"end,omitempty"`
	// StartRevision is the first revision to deliver. Zero means the
	// revision after the current one.
	StartRevision  int64 `json:"startRevision,omitempty"`
	PrevKV         bool  `json:"prevKV,omitempty"`
	ProgressNotify bool  `json:"progressNotify,omitempty"`
}

// WatchResponse is one response on a watch stream.
type WatchResponse struct {
	// Time is when the client received it.
	Time int64 `json:"time"`
	// Revision is the header revision.
	Revision int64   `json:"revision"`
	Events   []Event `json:"events,omitempty"`
	// Progress is true for a progress notification: no events, only a
	// header revision.
	Progress bool `json:"progress,omitempty"`
	Created  bool `json:"created,omitempty"`
	Canceled bool `json:"canceled,omitempty"`
	// CompactRevision is set when the server canceled the watch because its
	// next revision was compacted.
	CompactRevision int64  `json:"compactRevision,omitempty"`
	CancelReason    string `json:"cancelReason,omitempty"`
}

// EventType is the kind of a watch event.
type EventType uint8

// Event types, as in etcd's mvccpb.Event.
const (
	EventPut EventType = iota + 1
	EventDelete
)

// Event is one watch event. For a delete, KV holds the key and the mod
// revision of the delete. PrevKV is the value before the event, if the watch
// asked for it.
type Event struct {
	Type   EventType `json:"type"`
	KV     KeyValue  `json:"kv"`
	PrevKV *KeyValue `json:"prevKV,omitempty"`
}

// Revision is the revision of the event.
func (e Event) Revision() int64 { return e.KV.ModRevision }

// IsCreate reports whether the event creates its key.
func (e Event) IsCreate() bool { return e.Type == EventPut && e.KV.Version == 1 }

// InRange reports whether key is selected by the range [start, end) with
// etcd's meaning of end.
func InRange(key, start, end string) bool {
	switch end {
	case "":
		return key == start
	case AllKeys:
		return key >= start
	default:
		return key >= start && key < end
	}
}

// Mutates reports whether the request can change the store.
func (r Request) Mutates() bool {
	switch {
	case r.Put != nil, r.Delete != nil, r.LeaseGrant != nil, r.LeaseRevoke != nil, r.Compact != nil:
		return true
	case r.Txn != nil:
		for _, op := range slices.Concat(r.Txn.Success, r.Txn.Failure) {
			if op.Range == nil {
				return true
			}
		}
	}
	return false
}

// Validate checks that h is well formed: one field set per request,
// responses that match their requests, and no client with two calls in
// flight. Checkers call it first, so a malformed history fails instead of
// passing by accident.
func (h *History) Validate() error {
	byClient := map[int][]*Op{}
	for i := range h.Ops {
		op := &h.Ops[i]
		if err := op.validate(); err != nil {
			return fmt.Errorf("op %d (client %d): %w", i, op.Client, err)
		}
		byClient[op.Client] = append(byClient[op.Client], op)
	}
	for client, ops := range byClient {
		slices.SortFunc(ops, func(a, b *Op) int { return cmp.Compare(a.Call, b.Call) })
		for i := 1; i < len(ops); i++ {
			if ops[i].Call <= ops[i-1].Return {
				return fmt.Errorf("client %d calls at %d before its previous call returned at %d", client, ops[i].Call, ops[i-1].Return)
			}
		}
	}
	for i, r := range h.Log {
		if err := r.validate(); err != nil {
			return fmt.Errorf("log entry %d: %w", i, err)
		}
	}
	for i := range h.Watches {
		for j, resp := range h.Watches[i].Responses {
			for k, ev := range resp.Events {
				if ev.Type != EventPut && ev.Type != EventDelete {
					return fmt.Errorf("watch %d response %d event %d: unknown event type %d", i, j, k, ev.Type)
				}
			}
		}
	}
	return nil
}

func (op *Op) validate() error {
	if op.Return < op.Call {
		return fmt.Errorf("returns at %d before its call at %d", op.Return, op.Call)
	}
	if err := op.Req.validate(); err != nil {
		return err
	}
	if (op.Resp == nil) == (op.Err == nil) {
		return errors.New("exactly one of Resp and Err must be set")
	}
	if op.Resp == nil {
		return nil
	}
	r, q := op.Resp, op.Req
	ok := (q.Range != nil) == (r.Range != nil) &&
		(q.Put != nil) == (r.Put != nil) &&
		(q.Delete != nil) == (r.Delete != nil) &&
		(q.Txn != nil) == (r.Txn != nil) &&
		(q.LeaseGrant != nil) == (r.LeaseGrant != nil) &&
		(q.LeaseRevoke != nil) == (r.LeaseRevoke != nil) &&
		(q.Compact != nil) == (r.Compact != nil)
	if !ok {
		return errors.New("response type does not match the request")
	}
	if q.Txn != nil {
		branch := q.Txn.Success
		if !r.Txn.Succeeded {
			branch = q.Txn.Failure
		}
		if len(branch) != len(r.Txn.Results) {
			return fmt.Errorf("txn branch has %d operations but %d results", len(branch), len(r.Txn.Results))
		}
		for i, o := range branch {
			res := r.Txn.Results[i]
			if (o.Range != nil) != (res.Range != nil) || (o.Put != nil) != (res.Put != nil) || (o.Delete != nil) != (res.Delete != nil) {
				return fmt.Errorf("txn result %d does not match its operation", i)
			}
		}
	}
	return nil
}

func (r Request) validate() error {
	n := 0
	for _, set := range []bool{r.Range != nil, r.Put != nil, r.Delete != nil, r.Txn != nil, r.LeaseGrant != nil, r.LeaseRevoke != nil, r.Compact != nil} {
		if set {
			n++
		}
	}
	if n != 1 {
		return fmt.Errorf("request has %d fields set, want 1", n)
	}
	if r.Txn == nil {
		return nil
	}
	for _, c := range r.Txn.Compare {
		if c.Target < TargetVersion || c.Target > TargetLease || c.Result < Equal || c.Result > NotEqual {
			return fmt.Errorf("compare on %q has target %d and result %d", c.Key, c.Target, c.Result)
		}
	}
	for _, op := range slices.Concat(r.Txn.Success, r.Txn.Failure) {
		n := 0
		for _, set := range []bool{op.Range != nil, op.Put != nil, op.Delete != nil} {
			if set {
				n++
			}
		}
		if n != 1 {
			return fmt.Errorf("txn operation has %d fields set, want 1", n)
		}
	}
	return nil
}
