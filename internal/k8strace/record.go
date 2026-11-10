// Package k8strace holds what the bench knows of real Kubernetes traffic:
// the log cmd/etcdrec writes while it passes kube-apiserver's etcd calls
// through, and the trace cmd/k8strace builds from that log for loadgen to
// replay.
//
// A log keeps every key, revision, lease ID and size of every message, and
// no value bytes.
package k8strace

// Record is one line of a log, in the order the proxy saw the messages.
// Each gRPC call has an Open record, a Req record for each request
// message, a Resp record for each response message, and an End record.
type Record struct {
	// T is the wall clock in ns since the Unix epoch, taken under the
	// log's lock: a Req is logged before the store sees the request, a
	// Resp before the client sees the response.
	T     int64  `json:"t"`
	Event string `json:"ev"`
	Call  int64  `json:"call"`
	// Open: the client connection, the store node the call went to (1 to
	// 3) and the gRPC method, such as /etcdserverpb.KV/Range.
	Conn   int64  `json:"conn,omitempty"`
	Node   int    `json:"node,omitempty"`
	Method string `json:"method,omitempty"`
	// Req and Resp: the message's protobuf size.
	Bytes int       `json:"bytes,omitempty"`
	Req   *Request  `json:"req,omitempty"`
	Resp  *Response `json:"resp,omitempty"`
	// End: the gRPC status, empty when the call ended well. Req and
	// Resp: why the message could not be decoded.
	Err string `json:"err,omitempty"`
}

// The methods the log decodes. Others keep only their sizes.
const (
	MethodRange          = "/etcdserverpb.KV/Range"
	MethodRangeStream    = "/etcdserverpb.KV/RangeStream"
	MethodPut            = "/etcdserverpb.KV/Put"
	MethodDelete         = "/etcdserverpb.KV/DeleteRange"
	MethodTxn            = "/etcdserverpb.KV/Txn"
	MethodCompact        = "/etcdserverpb.KV/Compact"
	MethodWatch          = "/etcdserverpb.Watch/Watch"
	MethodLeaseGrant     = "/etcdserverpb.Lease/LeaseGrant"
	MethodLeaseRevoke    = "/etcdserverpb.Lease/LeaseRevoke"
	MethodLeaseKeepAlive = "/etcdserverpb.Lease/LeaseKeepAlive"
	MethodLeaseTTL       = "/etcdserverpb.Lease/LeaseTimeToLive"
)

// The events of a Record.
const (
	EventOpen = "open"
	EventReq  = "req"
	EventResp = "resp"
	EventEnd  = "end"
)

// Request is one request message. Exactly one field is set, or none for
// a method the log does not decode.
type Request struct {
	Range      *Range      `json:"range,omitempty"`
	Put        *Put        `json:"put,omitempty"`
	Delete     *Delete     `json:"delete,omitempty"`
	Txn        *Txn        `json:"txn,omitempty"`
	Compact    *Compact    `json:"compact,omitempty"`
	LeaseGrant *LeaseGrant `json:"lease_grant,omitempty"`
	// LeaseRevoke, LeaseKeepAlive and LeaseTimeToLive name the lease.
	Lease *int64        `json:"lease,omitempty"`
	Watch *WatchRequest `json:"watch,omitempty"`
}

// Range is a RangeRequest.
type Range struct {
	Key          string `json:"key"`
	End          string `json:"end,omitempty"`
	Limit        int64  `json:"limit,omitempty"`
	Rev          int64  `json:"rev,omitempty"`
	Serializable bool   `json:"serializable,omitempty"`
	KeysOnly     bool   `json:"keys_only,omitempty"`
	CountOnly    bool   `json:"count_only,omitempty"`
	// Sort is the order and target as etcd numbers them, 0 for none.
	SortOrder  int32 `json:"sort_order,omitempty"`
	SortTarget int32 `json:"sort_target,omitempty"`
	// The filters on revisions, 0 for none.
	MinMod    int64 `json:"min_mod,omitempty"`
	MaxMod    int64 `json:"max_mod,omitempty"`
	MinCreate int64 `json:"min_create,omitempty"`
	MaxCreate int64 `json:"max_create,omitempty"`
}

// Put is a PutRequest. Size is the value's length.
type Put struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	Lease       int64  `json:"lease,omitempty"`
	PrevKV      bool   `json:"prev_kv,omitempty"`
	IgnoreValue bool   `json:"ignore_value,omitempty"`
	IgnoreLease bool   `json:"ignore_lease,omitempty"`
}

// Delete is a DeleteRangeRequest.
type Delete struct {
	Key    string `json:"key"`
	End    string `json:"end,omitempty"`
	PrevKV bool   `json:"prev_kv,omitempty"`
}

// Txn is a TxnRequest.
type Txn struct {
	Compare []LogCompare `json:"compare,omitempty"`
	Success []LogOp      `json:"success,omitempty"`
	Failure []LogOp      `json:"failure,omitempty"`
}

// LogCompare is one compare of a Txn. Target is etcd's CompareTarget: 0
// version, 1 create revision, 2 mod revision, 3 value, 4 lease. Result is
// its CompareResult: 0 equal, 1 greater, 2 less, 3 not equal. Value is the
// number compared against, or the value's length for target 3.
type LogCompare struct {
	Target int32  `json:"target"`
	Result int32  `json:"result"`
	Key    string `json:"key"`
	End    string `json:"end,omitempty"`
	Value  int64  `json:"value"`
}

// The compare targets the replay must map from recorded to replayed
// numbers.
const (
	TargetVersion = 0
	TargetCreate  = 1
	TargetMod     = 2
	TargetValue   = 3
	TargetLease   = 4
)

// LogOp is one RequestOp of a Txn: exactly one field is set.
type LogOp struct {
	Range  *Range  `json:"range,omitempty"`
	Put    *Put    `json:"put,omitempty"`
	Delete *Delete `json:"delete,omitempty"`
	Txn    *Txn    `json:"txn,omitempty"`
}

// Compact is a CompactionRequest.
type Compact struct {
	Rev      int64 `json:"rev"`
	Physical bool  `json:"physical,omitempty"`
}

// LeaseGrant is a LeaseGrantRequest. ID is the one the client asked for,
// 0 to let the store choose.
type LeaseGrant struct {
	TTL int64 `json:"ttl"`
	ID  int64 `json:"id,omitempty"`
}

// WatchRequest is one message on a Watch stream: a create, a cancel or a
// progress request.
type WatchRequest struct {
	Create   *WatchCreate `json:"create,omitempty"`
	Cancel   *int64       `json:"cancel,omitempty"`
	Progress bool         `json:"progress,omitempty"`
}

// WatchCreate is a WatchCreateRequest.
type WatchCreate struct {
	Key      string `json:"key"`
	End      string `json:"end,omitempty"`
	StartRev int64  `json:"start_rev,omitempty"`
	Progress bool   `json:"progress_notify,omitempty"`
	// Filters are etcd's: 0 drops puts, 1 drops deletes.
	Filters  []int32 `json:"filters,omitempty"`
	PrevKV   bool    `json:"prev_kv,omitempty"`
	ID       int64   `json:"id,omitempty"`
	Fragment bool    `json:"fragment,omitempty"`
}

// Response is one response message. Rev is its header's revision.
type Response struct {
	Rev int64 `json:"rev,omitempty"`
	// Range and RangeStream.
	Count int64 `json:"count,omitempty"`
	More  bool  `json:"more,omitempty"`
	KVs   []KV  `json:"kvs,omitempty"`
	// Put and Delete: the previous key-values, if asked; Delete: how many
	// keys it deleted.
	Prev    []KV  `json:"prev,omitempty"`
	Deleted int64 `json:"deleted,omitempty"`
	// Txn.
	Succeeded bool       `json:"succeeded,omitempty"`
	Ops       []Response `json:"ops,omitempty"`
	// Lease calls.
	Lease int64 `json:"lease,omitempty"`
	TTL   int64 `json:"ttl,omitempty"`
	// Watch.
	Watch *WatchResponse `json:"watch,omitempty"`
}

// KV is a key-value without its value: Size is the value's length.
type KV struct {
	Key     string `json:"key"`
	Mod     int64  `json:"mod"`
	Create  int64  `json:"create,omitempty"`
	Version int64  `json:"version,omitempty"`
	Size    int    `json:"size,omitempty"`
	Lease   int64  `json:"lease,omitempty"`
}

// WatchResponse is a WatchResponse.
type WatchResponse struct {
	ID         int64   `json:"id"`
	Created    bool    `json:"created,omitempty"`
	Canceled   bool    `json:"canceled,omitempty"`
	CompactRev int64   `json:"compact_rev,omitempty"`
	Fragment   bool    `json:"fragment,omitempty"`
	Events     []Event `json:"events,omitempty"`
}

// Event is a watch event: Delete is false for a put.
type Event struct {
	Delete bool `json:"delete,omitempty"`
	KV     KV   `json:"kv"`
	Prev   *KV  `json:"prev,omitempty"`
}
