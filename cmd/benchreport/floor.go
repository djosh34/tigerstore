package main

// The least leader CPU an operation needs, from first principles: what
// the operation carries (cmd/loadgen's per-operation counts) times what
// each step costs on this runner, timed alone (cmd/ceiling's primitives).
// Nothing of a store is timed. This follows the method a GPT-6 Astra
// second opinion set out on #506 and the model of #577.

// primitives are cmd/ceiling's measurements.
type primitives struct {
	Seal          cost        `json:"tls_seal"`
	Open          cost        `json:"tls_open"`
	Send          cost        `json:"tcp_send"`
	Recv          cost        `json:"tcp_recv"`
	CRC           cost        `json:"crc32c"`
	Copy          cost        `json:"copy"`
	Write         cost        `json:"wal_write"`
	Sync          cost        `json:"fdatasync"`
	SyncLatencyNs float64     `json:"sync_latency_ns"`
	Index         []indexCost `json:"index"`
	Miss          []missCost  `json:"miss"`
	Parse         cost        `json:"proto_parse"`
	FrameNs       float64     `json:"h2_frames_ns"`
	KVNs          float64     `json:"kv_metadata_ns"`
	LoopbackBps   float64     `json:"loopback_bytes_per_s"`
}

type cost struct {
	CallNs float64 `json:"call_ns"`
	ByteNs float64 `json:"byte_ns"`
}

// indexCost is the index at Keys keys: a lookup, an insert of a new key
// and a delete of one it has.
type indexCost struct {
	Keys     float64 `json:"keys"`
	LookupNs float64 `json:"lookup_ns"`
	InsertNs float64 `json:"insert_ns"`
	DeleteNs float64 `json:"delete_ns"`
}

// missCost is a read of a cold value in Bytes of data.
type missCost struct {
	Bytes float64 `json:"bytes"`
	Ns    float64 `json:"ns"`
}

// indexAt is the index of the largest size measured that keys reach, or
// the smallest: a smaller index costs no more, so the floor never
// overstates.
func (p primitives) indexAt(keys float64) indexCost {
	var c indexCost
	for i, x := range p.Index {
		if i == 0 || x.Keys <= keys {
			c = x
		}
	}
	return c
}

// missAt is a cold read in the largest data measured that bytes reach,
// or the smallest.
func (p primitives) missAt(bytes float64) float64 {
	var ns float64
	for i, x := range p.Miss {
		if i == 0 || x.Bytes <= bytes {
			ns = x.Ns
		}
	}
	return ns
}

// The model's assumptions.
const (
	// entryHeader is the bytes a write adds as a log entry and on the
	// wire to a peer, besides its keys and values: a guess.
	entryHeader = 16
	// recordBytes is the most a TLS record holds.
	recordBytes = 16 << 10
	// peers are the leader's sockets to the followers.
	peers = 2
)

// load is what an operation carries, as the floor needs it. Endpoints is
// how many nodes the clients spread their calls over, so the leader takes
// 1/Endpoints of them, of their answers and of the watch events; Conns is
// the client connections of the leader, watch connections included.
// Turn is the operations one turn of the leader's event loop handles,
// with one receive and one send per busy socket, one log write and one
// fdatasync: at most every call in flight, which the floor assumes. Keys
// and Bytes are what the store holds: the keys its index has and the
// bytes of their keys and values.
type load struct {
	PerOp       perOp
	Durable     float64 // bytes each node makes durable per operation
	Endpoints   float64
	Conns       float64
	Turn        float64
	Keys, Bytes float64
}

// part is a share of the floor, in ns per operation.
type part struct {
	Name string
	Ns   float64
}

// floor returns the least leader CPU of one operation, part by part.
func floor(p primitives, l load) []part {
	e := max(l.Endpoints, 1)
	o := l.PerOp
	turn := max(l.Turn, 1)
	entries := l.Durable + entryHeader*o.Entries
	// The leader takes its share of the client calls, and the writes the
	// followers forward to it; it sends its share of the answers and
	// events, and every entry to both followers.
	in := o.OutBytes/e + (1-1/e)*entries
	out := o.InBytes/e + peers*entries
	sockets := (l.Conns + peers) / turn // receives, and sends, per operation
	// The leader reads the key-values of its share of the reads, and the
	// previous ones of its share of the writes; a watch event's are those
	// the write just applied, so they cost only their metadata.
	reads := o.KVs / e
	prevs := o.PrevKVs / e
	// The leader parses its share of the calls and every forwarded write,
	// frames its share of the calls, and encodes the key-values of its
	// share of the answers and events.
	calls := 1/e + (1-1/e)*o.Entries
	idx := p.indexAt(l.Keys)
	return []part{
		{"protobuf and HTTP/2", calls*(p.Parse.CallNs+p.FrameNs) + in*p.Parse.ByteNs + (reads+prevs+o.Events/e)*p.KVNs},
		{"TLS open", in*p.Open.ByteNs + (sockets+in/recordBytes)*p.Open.CallNs},
		{"TLS seal", out*p.Seal.ByteNs + (sockets+out/recordBytes)*p.Seal.CallNs},
		{"socket receives", in*p.Recv.ByteNs + sockets*p.Recv.CallNs},
		{"socket sends", out*p.Send.ByteNs + sockets*p.Send.CallNs},
		{"log CRC32C", entries * p.CRC.ByteNs},
		{"log write, O_DIRECT", entries*p.Write.ByteNs + p.Write.CallNs/turn},
		{"fdatasync", p.Sync.CallNs / turn},
		{"values copied in", l.Durable * p.Copy.ByteNs},
		// Every node applies every write, and with it its compares and
		// lease lookups; the leader reads only its share.
		{"index lookups", (reads + o.Writes + o.Checks + o.Leases) * idx.LookupNs},
		{"index inserts and deletes", o.Creates*idx.InsertNs + o.Deletes*idx.DeleteNs},
		{"cold value reads", (reads + prevs) * p.missAt(l.Bytes)},
	}
}

func sum(parts []part) float64 {
	var ns float64
	for _, p := range parts {
		ns += p.Ns
	}
	return ns
}

// loopbackBytes is what one operation sends over loopback, all nodes
// together: the calls and their answers and events, the writes the
// followers forward, and every entry to both followers.
func loopbackBytes(l load) float64 {
	e := max(l.Endpoints, 1)
	entries := l.Durable + entryHeader*l.PerOp.Entries
	return l.PerOp.OutBytes + l.PerOp.InBytes + (1-1/e)*entries + peers*entries
}
