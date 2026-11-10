package main

// The least leader CPU an operation needs, from first principles: what
// the operation carries (cmd/loadgen's per-operation counts) times what
// each step costs on this runner, timed alone (cmd/ceiling's primitives).
// Nothing of a store is timed. This follows the method a GPT-6 Astra
// second opinion set out on #506 and the model of #577.

// primitives are cmd/ceiling's measurements.
type primitives struct {
	Seal          cost    `json:"tls_seal"`
	Open          cost    `json:"tls_open"`
	Send          cost    `json:"tcp_send"`
	Recv          cost    `json:"tcp_recv"`
	CRC           cost    `json:"crc32c"`
	Copy          cost    `json:"copy"`
	Write         cost    `json:"wal_write"`
	Sync          cost    `json:"fdatasync"`
	SyncLatencyNs float64 `json:"sync_latency_ns"`
	LookupNs      float64 `json:"lookup_ns"`
	InsertNs      float64 `json:"insert_ns"`
	DeleteNs      float64 `json:"delete_ns"`
	MissNs        float64 `json:"miss_ns"`
	LoopbackBps   float64 `json:"loopback_bytes_per_s"`
}

type cost struct {
	CallNs float64 `json:"call_ns"`
	ByteNs float64 `json:"byte_ns"`
}

// The model's assumptions.
const (
	// turnOps is the operations one turn of the leader's event loop
	// handles: one receive and one send per busy socket, one log write
	// and one fdatasync a turn. #577 found 64 a fair batch at the target
	// rates, with 288 to 576 calls in flight.
	turnOps = 64
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
type load struct {
	PerOp     perOp
	Durable   float64 // bytes each node makes durable per operation
	Endpoints float64
	Conns     float64
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
	entries := l.Durable + entryHeader*o.Writes
	// The leader takes its share of the client calls, and the writes the
	// followers forward to it; it sends its share of the answers and
	// events, and every entry to both followers.
	in := o.OutBytes/e + (1-1/e)*entries
	out := o.InBytes/e + peers*entries
	sockets := (l.Conns + peers) / turnOps // receives, and sends, per operation
	reads := o.KVs / e
	return []part{
		{"TLS open", in*p.Open.ByteNs + (sockets+in/recordBytes)*p.Open.CallNs},
		{"TLS seal", out*p.Seal.ByteNs + (sockets+out/recordBytes)*p.Seal.CallNs},
		{"socket receives", in*p.Recv.ByteNs + sockets*p.Recv.CallNs},
		{"socket sends", out*p.Send.ByteNs + sockets*p.Send.CallNs},
		{"log CRC32C", entries * p.CRC.ByteNs},
		{"log write, O_DIRECT", entries*p.Write.ByteNs + p.Write.CallNs/turnOps},
		{"fdatasync", p.Sync.CallNs / turnOps},
		{"values copied in", l.Durable * p.Copy.ByteNs},
		{"index lookups", (reads + o.Writes) * p.LookupNs},
		{"index inserts and deletes", o.Creates*p.InsertNs + o.Deletes*p.DeleteNs},
		{"cold value reads", reads * p.MissNs},
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
	entries := l.Durable + entryHeader*l.PerOp.Writes
	return l.PerOp.OutBytes + l.PerOp.InBytes + (1-1/e)*entries + peers*entries
}
