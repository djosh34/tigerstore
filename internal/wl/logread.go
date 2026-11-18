package wl

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A stopped node's log, as tools such as the chaos checks read it: its
// entries, and the batches and announcements they hold.

// Log is a stopped node's log: the newest checkpoint's index and term (0
// without one), the entries after it, and the newest commit index the
// node kept.
type Log struct {
	Base, BaseTerm uint64
	Commit         uint64
	Entries        []*pb.Entry
}

// ReadLog reads the log of the stopped node whose data dir is dir, through
// sys in a binary built for the simulator, or nil for the system's
// files. It writes nothing.
func ReadLog(sys Sys, dir string) (Log, error) { return readLog(sys, dir, false) }

// ReadWholeLog is ReadLog with the entries before the newest checkpoint
// too, from the log's first, while no checkpoint has cut the log (Base
// 0 then); a cut log reads as ReadLog reads it. past-ram's checker needs
// the log back to its workload's start, which a checkpoint may follow.
func ReadWholeLog(sys Sys, dir string) (Log, error) { return readLog(sys, dir, true) }

func readLog(sys Sys, dir string, whole bool) (Log, error) {
	env := envOf(sys)
	var l Log
	var from int64
	p, err := readPointer(env, filepath.Join(dir, "ckpt"))
	if err != nil {
		return Log{}, err
	}
	if p != nil {
		if l.Base, l.BaseTerm, err = ckptIndex(env, filepath.Join(dir, "ckpt", p.name)); err != nil {
			return Log{}, err
		}
		from, l.Commit = p.logFrom, max(l.Base, p.hs.GetCommit())
		if whole && p.cut == 0 {
			from, l.Base, l.BaseTerm = 0, 0, 0
		}
	}
	first := l.Base + 1
	_, err = scanWAL(env, filepath.Join(dir, "wal"), from, func(r *walRecord) error {
		switch r.kind {
		case recHardState:
			hs := &pb.HardState{}
			if err := proto.Unmarshal(r.data, hs); err != nil {
				return fmt.Errorf("the log's hard state at %d: %w", r.off, err)
			}
			l.Commit = max(l.Commit, hs.GetCommit())
		case recEntry:
			if r.index < first {
				return nil
			}
			last := first + uint64(len(l.Entries))
			if r.index > last {
				return fmt.Errorf("the log has entry %d after %d", r.index, last-1)
			}
			// A later entry replaces the ones from its index on, as the
			// append that wrote it did.
			l.Entries = append(l.Entries[:r.index-first], &pb.Entry{Index: new(r.index), Term: new(r.term), Type: r.typ.Enum(), Data: append([]byte(nil), r.data...)})
		}
		return nil
	})
	return l, err
}

// ckptIndex reads the index and term of the checkpoint in path.
func ckptIndex(env sysEnv, path string) (index, term uint64, err error) {
	f, err := env.openFile(path, os.O_RDONLY, 0)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	cr := &ckptReader{r: bufio.NewReader(f)}
	magic, err := cr.bytes(uint64(len(ckptMagic)))
	if readFailed(err) {
		return 0, 0, fmt.Errorf("%s: read the checkpoint: %w", path, err)
	}
	if err != nil || string(magic) != string(ckptMagic) {
		return 0, 0, fmt.Errorf("%s: %w: no magic", path, ErrCorruptCheckpoint)
	}
	if index, err = cr.uvarint(); err != nil {
		return 0, 0, err
	}
	term, err = cr.uvarint()
	return index, term, err
}

// Batch is a batch as a log entry holds it: the node that proposed it,
// its sequence there, its format, and its writes.
type Batch struct {
	Origin uint64
	Seq    uint64
	Format uint8
	Ops    []Op
}

// Op is one write of a batch: its kind and its request, as the client
// sent it, but a Txn's failure branch is left out when it only reads.
type Op struct {
	Kind uint8
	Req  []byte
}

// ErrNotBatch is an entry's data that is not a batch.
var ErrNotBatch = errors.New("not a batch")

var errShortBatch = errors.New("a batch shorter than its ops")

// ParseBatch reads a log entry's batch.
func ParseBatch(data []byte) (Batch, error) {
	origin, seq, ok := batchOrigin(data)
	if !ok {
		return Batch{}, ErrNotBatch
	}
	b := Batch{Origin: origin, Seq: seq, Format: data[16]}
	p := batchHdr
	for range binary.LittleEndian.Uint32(data[12:]) {
		if len(data) < p+opHdr {
			return b, errShortBatch
		}
		l := int(binary.LittleEndian.Uint32(data[p+1:]))
		if len(data) < p+opHdr+l {
			return b, errShortBatch
		}
		b.Ops = append(b.Ops, Op{Kind: data[p], Req: data[p+opHdr : p+opHdr+l]})
		p += opHdr + l
	}
	return b, nil
}

// AppendBatch appends a batch of ops to buf.
func AppendBatch(buf []byte, b Batch) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(b.Origin))
	buf = binary.LittleEndian.AppendUint64(buf, b.Seq)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.Ops)))
	buf = append(append(buf, b.Format), magic...)
	for _, o := range b.Ops {
		buf = append(buf, o.Kind)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(o.Req)))
		buf = append(buf, o.Req...)
	}
	return buf
}

// Announcement is a member's announcement (format.go): the newest format
// it supports, in which run, and the epoch it confirms.
type Announcement struct {
	ID     uint64
	Format uint8
	Run    uint64
	Epoch  uint64
}

// ErrUnknownOp is an op of a kind no request message stands for.
var ErrUnknownOp = errors.New("an op of an unknown kind")

// Caller is the user and auth revision of the write after it in its
// batch, while auth is on.
type Caller struct {
	User     string
	Revision uint64
}

// Message returns op as etcd's request message, an InternalRaftRequest
// for an auth request, or as an *Announcement or a *Caller.
func (o Op) Message() (any, error) {
	var m proto.Message
	switch o.Kind {
	case kCaller:
		ai, err := parseCaller(o.Req)
		if err != nil {
			return nil, err
		}
		return &Caller{User: ai.user, Revision: ai.revision}, nil
	case kAuthOp:
		m = &etcdpb.InternalRaftRequest{}
	case kMemberAttrs:
		mem, epoch, err := parseAnnouncement(o.Req)
		if err != nil {
			return nil, err
		}
		return &Announcement{ID: mem.id, Format: mem.format, Run: mem.run, Epoch: epoch}, nil
	case kPut:
		m = &etcdpb.PutRequest{}
	case kDel:
		m = &etcdpb.DeleteRangeRequest{}
	case kTxn:
		m = &etcdpb.TxnRequest{}
	case kCompact:
		m = &etcdpb.CompactionRequest{}
	case kLeaseGrant:
		m = &etcdpb.LeaseGrantRequest{}
	case kLeaseRevoke:
		m = &etcdpb.LeaseRevokeRequest{}
	case kLeaseKeepAlive:
		m = &etcdpb.LeaseKeepAliveRequest{}
	case kAlarm:
		m = &etcdpb.AlarmRequest{}
	case kDowngrade:
		m = &etcdpb.DowngradeRequest{}
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownOp, o.Kind)
	}
	return m, proto.Unmarshal(o.Req, m)
}

// Record is where a record of the log lies: its offset in the log, its
// length with its head, and its kind.
// KindSynced is the Kind of a ring's sync mark, whose Claims says where
// the log is durable before.
const KindSynced = recSynced

type Record struct {
	Off, Len int64
	Kind     byte
	// Claims, for a ring's sync mark, is where it says the log is durable
	// before; 0 for any other record.
	Claims int64
}

// Records are the records of a log that starts at from, up to the first
// that does not check, for the simulator, which damages them at rest.
// seg returns segment k's bytes, nil past the last.
func Records(seg func(k int64) []byte, from int64) []Record {
	var b []byte
	for k := from / segSize; ; k++ {
		s := seg(k)
		if s == nil {
			break
		}
		// A segment shorter than whole is the last; the first's length
		// counts before the part before from is cut off.
		short := int64(len(s)) < segSize
		if k == from/segSize {
			if int64(len(s)) < from%segSize {
				return nil
			}
			s = s[from%segSize:]
		}
		b = append(b, s...)
		if short {
			break
		}
	}
	var recs []Record
	for off := from; checks(b, off); {
		n := recHead + int64(binary.LittleEndian.Uint32(b[4:]))
		r := Record{Off: off, Len: n, Kind: b[8]}
		if r.Kind == recSynced {
			r.Claims = int64(binary.LittleEndian.Uint64(b[recHead:]))
		}
		recs = append(recs, r)
		off, b = off+n, b[n:]
	}
	return recs
}
