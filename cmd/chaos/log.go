package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/committed"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wl"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// nodeEvidence is what one stopped node's WAL holds.
type nodeEvidence struct {
	Dir              string `json:"dir"`
	Commit           uint64 `json:"commit"`
	Last             uint64 `json:"last"`
	FirstUncommitted uint64 `json:"first_uncommitted,omitempty"`
	TailSHA256       string `json:"tail_sha256,omitempty"`
}

// committedLog reads the logs of the stopped nodes in dirs, checks that
// their committed prefixes agree, and decodes the longest into requests.
func committedLog(dirs []string) ([]history.Request, []nodeEvidence, error) {
	return readCommitted(dirs, committed.Read, 1, decodeBatch)
}

// readCommitted reads the logs of the stopped nodes in dirs with read,
// checks that their committed entries from index first agree, and decodes
// the longest with decode.
func readCommitted(dirs []string, read func(string) (committed.Node, error), first uint64, decode func([]byte) ([]history.Request, error)) ([]history.Request, []nodeEvidence, error) {
	var nodes []committed.Node
	var evidence []nodeEvidence
	for _, dir := range dirs {
		n, err := read(dir)
		if err != nil {
			return nil, evidence, fmt.Errorf("node %s: %w", dir, err)
		}
		if err = n.Validate(); err != nil {
			return nil, evidence, fmt.Errorf("node %s: %w", dir, err)
		}
		item := nodeEvidence{Dir: dir, Commit: n.Commit, Last: n.Last()}
		if n.Commit < n.Last() {
			tail := n.Entries[n.Commit+1-n.First]
			item.FirstUncommitted = tail.GetIndex()
			item.TailSHA256 = fmt.Sprintf("%x", sha256.Sum256(tail.Data))
		}
		evidence = append(evidence, item)
		nodes = append(nodes, n)
	}
	canonical, err := committed.Prefix(nodes, first)
	if err != nil {
		return nil, evidence, err
	}
	var log []history.Request
	for _, entry := range canonical {
		if entry.GetType() != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		reqs, err := decode(entry.Data)
		if err != nil {
			return nil, evidence, fmt.Errorf("committed index %d: %w", entry.GetIndex(), err)
		}
		log = append(log, reqs...)
	}
	if len(log) == 0 {
		return nil, evidence, errors.New("the committed log is empty")
	}
	return log, evidence, nil
}

// decodeBatch decodes the writes of one committed entry of internal/wl.
// Member announcements change no key and are left out; any other request
// the campaign does not send fails.
func decodeBatch(data []byte) ([]history.Request, error) {
	b, err := wl.ParseBatch(data)
	if err != nil {
		return nil, err
	}
	var out []history.Request
	for _, o := range b.Ops {
		m, err := o.Message()
		if err != nil {
			return nil, err
		}
		pm, ok := m.(proto.Message)
		if _, auth := m.(*pb.InternalRaftRequest); !ok || auth {
			continue // an announcement, a caller or an auth request changes no key
		}
		q, err := campaignMessage(pm)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

// decodeMainBatches decodes the writes of main's engine's committed
// entries, which the previous release ran, one entry at a time in the
// log's order. A write the apply refused for the identity it carried
// (core.LogAuth) did not apply, and is left out (#698); so are member
// announcements, lease checkpoints and auth requests, which change no key.
// Any other request the campaign does not send fails.
func decodeMainBatches() func([]byte) ([]history.Request, error) {
	la := core.NewLogAuth()
	return func(data []byte) ([]history.Request, error) { return decodeMainBatch(la, data) }
}

func decodeMainBatch(la *core.LogAuth, data []byte) ([]history.Request, error) {
	var out []history.Request
	err := la.Writes(data, func(m proto.Message) error {
		switch m.(type) {
		case *pb.Member, *pb.LeaseCheckpointRequest, *pb.InternalRaftRequest, *pb.CompactionRequest:
			return nil
		}
		q, err := campaignMessage(m)
		if err != nil {
			return err
		}
		out = append(out, q)
		return nil
	})
	return out, err
}

// campaignMessage is a committed request the campaign sends, as history
// keeps it.
func campaignMessage(m proto.Message) (history.Request, error) {
	if err := rejectUnknown(m); err != nil {
		return history.Request{}, err
	}
	q, err := frometcd.Request(m)
	if err != nil {
		return history.Request{}, err
	}
	return q, campaignRequest(q)
}

func rejectUnknown(m proto.Message) error {
	var err error
	var visit func(protoreflect.Message)
	visit = func(m protoreflect.Message) {
		if len(m.GetUnknown()) != 0 {
			err = errors.New("unknown protobuf fields in committed request")
		}
		m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			if f.IsList() && f.Kind() == protoreflect.MessageKind {
				for i := 0; i < v.List().Len(); i++ {
					visit(v.List().Get(i).Message())
				}
			} else if f.Kind() == protoreflect.MessageKind {
				visit(v.Message())
			}
			return true
		})
	}
	visit(m.ProtoReflect())
	return err
}

func campaignRequest(q history.Request) error {
	if err := utf8Request(reflect.ValueOf(q)); err != nil {
		return err
	}
	checkPut := func(p *history.PutRequest) error {
		if p != nil && p.Lease != 0 {
			return errors.New("lease outside campaign")
		}
		return nil
	}
	if q.Put != nil {
		return checkPut(q.Put)
	}
	if q.Delete != nil {
		return nil
	}
	if q.Txn != nil {
		for _, o := range slices.Concat(q.Txn.Success, q.Txn.Failure) {
			if err := checkPut(o.Put); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("unsupported campaign request")
}

// Tiny independent replay used ONLY for the target fence. The repository's
// checkers of package check still validate the requests, metadata and
// watch events.
func finalRevision(log []history.Request) (int64, error) {
	rev := int64(1)
	keys := map[string]history.KeyValue{}
	for _, q := range log {
		var ops []history.TxnOp
		switch {
		case q.Put != nil:
			ops = []history.TxnOp{{Put: q.Put}}
		case q.Delete != nil:
			ops = []history.TxnOp{{Delete: q.Delete}}
		case q.Txn != nil:
			success := true
			for _, c := range q.Txn.Compare {
				kv := keys[c.Key]
				var n int64
				switch c.Target {
				case history.TargetVersion:
					n = kv.Version
				case history.TargetCreate:
					n = kv.CreateRevision
				case history.TargetMod:
					n = kv.ModRevision
				case history.TargetLease:
					n = kv.Lease
				case history.TargetValue:
					return 0, errors.New("unsupported target-fence compare")
				default:
					return 0, errors.New("unsupported target-fence compare")
				}
				var ok bool
				switch c.Result {
				case history.Equal:
					ok = n == c.Number
				case history.NotEqual:
					ok = n != c.Number
				case history.Less:
					ok = n < c.Number
				case history.Greater:
					ok = n > c.Number
				default:
					return 0, errors.New("unsupported comparison")
				}
				success = success && ok
			}
			ops = q.Txn.Failure
			if success {
				ops = q.Txn.Success
			}
		default:
			return 0, errors.New("unsupported target-fence request")
		}
		changed := false
		for _, o := range ops {
			if o.Put != nil {
				changed = true
			}
			if o.Delete != nil {
				for k := range keys {
					if history.InRange(k, o.Delete.Key, o.Delete.End) {
						changed = true
					}
				}
			}
		}
		if changed {
			rev++
		}
		for _, o := range ops {
			if p := o.Put; p != nil {
				kv := keys[p.Key]
				if kv.Version == 0 {
					kv.Key = p.Key
					kv.CreateRevision = rev
				}
				kv.ModRevision = rev
				kv.Version++
				kv.Value = p.Value
				keys[p.Key] = kv
			}
			if d := o.Delete; d != nil {
				for k := range keys {
					if history.InRange(k, d.Key, d.End) {
						delete(keys, k)
					}
				}
			}
		}
	}
	return rev, nil
}
