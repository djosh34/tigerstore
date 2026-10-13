package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/djosh34/tigerstore/history"
	tigerwal "github.com/djosh34/tigerstore/internal/wal"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type recovered struct {
	commit  uint64
	entries []*raftpb.Entry
	dir     string
}

// nodeEvidence is what one stopped node's WAL holds.
type nodeEvidence struct {
	Dir              string `json:"dir"`
	Commit           uint64 `json:"commit"`
	Last             uint64 `json:"last"`
	FirstUncommitted uint64 `json:"first_uncommitted,omitempty"`
	TailSHA256       string `json:"tail_sha256,omitempty"`
}

// committedLog reads the WALs of the stopped nodes in dirs, checks that
// their committed prefixes agree, and decodes the longest into requests.
// It reads private copies, so recovery repairs never touch the files.
func committedLog(dirs []string) ([]history.Request, []nodeEvidence, error) {
	var nodes []recovered
	var evidence []nodeEvidence
	for _, dir := range dirs {
		n, err := tigerLog(dir)
		if err != nil {
			return nil, evidence, fmt.Errorf("node %s: %w", dir, err)
		}
		if err = validatePrefix(n); err != nil {
			return nil, evidence, fmt.Errorf("node %s: %w", dir, err)
		}
		item := nodeEvidence{Dir: dir, Commit: n.commit, Last: uint64(len(n.entries))}
		if n.commit < uint64(len(n.entries)) {
			tail := n.entries[n.commit]
			item.FirstUncommitted = tail.GetIndex()
			item.TailSHA256 = fmt.Sprintf("%x", sha256.Sum256(tail.Data))
		}
		evidence = append(evidence, item)
		nodes = append(nodes, n)
	}
	canonical, err := committedPrefix(nodes)
	if err != nil {
		return nil, evidence, err
	}
	var log []history.Request
	for _, entry := range canonical {
		if entry.GetType() != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		reqs, err := decodeTigerBatch(entry.Data)
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

func validatePrefix(n recovered) error {
	if n.commit == 0 || n.commit > uint64(len(n.entries)) {
		return fmt.Errorf("missing committed prefix: commit %d, entries %d", n.commit, len(n.entries))
	}
	for i, e := range n.entries {
		if e == nil || e.GetIndex() != uint64(i+1) {
			return fmt.Errorf("missing bootstrap or gap at index %d", i+1)
		}
	}
	return nil
}

func committedPrefix(ns []recovered) ([]*raftpb.Entry, error) {
	if len(ns) == 0 {
		return nil, errors.New("no recovered WALs")
	}
	best := ns[0]
	for _, n := range ns {
		if n.commit > best.commit {
			best = n
		}
	}
	for _, n := range ns {
		for i := uint64(0); i < n.commit; i++ {
			a, b := n.entries[i], best.entries[i]
			if a.GetTerm() != b.GetTerm() || a.GetType() != b.GetType() || !bytes.Equal(a.Data, b.Data) {
				return nil, fmt.Errorf("conflicting committed entries at index %d in %s and %s", i+1, n.dir, best.dir)
			}
		}
	}
	return best.entries[:best.commit], nil
}

// Private byte copies implement the production recovery interface. Repairs
// affect only these buffers, never the preserved source files.
type (
	copyFS   struct{ files map[string]*copyFile }
	copyFile struct{ data []byte }
)

func (f *copyFile) ReadAll() ([]byte, error)    { return slices.Clone(f.data), nil }
func (f *copyFile) Write(p []byte) (int, error) { f.data = append(f.data, p...); return len(p), nil }

func (f *copyFile) Truncate(n int64) error {
	if n < 0 || n > int64(len(f.data)) {
		return errors.New("invalid private truncate")
	}
	f.data = f.data[:n]
	return nil
}
func (*copyFile) Sync() error  { return nil }
func (*copyFile) Close() error { return nil }
func (f *copyFS) List() ([]string, error) {
	var ns []string
	for n := range f.files {
		ns = append(ns, n)
	}
	slices.Sort(ns)
	return ns, nil
}

func (f *copyFS) Create(n string) (tigerwal.File, error) {
	if _, ok := f.files[n]; ok {
		return nil, os.ErrExist
	}
	v := &copyFile{}
	f.files[n] = v
	return v, nil
}

func (f *copyFS) Open(n string) (tigerwal.File, error) {
	v, ok := f.files[n]
	if !ok {
		return nil, os.ErrNotExist
	}
	return v, nil
}

func (f *copyFS) Remove(n string) error {
	if _, ok := f.files[n]; !ok {
		return os.ErrNotExist
	}
	delete(f.files, n)
	return nil
}
func (*copyFS) SyncDir() error { return nil }
func tigerLog(dir string) (recovered, error) {
	path := filepath.Join(dir, "wal")
	names, err := os.ReadDir(path)
	if err != nil {
		return recovered{}, err
	}
	fs := &copyFS{files: map[string]*copyFile{}}
	for _, n := range names {
		if !strings.HasSuffix(n.Name(), ".wal") {
			continue
		}
		b, e := os.ReadFile(filepath.Join(path, n.Name()))
		if e != nil {
			return recovered{}, e
		}
		fs.files[n.Name()] = &copyFile{data: b}
	}
	if len(fs.files) == 0 {
		return recovered{}, errors.New("no WAL segments")
	}
	w, r, err := tigerwal.Open(fs)
	if err != nil {
		return recovered{}, err
	}
	if err = w.Close(); err != nil {
		return recovered{}, err
	}
	return recovered{commit: r.HardState.Commit, entries: r.Entries, dir: dir}, nil
}

// Independent framing, not core.DecodeBatch. Every request and both txn
// branches are preserved; unsupported campaign operations fail closed.
func takeVarint(b *[]byte) (uint64, error) {
	v, n := binary.Uvarint(*b)
	if n <= 0 {
		return 0, errors.New("malformed batch varint")
	}
	*b = (*b)[n:]
	return v, nil
}

func decodeTigerBatch(b []byte) ([]history.Request, error) {
	count, err := takeVarint(&b)
	if err != nil {
		return nil, err
	}
	if count > uint64(len(b)) {
		return nil, errors.New("invalid batch count")
	}
	var out []history.Request
	for range count {
		for range 3 {
			if _, err = takeVarint(&b); err != nil {
				return nil, err
			}
		}
		if len(b) == 0 {
			return nil, errors.New("missing batch kind")
		}
		kind := b[0]
		b = b[1:]
		size, e := takeVarint(&b)
		if e != nil {
			return nil, e
		}
		if size > uint64(len(b)) {
			return nil, errors.New("short batch body")
		}
		var m proto.Message
		switch kind {
		case 1:
			m = &pb.PutRequest{}
		case 2:
			m = &pb.DeleteRangeRequest{}
		case 3:
			m = &pb.TxnRequest{}
		default:
			return nil, fmt.Errorf("unsupported campaign batch kind %d", kind)
		}
		if err = proto.Unmarshal(b[:size], m); err != nil {
			return nil, err
		}
		if err = rejectUnknown(m); err != nil {
			return nil, err
		}
		q, e := request(m)
		if e != nil {
			return nil, e
		}
		if e = campaignRequest(q); e != nil {
			return nil, e
		}
		out = append(out, q)
		b = b[size:]
	}
	if len(b) != 0 {
		return nil, errors.New("trailing batch bytes")
	}
	return out, nil
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
// four checkers still validate the requests, metadata and watch events.
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
