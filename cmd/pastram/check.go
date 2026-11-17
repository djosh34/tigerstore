package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/wl"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	etcdwal "go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// checkRecorded runs only after the controller stopped every process. WAL
// recovery can repair a torn uncommitted tail; no server reuses these files.
func checkRecorded(out string) error {
	b, err := os.ReadFile(filepath.Join(out, "recorded.json"))
	if err != nil {
		return err
	}
	var saved struct {
		Base    int64           `json:"base"`
		History history.History `json:"history"`
	}
	if err := json.Unmarshal(b, &saved); err != nil {
		return err
	}
	h := &saved.History
	var longest []history.Request
	for i := range 3 {
		path := filepath.Join(out, fmt.Sprintf("data%d", i))
		var log []history.Request
		var err error
		// etcd keeps its log in member/wal; tigerstore (internal/wl) keeps
		// a member file, and its log in wal.
		if info, statErr := os.Stat(filepath.Join(path, "member", "wal")); statErr == nil && info.IsDir() {
			log, err = etcdProofLog(filepath.Join(path, "member", "wal"))
		} else {
			log, err = proofLog(path)
		}
		if err != nil {
			return fmt.Errorf("node %d WAL: %w", i, err)
		}
		// The log may reach back past the start, to the preload.
		start := slices.IndexFunc(log, func(q history.Request) bool { return q.Put != nil && q.Put.Key == proofPrefix+"start" })
		if start < 0 {
			return fmt.Errorf("node %d WAL lacks the start of the recorded workload", i)
		}
		log = log[start:]
		for j := range min(len(log), len(longest)) {
			if !reflect.DeepEqual(log[j], longest[j]) {
				return fmt.Errorf("node %d committed request %d disagrees", i, j)
			}
		}
		if len(log) > len(longest) {
			longest = log
		}
	}
	h.Log = longest
	if err := shiftRevisions(reflect.ValueOf(h), saved.Base); err != nil {
		return err
	}
	if len(h.Ops) < 100 || len(h.Watches) != 3 || h.Target <= 1 {
		return errors.New("insufficient recorded proof coverage")
	}
	kinds := map[string]int{}
	for _, op := range h.Ops {
		if op.Resp == nil {
			continue
		}
		switch {
		case op.Req.Put != nil:
			kinds["put"]++
		case op.Req.Range != nil:
			kinds["range"]++
		case op.Req.Delete != nil:
			kinds["delete"]++
		case op.Req.Txn != nil:
			kinds["txn"]++
		case op.Req.LeaseGrant != nil:
			kinds["grant"]++
		case op.Req.LeaseRevoke != nil:
			kinds["revoke"]++
		case op.Req.Compact != nil:
			kinds["compact"]++
		}
	}
	for _, kind := range []string{"put", "range", "delete", "txn", "grant", "revoke", "compact"} {
		if kinds[kind] == 0 {
			return fmt.Errorf("no successful %s", kind)
		}
	}
	b, err = json.Marshal(h)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "history.json"), b, 0o644); err != nil {
		return err
	}
	// Porcupine keeps an ambiguous write open to the end of the history;
	// the member kill leaves dozens of them.
	lin, _, err := h.WithoutLostWrites()
	if err != nil {
		return err
	}
	type result struct {
		Name    string
		Seconds float64
		Error   string
		Skipped string `json:",omitempty"`
	}
	var results []result
	var failures error
	// Porcupine gets only a short history (check.ShortEnough); the witness
	// checker runs on every one.
	short := check.ShortEnough(lin)
	for _, c := range []struct {
		name string
		run  func() error
	}{
		{"Linearizable", func() error { return check.Linearizable(lin, 2*time.Minute) }},
		{"Witnessed-log", func() error { return check.Witnessed(h, check.FromLog, 2*time.Minute) }},
		{"Witnessed-revisions", func() error { return check.Witnessed(h, check.FromRevisions, 2*time.Minute) }},
		{"Watches", func() error { return check.Watches(h) }},
		{"Durable", func() error { return check.Durable(h) }},
		{"Revisions", func() error { return check.Revisions(h) }},
	} {
		if c.name == "Linearizable" && !short {
			results = append(results, result{Name: c.name, Skipped: fmt.Sprintf("%d operations, more than the %d Porcupine gets", len(lin.Ops), check.ShortOps)})
			continue
		}
		start := time.Now()
		e := c.run()
		r := result{Name: c.name, Seconds: time.Since(start).Seconds()}
		if e != nil {
			r.Error = e.Error()
			failures = errors.Join(failures, e)
		}
		results = append(results, r)
	}
	b, err = json.MarshalIndent(struct {
		Operations, LogRequests int
		Coverage                map[string]int
		Checks                  []result
	}{len(h.Ops), len(h.Log), kinds, results}, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(out, "checks.json"), b, 0o644)
	}
	return errors.Join(failures, err)
}

// proofLog reads the committed requests of the stopped tigerstore node
// whose data dir is dir, from the log's first entry while no checkpoint
// has cut it (wl.ReadWholeLog): a checkpoint may follow the workload's
// start. Member announcements, auth requests, callers, lease keepalives
// (which change no key or revision, and etcd does not log) and the bulk
// preload are left out.
func proofLog(dir string) ([]history.Request, error) {
	l, err := wl.ReadWholeLog(nil, dir)
	if err != nil {
		return nil, err
	}
	var log []history.Request
	for _, e := range l.Entries {
		if e.GetIndex() > l.Commit || e.GetType() != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		b, err := wl.ParseBatch(e.Data)
		if err != nil {
			return nil, fmt.Errorf("committed index %d: %w", e.GetIndex(), err)
		}
		for _, o := range b.Ops {
			m, err := o.Message()
			if err != nil {
				return nil, fmt.Errorf("committed index %d: %w", e.GetIndex(), err)
			}
			pm, ok := m.(proto.Message)
			switch m := m.(type) {
			case *pb.InternalRaftRequest, *pb.LeaseKeepAliveRequest:
				continue
			case *pb.TxnRequest:
				if bulkPreload(m) {
					continue
				}
			}
			if !ok {
				continue // an announcement or a caller
			}
			q, err := frometcd.Request(pm)
			if err != nil {
				return nil, fmt.Errorf("committed index %d: %w", e.GetIndex(), err)
			}
			log = append(log, q)
		}
	}
	return log, nil
}

func bulkPreload(q *pb.TxnRequest) bool {
	if len(q.Success) != 64 || len(q.Compare) != 0 || len(q.Failure) != 0 {
		return false
	}
	for _, op := range q.Success {
		p := op.GetRequestPut()
		if p == nil || !strings.HasPrefix(string(p.Key), "/registry/") {
			return false
		}
	}
	return true
}

func etcdProofLog(path string) (log []history.Request, err error) {
	w, err := etcdwal.Open(zap.NewNop(), path, &walpb.Snapshot{})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	_, hs, entries, err := w.ReadAll()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.GetIndex() > hs.GetCommit() || e.GetType() != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		var req pb.InternalRaftRequest
		if err := proto.Unmarshal(e.Data, &req); err != nil {
			return nil, err
		}
		var decodeErr error
		req.ProtoReflect().Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			switch string(f.Name()) {
			case "header", "ID", "cluster_version_set", "cluster_member_attr_set", "lease_checkpoint":
				return true
			}
			if f.Kind() != protoreflect.MessageKind {
				decodeErr = fmt.Errorf("unknown etcd field %s", f.Name())
				return false
			}
			m := v.Message().Interface()
			if txn, ok := m.(*pb.TxnRequest); ok && bulkPreload(txn) {
				return true
			}
			q, err := frometcd.Request(m)
			if err != nil {
				decodeErr = err
				return false
			}
			log = append(log, q)
			return true
		})
		if decodeErr != nil {
			return nil, decodeErr
		}
	}
	return log, nil
}

// Shift only named revision fields. Zero keeps its API meaning. The proof
// namespace starts empty, so any positive revision at/below base is invalid.
// Compare numbers are handled separately; lease IDs and versions never shift.
func shiftRevisions(v reflect.Value, base int64) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return shiftRevisions(v.Elem(), base)
	}
	if v.Kind() == reflect.Slice {
		for i := range v.Len() {
			if err := shiftRevisions(v.Index(i), base); err != nil {
				return err
			}
		}
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	if v.Type() == reflect.TypeFor[history.Compare]() {
		c := v.Addr().Interface().(*history.Compare)
		if (c.Target == history.TargetMod || c.Target == history.TargetCreate) && c.Number > 0 {
			c.Number -= base
		}
	}
	for i := range v.NumField() {
		name, field := v.Type().Field(i).Name, v.Field(i)
		if name == "Revision" || name == "CreateRevision" || name == "ModRevision" || name == "StartRevision" || name == "CompactRevision" || name == "Target" && field.Kind() == reflect.Int64 {
			if field.Int() > 0 {
				if field.Int() <= base {
					return fmt.Errorf("%s=%d at/below revision offset %d", name, field.Int(), base)
				}
				field.SetInt(field.Int() - base)
			}
		} else if err := shiftRevisions(field, base); err != nil {
			return err
		}
	}
	return nil
}
