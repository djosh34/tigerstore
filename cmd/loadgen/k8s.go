package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// The k8s workload replays a recording of a real Kubernetes control plane
// (bench/recordings). The seed draws one of the recordings, a cluster
// shape, and where in it each copy begins. The copies make one cluster
// that many times bigger: each copy's objects get a tag of their own
// right after their resource's prefix, as if each copy's namespaces and
// cluster-scoped objects had other names (/registry/pods/c007.ns/name).
// The recorded watches, on whole resources, see every copy's objects, so
// the store has the watchers of one real cluster. A LIST of a whole
// resource reads every copy's objects; a LIST of a namespace reads one
// copy's. Keys outside every watched resource get a prefix per copy.
//
// A call goes out once every reply it waits on has come: the replies its
// apiserver learned the revisions and leases it names from. It names the
// revisions and leases those replies gave in this replay, not the
// recorded numbers. So a copy runs as fast as the store lets it, and
// conflicts on a key happen where they happened in the recording.

// maxBlocked is how many calls of a copy may wait on replies before the
// copy looks no further ahead.
const maxBlocked = 4096

// k8sCopy is one copy of the cluster.
type k8sCopy struct {
	t      *k8strace.Trace
	start  *k8strace.Start
	id     int
	keys   *k8sKeys
	tag    []byte // inserted after a key's resource prefix
	prefix []byte // in front of a key under no watched prefix
	next   int    // the next call to look at
	// revs are the replies' header revisions by call, 0 until it came;
	// while a call waits on replies, minus how many it still waits on.
	// grants are the leases calls granted.
	revs     []int64
	grants   map[int32]int64
	itemRevs []int64 // the preload's revision of each start item
	leaseIDs []int64 // the IDs of the start's leases
	// waiters[i] is the first of the calls that wait on call i's reply,
	// in nodes (index + 1, 0 for none); blocked counts the calls that
	// wait.
	waiters []int32
	nodes   []waiter
	free    int32 // the first free node (index + 1), 0 for none
	blocked int
	ready   []int32
	back    int32 // a call taken from ready that could not start, or -1
}

func newCopy(t *k8strace.Trace, keys *k8sKeys, start *k8strace.Start, id int) *k8sCopy {
	return &k8sCopy{
		t: t, keys: keys, start: start, id: id, tag: fmt.Appendf(nil, "c%03d.", id), prefix: fmt.Appendf(nil, "/c%03d", id),
		next: start.K, revs: make([]int64, len(t.Calls)), grants: map[int32]int64{},
		itemRevs: make([]int64, len(start.Items)), leaseIDs: make([]int64, len(start.Leases)),
		waiters: make([]int32, len(t.Calls)), back: -1,
	}
}

// waiter is a call waiting on a reply, in a list of the reply's waiters.
type waiter struct {
	call, next int32
}

// wait adds call j to the waiters of call w.
func (cp *k8sCopy) wait(w, j int32) {
	n := cp.free
	if n == 0 {
		cp.nodes = append(cp.nodes, waiter{})
		n = int32(len(cp.nodes))
	} else {
		cp.free = cp.nodes[n-1].next
	}
	cp.nodes[n-1] = waiter{call: j, next: cp.waiters[w]}
	cp.waiters[w] = n
}

// k8sKeys says where each key of a trace gets a copy's tag.
type k8sKeys struct {
	// keys are the trace's keys, each with where the tag goes, side by
	// side, so a key costs one cache miss less.
	keys []k8sKey
	// cache marks, by call, the watch cache's calls (Trace.WatchCache).
	cache []bool
}

// k8sKey is a key and where a copy's tag goes in it: after split bytes,
// the key's watched prefix; keepKey for a watched prefix itself or its
// end, which every copy shares; prefixKey for a key under no watched
// prefix, which gets the copy's prefix in front. A key of up to
// inlineKey bytes, as every recorded key is, is held in the entry
// itself: a key's bytes elsewhere cost a cache miss more each time a
// request carries it.
type k8sKey struct {
	name   string
	split  int32
	n      int32 // bytes in inline, 0 if the key is longer
	inline [inlineKey]byte
}

// inlineKey makes an entry 128 bytes.
const inlineKey = 104

// text returns the key's bytes.
func (k *k8sKey) text() []byte {
	if k.n > 0 {
		return k.inline[:k.n]
	}
	return []byte(k.name)
}

const (
	keepKey   = -1
	prefixKey = -2
)

// newK8sKeys finds each key's watched prefix: the longest key of a range
// watch that the key starts with.
func newK8sKeys(t *k8strace.Trace) *k8sKeys {
	keep := map[string]bool{"\x00": true}
	var prefixes []string
	for _, ws := range t.Watches {
		for _, w := range ws.Creates {
			if w.End >= 0 {
				p := t.Keys[w.Key]
				prefixes = append(prefixes, p)
				keep[p] = true
				keep[string(prefixEnd([]byte(p)))] = true
			}
		}
	}
	k := &k8sKeys{keys: make([]k8sKey, len(t.Keys)), cache: t.WatchCache()}
	for i, name := range t.Keys {
		k.keys[i] = k8sKey{name: name, split: prefixKey}
		if len(name) <= inlineKey {
			k.keys[i].n = int32(copy(k.keys[i].inline[:], name))
		}
		if keep[name] {
			k.keys[i].split = keepKey
			continue
		}
		for _, p := range prefixes {
			if strings.HasPrefix(name, p) && int32(len(p)) > k.keys[i].split {
				k.keys[i].split = int32(len(p))
			}
		}
	}
	return k
}

// leftToCopy0 reports whether copy cp leaves call i to copy 0: whole-resource
// reads and progress requests come from kube-apiserver's watch cache, one
// per resource, which a bigger cluster still has only one of.
func (cp *k8sCopy) leftToCopy0(i int32) bool {
	return cp.id != 0 && cp.keys.cache[i]
}

// keyOf returns key k of copy cp.
func (cp *k8sCopy) keyOf(k int32) []byte {
	key := &cp.keys.keys[k]
	name := key.text()
	switch at := key.split; at {
	case keepKey:
		return append([]byte(nil), name...)
	case prefixKey:
		return append(append([]byte(nil), cp.prefix...), name...)
	default:
		return append(append(append([]byte(nil), name[:at]...), cp.tag...), name[at:]...)
	}
}

// answered(w) reports whether call w's reply has come, or w is before
// the copy's start.
func (cp *k8sCopy) answered(w int32) bool { return int(w) < cp.start.K || cp.revs[w] > 0 }

// nextReady returns a call whose waits are over.
func (cp *k8sCopy) nextReady() (int32, bool) {
	if cp.back >= 0 {
		i := cp.back
		cp.back = -1
		return i, true
	}
	for len(cp.ready) == 0 && cp.next < len(cp.t.Calls) && cp.blocked < maxBlocked {
		i := int32(cp.next)
		cp.next++
		var n int32
		for _, w := range cp.t.Calls[i].Waits {
			if !cp.answered(w) {
				cp.wait(w, i)
				n++
			}
		}
		if n == 0 {
			cp.ready = append(cp.ready, i)
		} else {
			cp.revs[i] = -int64(n)
			cp.blocked++
		}
	}
	if len(cp.ready) == 0 {
		return 0, false
	}
	i := cp.ready[0]
	cp.ready = cp.ready[1:]
	return i, true
}

// reply records call i's reply and frees the calls that waited on it.
func (cp *k8sCopy) reply(i int32, rev int64) {
	cp.revs[i] = max(rev, 1)
	for n := cp.waiters[i]; n != 0; {
		node := &cp.nodes[n-1]
		j, next := node.call, node.next
		if cp.revs[j]++; cp.revs[j] == 0 {
			cp.blocked--
			cp.ready = append(cp.ready, j)
		}
		node.next, cp.free = cp.free, n
		n = next
	}
	cp.waiters[i] = 0
}

// finished reports whether the copy has sent the last call of the trace.
func (cp *k8sCopy) finished() bool {
	return cp.next == len(cp.t.Calls) && len(cp.ready) == 0 && cp.blocked == 0 && cp.back < 0
}

// rev is the replay's revision for a revision of call i: of key's write
// for a compare, key -1 otherwise. A compare against a write the start
// overwrote, or one the recording learned before its window, gets 1,
// which no live key has; a read at a revision from before the start
// reads the newest.
func (cp *k8sCopy) rev(r k8strace.Rev, key int32) int64 {
	switch {
	case r == 0:
		return 0
	case r == k8strace.RevStale:
		return 1
	case !cp.start.Before(r):
		return cp.revs[r-1]
	case key < 0:
		return 0
	}
	if it, ok := cp.start.Item(key, r); ok {
		return cp.itemRevs[it]
	}
	return 1
}

// behindRev is the revision a call that the copies' compactions passed
// (behindCompaction) replies to the calls that name it: 1 for a read, as
// for any revision the store no longer has, and for a compaction the
// revision it compacted to, which the store had then, as kube-apiserver
// keeps its own revision.
func (c *k8sCall) behindRev(behind uint8) int64 {
	if behind == k8strace.CallCompact {
		return c.cp.compactRev(c.i)
	}
	return 1
}

// compactRev is the replay's revision that compaction call i compacts to.
func (cp *k8sCopy) compactRev(i int32) int64 { return cp.rev(cp.t.Calls[i].Rev, -1) }

// compactedMessage is the status message of etcd's answer to a read or a
// compaction at a compacted revision.
var compactedMessage = rpctypes.ErrorDesc(rpctypes.ErrGRPCCompacted)

// lease is the replay's ID of a lease, 0 if it is gone.
func (cp *k8sCopy) lease(l k8strace.LeaseRef) int64 {
	switch {
	case l == 0:
		return 0
	case l > 0 && int(l-1) >= cp.start.K:
		return cp.grants[int32(l-1)]
	}
	if i, ok := cp.start.Lease(l); ok {
		return cp.leaseIDs[i]
	}
	return 0
}

// k8sLoad replays the copies of one connection.
type k8sLoad struct {
	wk      *worker
	copies  []*k8sCopy
	watches []*k8sWatch // shared by every copy
	rr      int
	enc     encoder
	buf     []byte
	// compacted counts reads at a revision that another copy's
	// compaction removed first; kube-apiserver lists again then.
	// compactsBehind counts compactions to a revision another copy's
	// compaction had passed, which kube-apiserver's compactor logs and
	// tries again at its next interval.
	compacted, compactsBehind int64
	// skipped counts lease calls whose lease the start no longer has.
	skipped int64
	// cacheReads counts the watch cache's calls, reads of a whole watched
	// resource and progress requests, that copies other than 0 left to
	// copy 0 (leftToCopy0).
	cacheReads int64
}

// k8sCall is the call of the trace a stream carries, and what it writes
// if its Txn's compares fail. It keeps the call's kind, and whether it
// reads at a revision: by the answer, the trace's call has long left the
// cache, and reading it again cost 5% of the generator's core.
type k8sCall struct {
	cp     *k8sCopy
	i      int32
	kind   uint8
	atRev  bool
	failed tally
}

// wrote sets what st writes, as it is known so far.
func (w *k8sLoad) wrote(st *stream, t *tally) {
	st.written = t.bytes
	st.c.Writes, st.c.Creates, st.c.Deletes = t.writes, t.creates, t.deletes
	st.c.Entries, st.c.Checks, st.c.Leases = t.entries, t.checks, t.leases
}

func (w *k8sLoad) start(c *conn, st *stream, limit int) bool {
	for range len(w.copies) {
		cp := w.copies[w.rr]
		w.rr = (w.rr + 1) % len(w.copies)
		for {
			i, ok := cp.nextReady()
			if !ok {
				break
			}
			call := &cp.t.Calls[i]
			if cp.leftToCopy0(i) {
				w.cacheReads++
				cp.reply(i, 1)
				continue
			}
			if call.Kind == k8strace.CallProgress {
				w.progress(cp, i)
				continue
			}
			if (call.Kind == k8strace.CallLeaseRevoke || call.Kind == k8strace.CallLeaseKeepAlive || call.Kind == k8strace.CallLeaseTTL) && cp.lease(call.Lease) == 0 {
				w.skipped++
				cp.reply(i, 1)
				continue
			}
			w.enc.cp, w.enc.call = cp, i
			w.buf = w.enc.request(w.buf[:0], call)
			if !c.canStart(messageLen(w.buf), limit) {
				cp.back = i
				return false
			}
			st.many = call.Kind == k8strace.CallRangeStream
			c.startMessage(st, paths[call.Kind], w.buf)
			st.k8s = k8sCall{cp: cp, i: i, kind: call.Kind, atRev: call.Op.Rev != 0, failed: w.enc.failed}
			w.wrote(st, &w.enc.ok)
			return true
		}
	}
	return false
}

// progress sends a progress request on the recorded watch stream of call
// i, which every copy shares. Its answer, a progress notification on that
// stream, counts as an operation there. No call waits on it.
func (w *k8sLoad) progress(cp *k8sCopy, i int32) {
	w.watches[cp.t.Calls[i].Stream].c.post(w.watches[cp.t.Calls[i].Stream])
	cp.reply(i, 1)
}

// progressRequest is a WatchRequest with an empty progress_request.
var progressRequest = []byte{watchProgressReq<<3 | 2, 0}

// post asks c's writer to send a progress request on the watch ws, which
// streams on c. A worker that holds another connection's lock may call
// it: it takes c's mailbox lock, never c.mu, so no two workers wait on
// each other's connection.
func (c *conn) post(ws *k8sWatch) {
	c.mail.Lock()
	c.posted = append(c.posted, ws)
	c.mail.Unlock()
	c.signal()
}

// sendPosted writes the progress requests posted to c. Its writer calls
// it with c.mu held.
func (c *conn) sendPosted() {
	c.mail.Lock()
	posted := c.posted
	c.posted = c.spare[:0]
	c.mail.Unlock()
	at := now()
	for _, ws := range posted {
		if c.sendWin < int64(messageLen(progressRequest)) {
			// The window is closed: the request waits, and the
			// WINDOW_UPDATE that opens it wakes the writer.
			c.mail.Lock()
			c.posted = append(c.posted, ws)
			c.mail.Unlock()
			continue
		}
		c.out = c.appendMessage(c.out, ws.st, progressRequest, false)
		ws.pending = append(ws.pending, at)
	}
	c.spare = posted
}

func (w *k8sLoad) exhausted() bool {
	for _, cp := range w.copies {
		if !cp.finished() {
			return false
		}
	}
	return true
}

func (w *k8sLoad) message(st *stream, m []byte) error {
	call := &st.k8s
	switch call.kind {
	case k8strace.CallRange:
		return rangeReply(st, m, true)
	case k8strace.CallRangeStream:
		r, err := bytesField(m, rangeStreamResponse)
		if err != nil {
			return err
		}
		// Only the last chunk has the header.
		return rangeReply(st, r, false)
	case k8strace.CallTxn:
		rev, ok, err := txnOutcome(m)
		if err != nil {
			return err
		}
		st.rev = rev
		if !ok {
			w.wrote(st, &call.failed)
		}
		return txnKVs(st, m)
	case k8strace.CallLeaseGrant:
		id, err := varintField(m, leaseID)
		if err != nil {
			return err
		}
		call.cp.grants[call.i] = int64(id)
	}
	rev, ok := headerRev(m)
	if !ok {
		return errors.New("response has no header revision")
	}
	st.rev = rev
	return nil
}

func (w *k8sLoad) done(st *stream, at int64, err error) {
	call := &st.k8s
	if behind := behindCompaction(call.kind, call.atRev, err); behind != k8strace.CallKinds {
		if behind == k8strace.CallCompact {
			w.compactsBehind++
		} else {
			w.compacted++
		}
		err, st.msgs, st.rev = nil, 1, call.behindRev(behind)
	}
	if err == nil {
		err = messages(st)
	}
	if err != nil {
		// The result's errors name the kind of call that failed.
		err = fmt.Errorf("%s: %w", k8strace.KindNames[call.kind], err)
	} else {
		call.cp.reply(call.i, st.rev)
	}
	w.wk.stats.complete(st, at, err)
}

// behindCompaction reports, for a call of kind that failed with err,
// whether the copies' compactions passed it, as kube-apiserver meets that:
// CallRange for a read at a revision another copy's compaction removed,
// which it lists again; CallCompact for a compaction to a revision another
// copy's compaction passed, which its compactor tries again later. The 64
// copies compact one store, so under load their compactions land out of
// order. Only etcd's "compacted" answer counts: a future revision has
// the same code and is a store's bug. Anything else gives CallKinds.
func behindCompaction(kind uint8, atRev bool, err error) uint8 {
	var status *grpcStatus
	if err == nil || !errors.As(err, &status) || status.code != codeOutOfRange || status.message != compactedMessage {
		return k8strace.CallKinds
	}
	switch {
	case (kind == k8strace.CallRange || kind == k8strace.CallRangeStream) && atRev:
		return k8strace.CallRange
	case kind == k8strace.CallCompact:
		return k8strace.CallCompact
	}
	return k8strace.CallKinds
}

// rangeReply takes in a RangeResponse in one pass: the key-values it
// carried, and its header's revision, which it must have if needRev.
func rangeReply(st *stream, m []byte, needRev bool) error {
	err := fields(m, func(num protowire.Number, _ uint64, b []byte) error {
		//exhaustive:ignore // count and more are not needed
		switch num {
		case respHeader:
			rev, err := varintField(b, headerRevision)
			st.rev = int64(rev)
			return err
		case rangeKVs:
			st.c.KVs++
		}
		return nil
	})
	if err == nil && needRev && st.rev == 0 {
		err = errors.New("response has no header revision")
	}
	return err
}

// The fields of the answers inside a TxnResponse (etcd v3.7.2 rpc.proto).
const (
	opResponseRange = 1 // ResponseOp
	opResponseTxn   = 4
	putRespPrevKV   = 2 // PutResponse
	delPrevKVs      = 3 // DeleteRangeResponse
)

// txnKVs counts the key-values a TxnResponse carries, those its ranges
// read and the previous ones of its writes, nested Txns included.
func txnKVs(st *stream, m []byte) error {
	// countField counts the fields num of r into n.
	countField := func(r []byte, num protowire.Number, n *int64) error {
		return fields(r, func(f protowire.Number, _ uint64, _ []byte) error {
			if f == num {
				*n++
			}
			return nil
		})
	}
	return fields(m, func(num protowire.Number, _ uint64, op []byte) error {
		if num != txnResponses {
			return nil
		}
		return fields(op, func(num protowire.Number, _ uint64, r []byte) error {
			//exhaustive:ignore // a ResponseOp has no other answers
			switch num {
			case opResponseRange:
				return countField(r, rangeKVs, &st.c.KVs)
			case opResponsePut:
				return countField(r, putRespPrevKV, &st.c.PrevKVs)
			case opResponseDeleteRange:
				return countField(r, delPrevKVs, &st.c.PrevKVs)
			case opResponseTxn:
				return txnKVs(st, r)
			}
			return nil
		})
	})
}

// headerRev returns header.revision of a response, if it has a header.
func headerRev(m []byte) (int64, bool) {
	h, err := bytesField(m, respHeader)
	if err != nil || h == nil {
		return 0, false
	}
	rev, err := varintField(h, headerRevision)
	return int64(rev), err == nil && rev != 0
}

// k8sWatch is one of the recorded watch streams, which every copy's
// objects come through. Its stats count the progress answers and the
// events, under its connection's lock.
type k8sWatch struct {
	c        *conn
	st       *stream
	creates  int
	created  int
	pending  []int64 // when each progress request not yet answered went out
	stats    stats
	err      error
	progress stream // the call a progress answer completes, for stats
}

func (ws *k8sWatch) message(_ *stream, m []byte) error {
	if ws.err == nil {
		ws.err = ws.response(m)
	}
	return nil
}

func (ws *k8sWatch) response(m []byte) error {
	var id uint64
	var created, events bool
	at := now()
	inWindow := at >= ws.stats.winStart && at < ws.stats.winStop
	err := fields(m, func(n protowire.Number, v uint64, b []byte) error {
		//exhaustive:ignore // other fields do not change the watch
		switch n {
		case watchResponseID:
			id = v
		case watchCreated:
			created = v == 1
		case watchCanceled, watchCompactRev:
			if v != 0 {
				return errors.New("watch canceled or compacted")
			}
		case watchEvents:
			events = true
			var kvs int64
			err := fields(b, func(n protowire.Number, _ uint64, _ []byte) error {
				if n == eventKV || n == eventPrevKV {
					kvs++
				}
				return nil
			})
			if err != nil {
				return err
			}
			if inWindow {
				ws.stats.c.Events += kvs
				ws.stats.events++
			}
		}
		return nil
	})
	if inWindow {
		ws.stats.c.In += grpcPrefixLen + int64(len(m))
	}
	switch {
	case err != nil:
		return err
	case created:
		ws.created++
	case !events && int64(id) == -1 && len(ws.pending) > 0:
		// The answer to the oldest progress request.
		sent := ws.pending[0]
		ws.pending = ws.pending[1:]
		ws.progress = stream{intended: sent, sent: sent, msgs: 1, c: counts{Out: grpcPrefixLen + 2}}
		ws.stats.complete(&ws.progress, at, nil)
	}
	return nil
}

func (ws *k8sWatch) done(_ *stream, _ int64, err error) {
	if err == nil {
		err = errors.New("watch stream ended")
	}
	if ws.err == nil {
		ws.err = err
	}
}

var errTraceTooShort = errors.New("a copy ran out of its recording; record longer windows or replay more copies")

// checkCopies fails a measurement in which a copy reached the end of the
// trace: from then on it sent nothing, and the load was no longer the
// recording's.
func checkCopies(copies []*k8sCopy) error {
	for _, cp := range copies {
		if cp.next == len(cp.t.Calls) {
			return fmt.Errorf("copy %d: %w", cp.id, errTraceTooShort)
		}
	}
	return nil
}

func traceDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("-traces: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("-traces %q: %w", dir, errNotDir)
	}
	return nil
}

var errNotDir = errors.New("not a directory")
