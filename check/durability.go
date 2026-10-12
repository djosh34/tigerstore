package check

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/djosh34/tigerstore/history"
)

// Durable checks h.Ops against h.Log, the log the cluster has after the run,
// for example after one disk was erased and the other nodes cold-restarted:
//
//   - Every acked mutation that changed the store is in the log exactly as it
//     was sent, and replaying the log gives the response the client got,
//     header revision included. Overwrites, deletes, lease expiry and
//     compaction later in the log are legal.
//   - Every successful read, including the final scans after recovery,
//     returns what the log holds at its revision. A read can not have seen
//     a write the log lost, and the log can not hold a key no read returns.
//   - Every log entry that changed the store is a request some client sent,
//     used at most once. A LeaseRevoke may also be a lease expiry.
//
// Serializable reads are checked like the others: they must match the log
// at their own header revision.
func Durable(h *history.History) error {
	if err := h.Validate(); err != nil {
		return err
	}
	r, err := newReplay(h.Log)
	if err != nil {
		return err
	}
	var errs []error
	fail := func(format string, args ...any) {
		if len(errs) < maxErrors {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	// Log entries that changed the store, by their request. A grant is also
	// filed with ID -1, for ambiguous grants whose ID the client never
	// learned.
	entries := map[string][]int{}
	claimed := map[int]bool{}
	file := func(req history.Request, i int) error {
		k, err := fingerprint(req)
		entries[k] = append(entries[k], i)
		return err
	}
	for i, o := range r.outcomes {
		if o.err != nil || !changes(h.Log[i], o) {
			claimed[i] = true
			continue
		}
		if err := file(h.Log[i], i); err != nil {
			return err
		}
		if g := h.Log[i].LeaseGrant; g != nil {
			if err := file(anyGrant(g.TTL), i); err != nil {
				return err
			}
		}
	}
	claim := func(req history.Request, fits func(int) bool) (bool, error) {
		k, err := fingerprint(req)
		if err != nil {
			return false, err
		}
		for _, i := range entries[k] {
			if !claimed[i] && fits(i) {
				claimed[i] = true
				return true, nil
			}
		}
		return false, nil
	}

	// Acked mutations first, so they claim the entry that gave their
	// response.
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp == nil || !op.Req.Mutates() || !respChanges(op) {
			continue
		}
		req := modelRequest(op)
		ok, err := claim(req, func(j int) bool { return matches(r.outcomes[j], op.Resp) })
		if err != nil {
			return err
		}
		if !ok {
			fail("acked op %d (client %d, revision %d) %s is not in the log with the response the client got", i, op.Client, op.Resp.Revision, describeRequest(req))
		}
	}
	// Then ambiguous ones, which may or may not be in the log.
	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp != nil || !op.Err.Ambiguous || !op.Req.Mutates() {
			continue
		}
		req := op.Req
		if g := req.LeaseGrant; g != nil && g.ID == 0 {
			req = anyGrant(g.TTL)
		}
		if _, err := claim(req, func(int) bool { return true }); err != nil {
			return err
		}
	}
	for i := range h.Log {
		if claimed[i] || h.Log[i].LeaseRevoke != nil {
			// An unclaimed revoke is a lease expiry.
			continue
		}
		fail("log entry %d %s changed the store but no client sent it, or its client got an error that says it had no effect", i, describeRequest(h.Log[i]))
	}

	for i := range h.Ops {
		op := &h.Ops[i]
		if op.Resp == nil {
			continue
		}
		var reads []*history.RangeRequest
		var got []*history.RangeResponse
		switch {
		case op.Req.Range != nil:
			reads, got = []*history.RangeRequest{op.Req.Range}, []*history.RangeResponse{op.Resp.Range}
		case op.Req.Txn != nil && !respChanges(op):
			// A txn that changed nothing reads at its header revision.
			// One that wrote is checked through its log entry above.
			branch := op.Req.Txn.Success
			if !op.Resp.Txn.Succeeded {
				branch = op.Req.Txn.Failure
			}
			for j, o := range branch {
				reads = append(reads, o.Range)
				got = append(got, op.Resp.Txn.Results[j].Range)
			}
		}
		for j, q := range reads {
			rev := q.Revision
			if rev == 0 {
				rev = op.Resp.Revision
			}
			if op.Resp.Revision > r.rev {
				fail("op %d (client %d) read at revision %d, above the last revision %d of the log", i, op.Client, op.Resp.Revision, r.rev)
				break
			}
			if want := r.read(q, rev); !sameRange(want, got[j]) {
				fail("op %d (client %d) read %+v at revision %d and got %+v, but the log holds %+v", i, op.Client, *q, rev, *got[j], *want)
			}
		}
	}
	return errors.Join(errs...)
}

// fingerprint identifies a request by its content.
func fingerprint(r history.Request) (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("fingerprint of %s: %w", describeRequest(r), err)
	}
	return string(b), nil
}

// anyGrant stands for a grant of any ID with the given TTL.
func anyGrant(ttl int64) history.Request {
	return history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: -1, TTL: ttl}}
}

// changes reports whether a request with outcome o changed the store.
func changes(req history.Request, o outcome) bool {
	switch {
	case req.LeaseGrant != nil, req.LeaseRevoke != nil, req.Compact != nil, req.Put != nil:
		return true
	case req.Delete != nil:
		return o.resp.Delete.Deleted > 0
	case req.Txn != nil:
		return txnChanges(req.Txn, o.resp.Txn)
	}
	return false
}

// respChanges reports whether the response of an acked op shows that it
// changed the store.
func respChanges(op *history.Op) bool {
	switch {
	case op.Req.Delete != nil:
		return op.Resp.Delete.Deleted > 0
	case op.Req.Txn != nil:
		return txnChanges(op.Req.Txn, op.Resp.Txn)
	}
	return true
}

func txnChanges(t *history.TxnRequest, resp *history.TxnResponse) bool {
	branch := t.Success
	if !resp.Succeeded {
		branch = t.Failure
	}
	for i, o := range branch {
		if o.Put != nil || o.Delete != nil && resp.Results[i].Delete.Deleted > 0 {
			return true
		}
	}
	return false
}

func describeRequest(r history.Request) string {
	switch {
	case r.Range != nil:
		return fmt.Sprintf("range %+v", *r.Range)
	case r.Put != nil:
		return fmt.Sprintf("put %+v", *r.Put)
	case r.Delete != nil:
		return fmt.Sprintf("delete %+v", *r.Delete)
	case r.Txn != nil:
		return fmt.Sprintf("txn %+v", *r.Txn)
	case r.LeaseGrant != nil:
		return fmt.Sprintf("lease grant %+v", *r.LeaseGrant)
	case r.LeaseRevoke != nil:
		return fmt.Sprintf("lease revoke %+v", *r.LeaseRevoke)
	case r.Compact != nil:
		return fmt.Sprintf("compact %+v", *r.Compact)
	}
	return "empty request"
}
