package history

import "encoding/json"

// Fingerprint identifies a request by what it can change: its content,
// without a Txn's failure branch that only reads. That branch changes
// nothing, and internal/wl's log leaves it out, so a request and its
// copy in the log have one fingerprint.
func (r Request) Fingerprint() (string, error) {
	if t := r.Txn; t != nil && len(t.Failure) > 0 && readsOnly(t.Failure) {
		kept := *t
		kept.Failure = nil
		r.Txn = &kept
	}
	b, err := json.Marshal(r)
	return string(b), err
}

// readsOnly reports whether ops are ranges only.
func readsOnly(ops []TxnOp) bool {
	for _, o := range ops {
		if o.Range == nil {
			return false
		}
	}
	return true
}
