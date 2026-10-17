package history

import (
	"encoding/json"
	"errors"
)

// WithoutLostWrites returns a copy of h without the ambiguous writes that
// never took effect, and how many it left out. Porcupine must keep an
// ambiguous write open until the end of the history, and with a few dozen
// of them its search did not finish in a minute.
//
// A write took effect only if it is in the committed log, and the log
// holds every write that succeeded (Durable checks that). So when the log
// holds a request no more often than successful calls sent it, no
// ambiguous call with that request took effect. Most workload writes carry
// a value no other call sends, which makes this exact for them. Leaving
// out an ambiguous call is safe whatever the log says: it can only make
// the check stricter, as a read that saw the call's effect then fails.
func (h *History) WithoutLostWrites() (*History, int, error) {
	inLog := map[string]int{}
	var errs []error
	key := func(q Request) string {
		b, err := json.Marshal(q)
		errs = append(errs, err)
		return string(b)
	}
	for _, q := range h.Log {
		inLog[key(q)]++
	}
	for _, op := range h.Ops {
		if op.Err == nil && op.Req.Mutates() {
			inLog[key(op.Req)]--
		}
	}
	out := *h
	out.Ops = nil
	pruned := 0
	for _, op := range h.Ops {
		if op.Err != nil && op.Err.Ambiguous && op.Req.Mutates() && inLog[key(op.Req)] <= 0 {
			pruned++
			continue
		}
		out.Ops = append(out.Ops, op)
	}
	if err := errors.Join(errs...); err != nil {
		return h, 0, err
	}
	return &out, pruned, nil
}
