package mvcc

// CompareProgress remembers the matching prefix of each VALUE comparison
// while a transaction reads cold values in batches. Only keys and results
// survive a batch, never its values. A changed revision invalidates every
// result, so a read-only transaction still compares one consistent state.
// Budget bounds each batch's value copies, references and map entries.
// One value is the minimum read unit, even if it exceeds Budget.
type CompareProgress struct {
	Budget int64
	rev    int64
	next   int
	states []compareCursor
}

type compareCursor struct {
	key     []byte
	matched bool
	done    bool
	result  bool
}

func (p *CompareProgress) begin(rev int64) {
	if p == nil {
		return
	}
	if p.rev != rev {
		p.rev, p.states = rev, nil
	}
	p.next = 0
}

func (p *CompareProgress) cursor() *compareCursor {
	if p == nil {
		return nil
	}
	if p.next == len(p.states) {
		p.states = append(p.states, compareCursor{})
	}
	c := &p.states[p.next]
	p.next++
	return c
}

// CompareBytes is the reservation for a batch of comparison inputs.
// It includes value copies and per-value metadata, but no wire encoding:
// comparison inputs never leave the node as part of this read.
func CompareBytes(need []Fill) int64 {
	bytes := int64(1024)
	for _, f := range need {
		bytes += compareValueBytes(f.Key, f.Ref)
	}
	return bytes
}

func compareValueBytes(key []byte, ref Ref) int64 {
	return 512 + 2*int64(len(key)) + 2*int64(ref.Len)
}
