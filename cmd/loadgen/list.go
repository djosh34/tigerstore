package main

import (
	"bytes"
	"fmt"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"
)

// listRequest is a current-revision linearizable prefix Range. No limit,
// keys_only, count_only, revision or serializable option is set.
func listRequest() *msg {
	m := newMsg()
	m.bytes(rangeKey, []byte(keyPrefix))
	m.bytes(2, prefixEnd([]byte(keyPrefix)))
	return m
}

// listLoad validates every key and every value, in etcd's default key order.
// The seeded dataset is immutable throughout the read experiment.
type listLoad struct {
	base
	want  [][]byte
	order []int
}

func newListLoad(b base, want [][]byte) *listLoad {
	order := make([]int, b.ks.n)
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, c int) int { return bytes.Compare(b.ks.key(a), b.ks.key(c)) })
	return &listLoad{base: b, want: want, order: order}
}

func (w *listLoad) start(c *conn, st *stream) bool {
	if !c.canStart(w.tpl, w.limit) {
		return false
	}
	c.startStream(st, w.tpl)
	return true
}

func (w *listLoad) exhausted() bool { return false }

func (w *listLoad) message(_ *stream, m []byte) error {
	if _, err := responseRevision(m); err != nil {
		return err
	}
	var count, more uint64
	kvs := 0
	err := fields(m, func(num protowire.Number, v uint64, b []byte) error {
		//exhaustive:ignore // only RangeResponse fields are relevant here
		switch num {
		case rangeCount:
			count = v
		case 3: // RangeResponse.more
			more = v
		case rangeKVs:
			if kvs >= len(w.order) {
				return fmt.Errorf("list returned more than %d keys", len(w.order))
			}
			k := w.order[kvs]
			key, err := bytesField(b, kvKey)
			if err != nil {
				return err
			}
			if !bytes.Equal(key, w.ks.key(k)) {
				return fmt.Errorf("list key %d is %q, want %q", kvs, key, w.ks.key(k))
			}
			value, err := bytesField(b, kvValue)
			if err != nil {
				return err
			}
			if !bytes.Equal(value, w.want[k]) {
				return fmt.Errorf("list key %q returned a %d byte value different from its seed", key, len(value))
			}
			kvs++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if count != uint64(len(w.order)) || kvs != len(w.order) || more != 0 {
		return fmt.Errorf("list returned count %d, %d kvs, more=%d; want exactly %d complete keys", count, kvs, more, len(w.order))
	}
	return nil
}
