package main

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/djosh34/tigerstore/history"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const pressureSize = 96 << 10

// Pressure keys are outside the hot watch prefix. Every put and cold read
// is still in the same checked history, and the final paginated reads
// through all members check the entire key space. Random printable values
// avoid compression turning a past-RAM run into a small working set.
func pressureValue(rng *rand.Rand, identity string) string {
	b := make([]byte, pressureSize)
	for i := range b {
		b[i] = byte(33 + rng.IntN(90))
	}
	copy(b, identity)
	return string(b)
}
func pressureKey(i int) string { return fmt.Sprintf("/pressure/%06d", i) }

func (t *traffic) preloadPressure(keys int) error {
	rng := rand.New(rand.NewPCG(t.cfg.seed, 80))
	for k := range keys {
		value := pressureValue(rng, fmt.Sprintf("preload-%d-%d:", t.cfg.seed, k))
		op := t.r.call(t.cs[k%3], 80, history.Request{Put: &history.PutRequest{Key: pressureKey(k), Value: value}}, livenessBound)
		if op.Resp == nil {
			return fmt.Errorf("pressure key %d: %v", k, op.Err)
		}
		// Keep rotations farther apart than the WAL archiver's 100ms pass.
		time.Sleep(30 * time.Millisecond)
	}
	for i, cl := range t.cs {
		t.workers.Go(func() {
			rng := rand.New(rand.NewPCG(t.cfg.seed, uint64(81+i)))
			tick := time.NewTicker(300 * time.Millisecond)
			defer tick.Stop()
			for seq := 0; ; seq++ {
				select {
				case <-t.loadCtx.Done():
					return
				case <-tick.C:
				}
				key := pressureKey(rng.IntN(keys))
				q := history.Request{Range: &history.RangeRequest{Key: key}}
				if seq%5 == 0 {
					q = history.Request{Put: &history.PutRequest{Key: key, Value: pressureValue(rng, fmt.Sprintf("pressure-%d-%d-%d:", t.cfg.seed, i, seq))}}
				}
				t.r.call(cl, 81+i, q, t.cfg.requestTimeout)
			}
		})
	}
	return nil
}

// terminalRead records bounded pages at one revision, instead of asking a
// memory-limited node to buffer all live values in one response.
func (t *traffic) terminalRead(cl *clientv3.Client, client int) history.Op {
	var terminal history.Op
	key := "\x00"
	var rev int64
	for {
		q := history.Request{Range: &history.RangeRequest{Key: key, End: history.AllKeys, Revision: rev, Limit: 64}}
		op := t.r.call(cl, client, q, t.cfg.requestTimeout)
		if op.Resp == nil {
			return op
		}
		if terminal.Resp == nil {
			terminal = op
			response := *op.Resp
			page := *op.Resp.Range
			page.KVs = nil
			response.Range = &page
			terminal.Resp = &response
			rev = op.Resp.Revision
		}
		terminal.Resp.Range.KVs = append(terminal.Resp.Range.KVs, op.Resp.Range.KVs...)
		if !op.Resp.Range.More {
			terminal.Resp.Range.More = false
			return terminal
		}
		kvs := op.Resp.Range.KVs
		if len(kvs) == 0 {
			return history.Op{Err: &history.Error{Message: "terminal range has More with no keys"}}
		}
		key = kvs[len(kvs)-1].Key + "\x00"
	}
}
