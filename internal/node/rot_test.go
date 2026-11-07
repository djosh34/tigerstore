package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// errDamagedRead is the failure TestPlanted counts as catching B25; any
// other error is a setup failure.
var errDamagedRead = errors.New("a read returned a damaged value")

// damagedValueIsRefused writes one value, lets its entry leave memory,
// flips a bit of the value where the log holds it, and reads it. The
// node has no cache and no cleaner, so the read goes to that place, and
// only the value's checksum can see the damage: the read must fail and
// the node stop, never answer with the damaged bytes (#541).
func damagedValueIsRefused(t *testing.T) (err error) {
	d := &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}}
	cfg := Config{Core: core.DefaultConfig(1, []uint64{1}), Tick: 10 * time.Millisecond, SegmentBytes: 64 << 10, Logf: t.Logf}
	cfg.Core.WindowBytes, cfg.Core.WindowMaxBytes = 4<<10, 8<<10
	cfg.Core.CheckpointBytes = 1 << 40
	n, err := newOnDisk(cfg, "", d)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	stopped := false
	defer func() {
		cancel()
		if !stopped {
			<-ran
		}
	}()
	value := fmt.Appendf(nil, "the value that rots %01000d", 7)
	if _, err := do(n, &pb.PutRequest{Key: []byte("rot"), Value: value}); err != nil {
		return err
	}
	// Enough other writes that the entry of the value leaves the window.
	for i := range 64 {
		if _, err := do(n, &pb.PutRequest{Key: fmt.Appendf(nil, "fill%02d", i), Value: bytes.Repeat([]byte("f"), 512)}); err != nil {
			return err
		}
	}
	err = serialErr(d, func() error {
		for name, data := range d.d.Files() {
			if at := bytes.Index(data, value); at >= 0 {
				d.d.Rot(name, at+len(value)/2)
				return nil
			}
		}
		return errors.New("the value is in no file")
	})
	if err != nil {
		return err
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	r, err := n.Do(rctx, &pb.RangeRequest{Key: []byte("rot")})
	if err != nil {
		// The read failed. It must be because the node stopped on the
		// value's checksum, as it must, not for another reason.
		stopped = true
		select {
		case rerr := <-ran:
			if !errors.Is(rerr, mvcc.ErrBadValue) {
				return fmt.Errorf("the read failed with %w, and the node stopped with %w, not on the checksum", err, rerr)
			}
			return nil
		case <-time.After(5 * time.Second):
			stopped = false
			return fmt.Errorf("the read failed with %w, and the node goes on", err)
		}
	}
	if kvs := r.(*pb.RangeResponse).Kvs; len(kvs) == 1 && !bytes.Equal(kvs[0].Value, value) {
		return errDamagedRead
	}
	return errors.New("the read of the damaged value answered without the damage")
}

// TestDamagedValueIsRefused runs the scenario of B25 without the bug.
func TestDamagedValueIsRefused(t *testing.T) {
	if err := damagedValueIsRefused(t); err != nil {
		t.Fatal(err)
	}
}
