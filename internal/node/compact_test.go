package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
)

// runCompacting starts a single node with auto compaction.
func runCompacting(t *testing.T, mode string, retention time.Duration) *Node {
	t.Helper()
	n, err := New(Config{
		Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir(), Tick: 10 * time.Millisecond, Logf: t.Logf,
		AutoCompactionMode: mode, AutoCompactionRetention: retention, RevisionCompactEvery: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := n.Run(ctx, noPeers{}); err != nil {
			t.Errorf("the node stopped: %v", err)
		}
	})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return n
}

// compacted reports whether a range at rev answers ErrGRPCCompacted.
func compacted(n *Node, rev int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := n.Do(ctx, &pb.RangeRequest{Key: []byte("k"), Revision: rev})
	if errors.Is(err, rpctypes.ErrGRPCCompacted) {
		return true, nil
	}
	return false, err
}

// TestAutoCompaction runs etcd's two compactors: periodic compacts at
// the revision of one retention ago, revision at the current revision
// less the retention.
func TestAutoCompaction(t *testing.T) {
	t.Run("periodic", func(t *testing.T) {
		n := runCompacting(t, CompactPeriodic, time.Second)
		var early int64
		for i := 0; ; i++ {
			r, err := do(n, &pb.PutRequest{Key: []byte("k"), Value: fmt.Appendf(nil, "%d", i)})
			if err != nil {
				t.Fatal(err)
			}
			rev := r.(*pb.PutResponse).Header.Revision
			if i == 0 {
				early = rev
			}
			if ok, err := compacted(n, early); err != nil || ok {
				// Not before a second has passed.
				if err == nil && i < 8 {
					t.Fatalf("compacted after %d puts 100 ms apart", i)
				}
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if i == 50 {
				t.Fatal("no compaction after 5 s")
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	t.Run("revision", func(t *testing.T) {
		n := runCompacting(t, CompactRevision, 5)
		var rev int64
		for i := range 20 {
			r, err := do(n, &pb.PutRequest{Key: []byte("k"), Value: fmt.Appendf(nil, "%d", i)})
			if err != nil {
				t.Fatal(err)
			}
			rev = r.(*pb.PutResponse).Header.Revision
		}
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			below, err := compacted(n, rev-6)
			if err != nil {
				t.Fatal(err)
			}
			if below {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no compaction after 5 s")
			}
		}
		if ok, err := compacted(n, rev-4); err != nil || ok {
			t.Fatalf("revision %d of %d, within the retention of 5: compacted %v, %v", rev-4, rev, ok, err)
		}
	})
}

func TestParseCompaction(t *testing.T) {
	for _, c := range []struct {
		mode, retention string
		want            time.Duration
		bad             bool
	}{
		{"periodic", "0", 0, false},
		{"periodic", "", 0, false},
		{"periodic", "2", 2 * time.Hour, false},
		{"periodic", "5m", 5 * time.Minute, false},
		{"revision", "1000", 1000, false},
		{"revision", "1m", time.Minute, false},
		{"periodic", "-1", 0, true},
		{"periodic", "soon", 0, true},
		{"", "1", 0, true},
		{"daily", "1", 0, true},
	} {
		got, err := ParseCompaction(c.mode, c.retention)
		if (err != nil) != c.bad || got != c.want {
			t.Errorf("ParseCompaction(%q, %q) = %v, %v", c.mode, c.retention, got, err)
		}
	}
}
