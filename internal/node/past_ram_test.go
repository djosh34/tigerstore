package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/wal"
)

// The real cleaner may pack a segment while a reader holds its file, but
// must not remove that file until the reader finishes. No new write is
// needed to release the pending removal.
func TestColdReaderDuringCleaning(t *testing.T) {
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	dir := t.TempDir()
	r := runClean(t, dir, 0, func(c *Config) {
		c.BeforeRead = func() {
			if armed.CompareAndSwap(true, false) {
				close(entered)
				<-release
			}
		}
	})
	// Release before runClean's cleanup waits for the helper to stop.
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	cold, hot := keys("c", 20), keys("h", 20)
	if _, err := putAll(r.n, 0, cold...); err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 40; round++ {
		rev, err := putAll(r.n, round, hot...)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := do(r.n, &pb.CompactionRequest{Revision: rev}); err != nil {
			t.Fatal(err)
		}
	}
	original := filepath.Join(dir, "wal", wal.SegmentName(1))
	if _, err := os.Stat(original); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	done := make(chan error, 1)
	go func() {
		resp, err := do(r.n, &pb.RangeRequest{Key: []byte(cold[0])})
		if err == nil {
			kvs := resp.(*pb.RangeResponse).Kvs
			if len(kvs) != 1 || string(kvs[0].Value) != string(bigValue(cold[0], 0)) {
				err = fmt.Errorf("held reader returned %v", kvs)
			}
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("read did not reach disk: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not enter within 5 seconds")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.n.Defragment(ctx); err != nil {
		t.Fatal(err)
	}
	_, packs := walFiles(t, dir)
	if packs == 0 {
		t.Fatal("cleaner made no pack")
	}
	if _, err := os.Stat(original); err != nil {
		t.Fatalf("cleaning removed a segment its reader holds: %v", err)
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		_, err := os.Stat(original)
		if os.IsNotExist(err) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("original segment remains after reader finished: %v", err)
		}
	}
}
