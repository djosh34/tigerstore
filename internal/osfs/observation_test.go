package osfs

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/wal"
)

func TestObservationBucketsAndErrors(t *testing.T) {
	var o FileObservation
	bounds := WallNSBounds()
	want := make([]uint64, len(bounds)+1)
	var sum uint64
	for i, ns := range bounds {
		o.write.record(time.Duration(ns), 3, nil)
		want[i]++
		sum += ns
		o.write.record(time.Duration(ns+1), 2, os.ErrPermission)
		want[i+1]++
		sum += ns + 1
	}
	o.write.record(0, 0, os.ErrClosed)
	want[0]++
	s := o.Snapshot()
	if s.Write.Calls != 23 || s.Write.Errors != 12 || s.Write.Bytes != 55 || s.Write.WallNSSum != sum {
		t.Fatalf("write totals: %+v", s.Write)
	}
	if !reflect.DeepEqual(s.Write.WallNSBuckets, want) {
		t.Fatalf("buckets %v, want %v", s.Write.WallNSBuckets, want)
	}
	if s.Fsync.Calls != 0 || len(s.Fsync.WallNSBuckets) != 12 {
		t.Fatalf("fsync totals: %+v", s.Fsync)
	}
	// Callers cannot mutate live counters through an exported slice.
	s.Write.WallNSBuckets[0] = 999
	if o.Snapshot().Write.WallNSBuckets[0] != want[0] {
		t.Fatal("snapshot shares counter storage")
	}
}

func TestObservationConcurrentCounters(t *testing.T) {
	var o FileObservation
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				o.write.record(4*time.Microsecond, 7, os.ErrPermission)
				o.fsync.record(16*time.Microsecond, 0, nil)
				_ = o.Snapshot()
			}
		})
	}
	wg.Wait()
	s := o.Snapshot()
	if s.Write.Calls != 400 || s.Write.Errors != 400 || s.Write.Bytes != 2800 || s.Write.WallNSSum != 1_600_000 || s.Write.WallNSBuckets[1] != 400 || s.Fsync.Calls != 400 || s.Fsync.Errors != 0 || s.Fsync.WallNSSum != 6_400_000 || s.Fsync.WallNSBuckets[2] != 400 {
		t.Fatalf("concurrent totals: %+v", s)
	}
}

func TestObservationFileResults(t *testing.T) {
	dir, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plain, err := dir.Observe(nil).Create("plain")
	if err != nil {
		t.Fatal(err)
	}
	if _, observed := plain.(observedFile); observed {
		t.Fatal("off mode wrapped the file")
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	var o FileObservation
	dir = dir.Observe(&o)
	f, err := dir.Create("observed")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write([]byte("value")); n != 5 || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := dir.SyncDir(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write([]byte("lost")); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed write = %d, %v", n, err)
	}
	if err := f.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed sync = %v", err)
	}
	f, err = dir.Open("observed")
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.ReadAll()
	if err != nil || string(b) != "value" {
		t.Fatalf("read = %q, %v", b, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	s := o.Snapshot()
	if s.Write.Calls != 2 || s.Write.Errors != 1 || s.Write.Bytes != 5 || s.Fsync.Calls != 2 || s.Fsync.Errors != 1 || s.Fsync.Bytes != 0 {
		t.Fatalf("file totals: %+v", s)
	}
	for _, op := range []FileOperation{s.Write, s.Fsync} {
		var calls uint64
		for _, count := range op.WallNSBuckets {
			calls += count
		}
		if calls != op.Calls {
			t.Fatalf("bucket sum %d, calls %d", calls, op.Calls)
		}
	}
}

func TestObservationWALBytesAndRecoveryUnchanged(t *testing.T) {
	var observed FileObservation
	var dirs []Dir
	for _, counters := range []*FileObservation{nil, &observed} {
		dir, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dir = dir.Observe(counters)
		dirs = append(dirs, dir)
		w, _, err := wal.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		w.SetSegmentBytes(100)
		for i := uint64(1); i <= 20; i++ {
			w.Append(&wal.HardState{Term: 1, Commit: i}, []*raftpb.Entry{{Term: new(uint64(1)), Index: new(i), Data: []byte("value")}})
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		w, recovered, err := wal.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(recovered.Entries) != 20 || recovered.HardState.Commit != 20 {
			t.Fatalf("recovery: %+v", recovered)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	left, err := dirs[0].List()
	if err != nil {
		t.Fatal(err)
	}
	right, err := dirs[1].List()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("files changed: %v vs %v", left, right)
	}
	for _, name := range left {
		var contents [][]byte
		for _, dir := range dirs {
			f, err := dir.Open(name)
			if err != nil {
				t.Fatal(err)
			}
			b, err := f.ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			contents = append(contents, b)
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(contents[0], contents[1]) {
			t.Fatalf("WAL bytes changed in %s", name)
		}
	}
	if s := observed.Snapshot(); s.Write.Calls <= 20 || s.Fsync.Calls <= 20 || s.Write.Errors != 0 || s.Fsync.Errors != 0 {
		t.Fatalf("header calls not counted: %+v", s)
	}
}
