package node

import (
	"errors"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// syncFile is a segment whose syncs fail from the failAt-th on. cut is
// where a failed sync cut it, -1 if none did.
type syncFile struct {
	wal.File
	syncs, failAt int
	cut           int64
}

func (f *syncFile) Truncate(size int64) error {
	f.cut = size
	return nil
}

// segmentFS is a disk whose segments open as syncFiles; seg is the last.
type segmentFS struct {
	*simfs.Disk
	failAt int
	seg    *syncFile
}

func (d *segmentFS) Open(name string) (wal.File, error) {
	f, err := d.Disk.Open(name)
	if err != nil || !strings.HasSuffix(name, ".wal") {
		return f, err
	}
	d.seg = &syncFile{File: f, failAt: d.failAt, cut: -1}
	return d.seg, nil
}

// syncNode is a node whose log is on a segmentFS, and the log's segment.
func syncNode(t *testing.T, failAt int) (*Node, *syncFile) {
	t.Helper()
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	d := &segmentFS{Disk: simfs.New(), failAt: failAt}
	w, _, err := wal.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	return &Node{cfg: Config{Metrics: m}, log: &diskLog{w: w}}, d.seg
}

var errSync = errors.New("sync failed")

func (f *syncFile) Sync() error {
	f.syncs++
	if f.syncs >= f.failAt {
		return errSync
	}
	return nil
}

// TestSyncGroupsStopAtFailure syncs written groups one at a time, the
// second sync failing: the groups come back in order, the first synced, a
// group that needed no sync before the failure, and every later one with
// the error, also one whose own sync would have worked and one that needed
// no sync. The failed sync cuts the segment back to where the first
// group's bytes end, the part synced (#364).
func TestSyncGroupsStopAtFailure(t *testing.T) {
	n, f := syncNode(t, 2)
	groups := make(chan written)
	synced := make(chan syncedGroup, 5)
	done := make(chan struct{})
	reported := 0
	go func() {
		n.syncGroups(groups, synced, func(_ written, err error) error {
			reported++
			return err
		})
		close(done)
	}()
	for i, file := range []wal.File{f, nil, f, nil, f} {
		groups <- written{jobs: make([]core.PersistJob, i+1), f: file, end: int64(100 * (i + 1))}
		s := <-synced
		if reported != i+1 {
			t.Fatalf("group %d returned before its report", i)
		}
		if len(s.g.jobs) != i+1 {
			t.Fatalf("group %d came back as group %d", i, len(s.g.jobs)-1)
		}
		if want := i >= 2; (s.err != nil) != want || (want && !errors.Is(s.err, errSync)) {
			t.Fatalf("group %d came back with %v", i, s.err)
		}
	}
	close(groups)
	<-done
	if f.syncs != 2 {
		t.Fatalf("%d syncs, want none after the failed one", f.syncs)
	}
	if f.cut != 100 {
		t.Fatalf("the failed sync cut the segment at %d, want 100", f.cut)
	}
}

// A failure after sync, such as removing obsolete files, must also stop
// the pipeline. No later group may be reported as successful.
func TestSyncGroupsStopAtReportFailure(t *testing.T) {
	n, f := syncNode(t, 10)
	groups := make(chan written)
	synced := make(chan syncedGroup, 2)
	done := make(chan struct{})
	errReport := errors.New("report failed")
	var reports []error
	go func() {
		n.syncGroups(groups, synced, func(_ written, err error) error {
			reports = append(reports, err)
			if err == nil {
				return errReport
			}
			return err
		})
		close(done)
	}()
	for range 2 {
		groups <- written{f: f}
		s := <-synced
		if !errors.Is(s.err, errReport) {
			t.Errorf("group returned %v, want the report error", s.err)
		}
	}
	close(groups)
	<-done
	if f.syncs != 1 {
		t.Errorf("%d syncs, want none after the failed report", f.syncs)
	}
	if len(reports) != 2 || reports[0] != nil || !errors.Is(reports[1], errReport) {
		t.Errorf("reports = %v, want success followed by the report error", reports)
	}
}
