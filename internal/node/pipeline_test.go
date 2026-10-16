package node

import (
	"errors"
	"testing"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/wal"
)

// syncFile is a segment whose syncs fail from the failAt-th on.
type syncFile struct {
	wal.File
	syncs, failAt int
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
// second sync failing: the groups come back in order, the first synced
// and every later one with the error, also one whose own sync would have
// worked, and one that needed no sync.
func TestSyncGroupsStopAtFailure(t *testing.T) {
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{cfg: Config{Metrics: m}}
	f := &syncFile{failAt: 2}
	groups := make(chan written)
	synced := make(chan syncedGroup, 4)
	done := make(chan struct{})
	reported := 0
	go func() {
		n.syncGroups(groups, synced, func(_ written, err error) error {
			reported++
			return err
		})
		close(done)
	}()
	for i, file := range []wal.File{f, f, nil, f} {
		groups <- written{jobs: make([]core.PersistJob, i+1), f: file}
		s := <-synced
		if reported != i+1 {
			t.Fatalf("group %d returned before its report", i)
		}
		if len(s.g.jobs) != i+1 {
			t.Fatalf("group %d came back as group %d", i, len(s.g.jobs)-1)
		}
		if want := i >= 1; (s.err != nil) != want || (want && !errors.Is(s.err, errSync)) {
			t.Fatalf("group %d came back with %v", i, s.err)
		}
	}
	close(groups)
	<-done
	if f.syncs != 2 {
		t.Fatalf("%d syncs, want none after the failed one", f.syncs)
	}
}

// A failure after sync, such as removing obsolete files, must also stop
// the pipeline. No later group may be reported as successful.
func TestSyncGroupsStopAtReportFailure(t *testing.T) {
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{cfg: Config{Metrics: m}}
	f := &syncFile{failAt: 10}
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
