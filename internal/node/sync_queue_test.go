package node

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
)

// A written-only group waits for earlier syncs, not for a later append's
// sync. Queue two such groups behind a held sync, then another append.
func TestSyncGroupsReportWrittenPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, err := metrics.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		n := &Node{cfg: Config{Metrics: m, BeforeSync: func() { <-release }}}
		f := &syncFile{failAt: 3}
		groups := make(chan written, 4)
		synced := make(chan syncedGroup, 4)
		reported := make(chan written, 4)
		done := make(chan struct{})
		now := time.Now()
		group := func(id int, needsSync bool) written {
			g := written{jobs: make([]core.PersistJob, id+1), flushed: now}
			if needsSync {
				g.f = f
			}
			return g
		}
		groups <- group(0, true)
		go func() {
			n.syncGroups(groups, synced, func(g written, err error) error {
				reported <- g
				return err
			})
			close(done)
		}()
		defer func() {
			close(release)
			close(groups)
			<-done
		}()
		synctest.Wait()
		groups <- group(1, false)
		groups <- group(2, false)
		groups <- group(3, true)
		synctest.Wait()
		if len(reported) != 0 {
			t.Fatal("reported a group before the preceding sync finished")
		}
		// Finish only the first sync. The later append remains held.
		release <- struct{}{}
		synctest.Wait()
		for id := range 3 {
			select {
			case g := <-reported:
				if len(g.jobs) != id+1 {
					t.Fatalf("reported group %d, want %d", len(g.jobs)-1, id)
				}
			default:
				t.Fatalf("written group %d waited for a later sync", id)
			}
		}
		if len(reported) != 0 {
			t.Fatal("reported the later append before its sync")
		}
		release <- struct{}{}
		synctest.Wait()
		g := <-reported
		if len(g.jobs) != 4 {
			t.Fatalf("last report has %d jobs, want 4", len(g.jobs))
		}
		for range 4 {
			if s := <-synced; s.err != nil {
				t.Fatal(s.err)
			}
		}
		if f.syncs != 2 {
			t.Fatalf("got %d syncs, want 2", f.syncs)
		}
	})
}

// A report error in the written-only prefix stops the pipeline before
// the next sync, just as a sync error does.
func TestSyncGroupsWrittenPrefixFailure(t *testing.T) {
	m, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{cfg: Config{Metrics: m}}
	f := &syncFile{failAt: 10}
	groups := make(chan written, 3)
	groups <- written{}
	groups <- written{f: f}
	groups <- written{}
	close(groups)
	synced := make(chan syncedGroup, 3)
	errReport := errors.New("written-only report failed")
	var reports []error
	n.syncGroups(groups, synced, func(_ written, err error) error {
		reports = append(reports, err)
		return errReport
	})
	if f.syncs != 0 {
		t.Errorf("got %d syncs after a failed prefix, want none", f.syncs)
	}
	if len(reports) != 3 || reports[0] != nil || !errors.Is(reports[1], errReport) || !errors.Is(reports[2], errReport) {
		t.Errorf("reports = %v, want success followed by the report error twice", reports)
	}
	for range 3 {
		if s := <-synced; !errors.Is(s.err, errReport) {
			t.Errorf("group returned %v, want the report error", s.err)
		}
	}
}
