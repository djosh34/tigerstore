package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

// The version gate. Each batch carries the format it is written in. A
// binary reads and writes the formats up to maxFormat; a newer format is
// used only once every member supports it, so a cluster can be upgraded
// one node at a time, and rolled back until then. As etcd's cluster
// version, the cluster's format never goes down.
//
// Each member announces, with its name and client URLs, the newest format
// it supports and its run: a count of its starts, kept in the data dir.
// An announcement of an older run than the member's last changes nothing,
// however late it arrives. Every change of a member's support or run, and
// every membership change, starts a new epoch, the index that changed it.
// The cluster takes the newest format all members support only once every
// member has confirmed that epoch, by an announcement that names it: a
// member that was down while another changed its support may come back
// on an older binary, and its earlier support is not consent (#174).

// baseFormat is the format every binary of this engine reads.
const baseFormat = 1

// maxFormat is the newest format this binary reads and writes. The
// rolling-upgrade workflow builds an old and a new binary from this source
// by changing it.
const maxFormat = 1

// ErrTooOld stops a node whose cluster uses a newer format than its
// binary reads.
var ErrTooOld = errors.New("this binary is too old for the cluster")

func tooOld(cluster uint8) error {
	return fmt.Errorf("%w: the cluster uses format %d and this binary reads formats up to %d; a format never goes down once used, so start a binary that reads format %d",
		ErrTooOld, cluster, maxFormat, cluster)
}

// errFormatAhead is a batch in a format the cluster had not taken when
// it was committed: a broken gate.
var errFormatAhead = errors.New("a batch in a format the cluster does not use yet")

// checkFormat checks a committed batch's format.
func (n *node) checkFormat(f uint8) error {
	if f > maxFormat {
		return tooOld(f)
	}
	if f > n.format {
		return fmt.Errorf("%w: format %d, the cluster's %d", errFormatAhead, f, n.format)
	}
	return nil
}

// applyAnnouncement applies a member's announcement, committed at index.
func (n *node) applyAnnouncement(a *member, epoch, index uint64) error {
	i := n.memberAt(a.id)
	if i < 0 {
		return nil
	}
	m := &n.members[i]
	if a.run < m.run {
		return nil
	}
	m.name, m.clientURLs = a.name, a.clientURLs
	if a.format != m.format || a.run != m.run {
		m.format, m.run = a.format, a.run
		n.formatEpoch = index
	}
	if epoch == n.formatEpoch {
		m.confirmed = epoch
	}
	return n.activateFormat()
}

// activateFormat raises the cluster's format to the newest every member
// supports, once every member has confirmed the current epoch.
func (n *node) activateFormat() error {
	low := uint8(255)
	for i := range n.members {
		m := &n.members[i]
		if m.confirmed != n.formatEpoch {
			return nil
		}
		low = min(low, m.format)
	}
	if low > n.format {
		n.format = low
		n.logf("the cluster uses format %d from index %d", low, n.formatEpoch)
	}
	if n.format > maxFormat {
		return tooOld(n.format)
	}
	return nil
}

// supportedFormat is the newest format every member supports.
func (n *node) supportedFormat() uint8 {
	low := uint8(255)
	for i := range n.members {
		low = min(low, n.members[i].format)
	}
	return low
}

// announces reports whether this node, member m, has an announcement to
// make: its name, client URLs, format or run are not the list's, or a
// newer format waits for its confirmation.
func (n *node) announces(m *member) bool {
	if m.run > n.run {
		// A later start of this node announced: this run is stale.
		return false
	}
	if m.name != n.self.name || !slices.Equal(m.clientURLs, n.self.clientURLs) || m.format != maxFormat || m.run != n.run {
		return true
	}
	return m.confirmed != n.formatEpoch && n.supportedFormat() > n.format
}

// startRun takes this start's run: its announcements name it, and its
// batches' sequences start after it.
func (n *node) startRun(run uint64) { n.run, n.fwdSeq = run, run<<32 }

// runFile, in the data dir, counts the node's starts.
const runFile = "run"

// nextRun counts this start in dir's run file, durably before the node
// announces anything, and returns it.
func nextRun(dir string) (uint64, error) {
	path := filepath.Join(dir, runFile)
	var run uint64
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return 0, err
	case len(b) != 8:
		return 0, fmt.Errorf("%s holds %d bytes, not 8", path, len(b))
	default:
		run = binary.LittleEndian.Uint64(b)
	}
	run++
	if err := writeFileSynced(path+".tmp", binary.LittleEndian.AppendUint64(nil, run)); err != nil {
		return 0, err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return 0, err
	}
	return run, syncDir(dir)
}
