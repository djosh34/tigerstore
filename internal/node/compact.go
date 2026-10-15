package node

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
)

// Auto compaction follows etcd v3.7.2's compactors
// (etcdserver/api/v3compactor), which run on the leader only and compact
// through the log like a client's Compact, as the root user.
//
// periodic, with a retention R: it notes the revision every R/10 (at
// most every 6 minutes), and once R has passed since the start, or since
// the last compaction with R up to an hour and else an hour, it compacts
// at the revision it noted R ago.
//
// revision, with a retention of N revisions: every 5 minutes it compacts
// at the current revision less N.

// The compaction modes of --auto-compaction-mode.
const (
	CompactPeriodic = "periodic"
	CompactRevision = "revision"
)

// revisionInterval is how often the revision compactor runs.
const revisionInterval = 5 * time.Minute

// Compaction is an auto compaction setting: a Period to keep in periodic
// mode, or a count of Revisions to keep in revision mode. The zero
// Compaction turns auto compaction off.
type Compaction struct {
	Period    time.Duration
	Revisions int64
}

// ParseCompaction is etcd's parseCompactionRetention with its check of
// the mode: a number is hours in periodic mode and revisions in revision
// mode; anything else is a duration, which revision mode takes as its
// count of nanoseconds, as etcd does. 0 turns auto compaction off.
func ParseCompaction(mode, retention string) (Compaction, error) {
	switch mode {
	case CompactPeriodic, CompactRevision:
	case "":
		return Compaction{}, errors.New("undefined auto-compaction-mode")
	default:
		return Compaction{}, fmt.Errorf("unknown auto-compaction-mode %q", mode)
	}
	if retention == "" {
		retention = "0"
	}
	d, err := time.ParseDuration(retention)
	if h, aerr := strconv.Atoi(retention); aerr == nil && h >= 0 {
		d, err = time.Duration(h), nil
		if mode == CompactPeriodic {
			d = time.Duration(h) * time.Hour
		}
	}
	if err != nil {
		return Compaction{}, fmt.Errorf("error parsing CompactionRetention: %w", err)
	}
	if mode == CompactRevision {
		return Compaction{Revisions: int64(d)}, nil
	}
	return Compaction{Period: d}, nil
}

// leads reports whether this node is the leader, which alone compacts.
func (n *Node) leads() bool { return n.Leader() == n.cfg.Core.ID }

// revision is the node's current revision.
func (n *Node) revision(ctx context.Context) (int64, error) {
	st, err := n.Status(ctx)
	if err != nil {
		return 0, err
	}
	return st.Header.Revision, nil
}

// autoCompact compacts at rev as the root user. A revision compacted
// already counts as done, as in etcd.
func (n *Node) autoCompact(ctx context.Context, rev int64) bool {
	_, err := n.DoAs(ctx, core.RootCaller(), &pb.CompactionRequest{Revision: rev})
	if err != nil && !errors.Is(err, rpctypes.ErrGRPCCompacted) {
		n.logf("auto compaction at revision %d: %v", rev, err)
		return false
	}
	n.logf("auto compaction at revision %d", rev)
	return true
}

// compactPeriodically is etcd's Periodic compactor.
func (n *Node) compactPeriodically(ctx context.Context, period time.Duration) {
	compactInterval := min(period, time.Hour)
	retry := compactInterval / 10
	retentions := int(period/retry) + 1
	revs := make([]int64, 0, retentions)
	var lastRevision int64
	lastSuccess := time.Now()
	base := period
	for {
		rev, err := n.revision(ctx)
		if err != nil {
			return
		}
		revs = append(revs, rev)
		if len(revs) > retentions {
			revs = revs[1:]
		}
		if !sleep(ctx, retry) {
			return
		}
		if !n.leads() {
			continue
		}
		rev = revs[0]
		if time.Since(lastSuccess) < base || rev == lastRevision {
			continue
		}
		base = compactInterval
		if n.autoCompact(ctx, rev) {
			lastRevision, lastSuccess = rev, time.Now()
		}
	}
}

// compactByRevision is etcd's Revision compactor.
func (n *Node) compactByRevision(ctx context.Context, retention int64, every time.Duration) {
	var prev int64
	for sleep(ctx, every) {
		if !n.leads() {
			continue
		}
		rev, err := n.revision(ctx)
		if err != nil {
			return
		}
		rev -= retention
		if rev <= 0 || rev == prev {
			continue
		}
		if n.autoCompact(ctx, rev) {
			prev = rev
		}
	}
}
