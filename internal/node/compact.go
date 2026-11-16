package node

import (
	"errors"
	"fmt"
	"strconv"
	"time"
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
