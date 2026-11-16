package node

import (
	"testing"
	"time"
)

func TestParseCompaction(t *testing.T) {
	for _, c := range []struct {
		mode, retention string
		want            Compaction
		bad             bool
	}{
		{"periodic", "0", Compaction{}, false},
		{"periodic", "", Compaction{}, false},
		{"periodic", "2", Compaction{Period: 2 * time.Hour}, false},
		{"periodic", "5m", Compaction{Period: 5 * time.Minute}, false},
		{"revision", "1000", Compaction{Revisions: 1000}, false},
		{"revision", "1m", Compaction{Revisions: int64(time.Minute)}, false},
		{"periodic", "-1", Compaction{}, true},
		{"periodic", "soon", Compaction{}, true},
		{"", "1", Compaction{}, true},
		{"daily", "1", Compaction{}, true},
		// etcd lets a negative duration through. In revision mode it
		// then compacts nothing, as tigerstore does; in periodic mode it
		// compacts in a loop that never waits, which tigerstore does not
		// copy (docs/design.md).
		{"revision", "-5m", Compaction{Revisions: int64(-5 * time.Minute)}, false},
		{"periodic", "-1h", Compaction{Period: -time.Hour}, false},
	} {
		got, err := ParseCompaction(c.mode, c.retention)
		if (err != nil) != c.bad || got != c.want {
			t.Errorf("ParseCompaction(%q, %q) = %v, %v", c.mode, c.retention, got, err)
		}
	}
}
