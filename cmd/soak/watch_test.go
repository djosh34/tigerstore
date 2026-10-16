package main

import (
	"strings"
	"testing"
)

func TestMatch(t *testing.T) {
	put := func(rev int64, val uint64) change { return change{lo: rev, hi: rev, val: val} }
	del := func(lo, hi int64) change { return change{lo: lo, hi: hi, del: true} }
	cases := []struct {
		name        string
		acked, seen []change
		gaps        []span
		progress    int64
		final       bool
		// settled, when set, is the revision of the final list, which saw
		// the object present, on a lease when leased is set.
		settled int64
		leased  bool
		// want is a part of the finding, or "" for none; left is how many
		// changes stay to match later.
		want string
		left int
	}{
		{name: "every change has its event", acked: []change{put(12, 1), del(13, 13)}, seen: []change{put(12, 1), del(13, 13)}, progress: 20, final: true},
		{name: "an event not come yet", acked: []change{put(12, 1), put(25, 2)}, seen: []change{put(12, 1)}, progress: 20, left: 1},
		{name: "a lost event", acked: []change{put(12, 1), put(15, 2)}, seen: []change{put(12, 1)}, progress: 20, want: "did not send the put of value 0000000000000002 at revision 15"},
		{name: "the wrong value", acked: []change{put(12, 1)}, seen: []change{put(12, 9)}, progress: 20, want: "did not send the put"},
		{name: "before the watch", acked: []change{put(5, 1)}, progress: 20, final: true},
		{name: "in a compaction gap", acked: []change{put(12, 1)}, gaps: []span{{11, 14}}, progress: 20, final: true},
		{name: "a delete learned later", acked: []change{del(13, 18)}, seen: []change{del(16, 16)}, progress: 20, final: true},
		{name: "an optional change", acked: []change{{lo: 12, hi: 18, val: 3, opt: true}, {lo: 12, hi: 18, del: true, opt: true}}, seen: []change{del(14, 14)}, progress: 20, final: true},
		{name: "an event no write made, before the end", seen: []change{put(14, 4)}, progress: 20, left: 0},
		{name: "an event no write made", seen: []change{put(14, 4)}, progress: 20, final: true, want: "which no write of its worker made"},
		{name: "a lease that expires after the final list", acked: []change{put(12, 1)}, seen: []change{put(12, 1), del(17, 17)}, progress: 20, final: true, settled: 15, leased: true},
		{name: "two deletes after the final list", acked: []change{put(12, 1)}, seen: []change{put(12, 1), del(17, 17), del(17, 17)}, progress: 20, final: true, settled: 15, leased: true, want: "sent a delete"},
		{name: "a delete after the final list, with no lease", acked: []change{put(12, 1)}, seen: []change{put(12, 1), del(17, 17)}, progress: 20, final: true, settled: 15, want: "sent a delete"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := &watched{prefix: "/registry/pods/", start: 10, gaps: c.gaps}
			o := &object{key: "/registry/pods/soak-w000/obj-0000001", acked: c.acked, seen: c.seen, settled: c.settled, present: c.settled > 0, leased: c.leased}
			err := w.match(o, c.progress, c.final)
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("unexpected finding: %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("got %v, want a finding with %q", err, c.want)
			case c.want == "" && len(o.acked) != c.left:
				t.Fatalf("%d changes left, want %d", len(o.acked), c.left)
			}
		})
	}
}
