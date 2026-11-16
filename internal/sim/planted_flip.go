//go:build planted

package sim

import (
	"fmt"

	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
)

func init() {
	flipPayload = func(s *Sim, n *node) bool {
		type spot struct {
			name string
			off  int
		}
		var spots []spot
		if s.cfg.wl {
			// On wl: the last byte of an entry's data in its log.
			var ptr wl.Pointer
			if b := n.disk.DurableBytes(wlPointerFile); b != nil {
				var err error
				if ptr, err = wl.ReadPointer(b); err != nil {
					return false
				}
			}
			seg := func(k int64) []byte { return n.disk.DurableBytes(wlSegment(k)) }
			for _, e := range wl.PayloadEnds(seg, ptr.LogFrom) {
				spots = append(spots, spot{wlSegment(e[0]), int(e[1])})
			}
		} else {
			for _, name := range n.disk.DurableNames() {
				for _, off := range wal.PayloadEnds(n.disk.DurableBytes(name)) {
					spots = append(spots, spot{name, off})
				}
			}
		}
		if len(spots) == 0 {
			return false
		}
		p := spots[s.rng.IntN(len(spots))]
		n.disk.FlipDurable(p.name, p.off)
		return true
	}
}

// wlSegment is the name of segment k of a wl node's log on its disk.
func wlSegment(k int64) string { return fmt.Sprintf("wal/%016x.seg", k) }
