//go:build planted

package sim

import "github.com/djosh34/tigerstore/internal/wal"

func init() {
	flipPayload = func(s *Sim, n *node) bool {
		type spot struct {
			name string
			off  int
		}
		var spots []spot
		for _, name := range n.disk.DurableNames() {
			for _, off := range wal.PayloadEnds(n.disk.DurableBytes(name)) {
				spots = append(spots, spot{name, off})
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
