package sim

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// exp441Stats marks the shape of a checkable history (#441).
func exp441Stats(h *history.History) {
	failed, ambiguous, effect, watchEvents := 0, 0, 0, 0
	for _, op := range h.Ops {
		if op.Err != nil {
			failed++
			if op.Err.Ambiguous {
				ambiguous++
				if op.Effect != nil {
					effect++
				}
			}
		}
	}
	for _, w := range h.Watches {
		for _, r := range w.Responses {
			watchEvents += len(r.Events)
		}
	}
	mark("stats ops=%d failed=%d ambiguous=%d ambiguous-with-effect=%d log=%d watches=%d watch-events=%d target=%d",
		len(h.Ops), failed, ambiguous, effect, len(h.Log), len(h.Watches), watchEvents, h.Target)
}

// exp441Save writes the history, gzipped, to $EXP441_OUT on the real disk
// (#441).
func exp441Save(h *history.History) {
	out := os.Getenv("EXP441_OUT")
	if out == "" {
		return
	}
	mark("phase save-history start")
	f := simrun.RealCreate(filepath.Join(out, "history.json.gz"))
	if f == nil {
		mark("save-history: cannot create")
		return
	}
	z, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err := json.NewEncoder(z).Encode(h); err != nil {
		mark("save-history: %v", err)
	}
	if err := z.Close(); err != nil {
		mark("save-history: %v", err)
	}
	if err := f.Close(); err != nil {
		mark("save-history: %v", err)
	}
	mark("phase save-history end")
}
