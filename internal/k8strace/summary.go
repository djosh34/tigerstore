package k8strace

import (
	"fmt"
	"slices"
	"strings"
)

// Summary is what a trace holds, in numbers.
type Summary struct {
	Calls        int
	PerSecond    float64
	ByKind       [CallKinds]int
	Writes       int // Put, Delete and Txn calls that write when they succeed
	Failed       int // Txns whose compares failed when recorded
	Waiting      int // calls that wait on an earlier reply
	Chain        int // the longest chain of calls that each wait on the one before
	PreloadKeys  int
	PreloadBytes int64 // keys and values
	Leases       int
	Watches      int // watches, on WatchStreams streams
	WatchStreams int
}

// Summarize counts what t holds.
func Summarize(t *Trace) Summary {
	s := Summary{Calls: len(t.Calls), Leases: len(t.Leases), WatchStreams: len(t.Watches), PreloadKeys: len(t.Preload)}
	if t.Seconds > 0 {
		s.PerSecond = float64(len(t.Calls)) / t.Seconds
	}
	depth := make([]int32, len(t.Calls))
	for i, c := range t.Calls {
		s.ByKind[c.Kind]++
		if c.Kind == CallPut || c.Kind == CallDelete || c.Kind == CallTxn && writes(&c.Op) {
			s.Writes++
		}
		if c.Failed {
			s.Failed++
		}
		if len(c.Waits) > 0 {
			s.Waiting++
		}
		for _, w := range c.Waits {
			depth[i] = max(depth[i], depth[w]+1)
		}
		s.Chain = max(s.Chain, int(depth[i])+1)
	}
	for _, it := range t.Preload {
		s.PreloadBytes += int64(len(t.Keys[it.Key])) + int64(it.Size)
	}
	for _, w := range t.Watches {
		s.Watches += len(w.Creates)
	}
	return s
}

func writes(op *Op) bool {
	switch op.Kind {
	case OpPut, OpDelete:
		return true
	case OpTxn:
		for i := range op.Success {
			if writes(&op.Success[i]) {
				return true
			}
		}
		for i := range op.Failure {
			if writes(&op.Failure[i]) {
				return true
			}
		}
	}
	return false
}

// Markdown prints the summary of t and what its conversion dropped.
func Markdown(t *Trace, dropped Dropped) string {
	s := Summarize(t)
	var b strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	p("## Trace %s\n\n%s\n\n", t.Shape, t.About)
	p("- %d calls in %.0f s: %.0f calls/s\n", s.Calls, t.Seconds, s.PerSecond)
	p("- %d write calls; %d Txns whose compares failed\n", s.Writes, s.Failed)
	p("- %d calls wait on an earlier reply; the longest chain is %d calls\n", s.Waiting, s.Chain)
	p("- preload: %d keys, %.1f MiB of keys and values, %d leases\n", s.PreloadKeys, float64(s.PreloadBytes)/(1<<20), s.Leases)
	p("- %d watches on %d streams\n\n", s.Watches, s.WatchStreams)
	p("| Call | Count | Share |\n|---|---|---|\n")
	for k, n := range s.ByKind {
		if n > 0 {
			p("| %s | %d | %.1f%% |\n", KindNames[k], n, 100*float64(n)/float64(max(s.Calls, 1)))
		}
	}
	if len(dropped) > 0 {
		p("\n| Left out or changed | Count |\n|---|---|\n")
		reasons := make([]string, 0, len(dropped))
		for r := range dropped {
			reasons = append(reasons, r)
		}
		slices.Sort(reasons)
		for _, r := range reasons {
			p("| %s | %d |\n", r, dropped[r])
		}
	}
	return b.String()
}
