"""Diagnosis-only counters; does not change a predicate or placement."""
from pathlib import Path
p = Path("check/witness_revisions.go")
s = p.read_text()
needle = "func (r *revisionOrder) readsFit(o optional) bool {\n"
assert s.count(needle) == 1
s = s.replace(needle, needle + """
	diag289Reads[o.op]++
	if c := r.w.reqs[o.op].Compact; c != nil && c.Revision <= r.w.live.compacted {
		diag289Superseded[o.op]++
	}
""")
p.write_text(s)
