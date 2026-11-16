#!/usr/bin/env python3
# Measure only: folds `perf script` output into "root;...;leaf count" lines,
# offsets dropped, for any subtree's cost to be summed later.
import collections, re, sys
counts = collections.Counter()
stack = []
for line in sys.stdin:
    if not line.strip():
        if stack:
            counts[";".join(reversed(stack))] += 1
        stack = []
        continue
    if line[0] in " \t":
        m = re.match(r"\s+[0-9a-f]+\s+(.+?)\s+\((.*)\)\s*$", line)
        if m:
            sym = re.sub(r"\+0x[0-9a-f]+$", "", m.group(1))
            if sym == "[unknown]":
                sym = "[" + m.group(2).split("/")[-1] + "]"
            stack.append(sym)
    else:
        stack = [] if not line.startswith(" ") else stack
        head = line.split()
        comm = head[0] if head else "?"
        stack = []
        pending_comm = comm
if stack:
    counts[";".join(reversed(stack))] += 1
for k, v in counts.most_common():
    print(f"{k} {v}")
