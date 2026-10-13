#!/usr/bin/env python3
"""Read archived pprof -raw output. Inclusive rows overlap; partitions do not."""
import argparse
import collections
import json
from pathlib import Path
import re

# Match function names in all locations, including inlined stack frames.
MECHANISMS = {
    "TLS with socket descendants": r"crypto/tls\.",
    "TLS without socket syscall samples": r"crypto/tls\.",
    "crypto implementation": r"crypto/(internal|aes|cipher|sha|chacha|poly)",
    "socket syscall samples": r"internal/poll\.\(\*FD\)\.(Read|Write|Writev)",
    "all syscall samples": r"internal/runtime/syscall|syscall\.(Syscall|RawSyscall)",
    "HTTP2 and HPACK": r"golang.org/x/net/http2|internal/server\.frameHeader",
    "entry encode": r"internal/core\.encodeBatch",
    "entry walk and parse": r"internal/core\.(walkBatch|ParsePut|UnmarshalAlias)",
    "raft": r"go.etcd.io/raft/v3\.",
    "peer send": r"internal/peer\.\(\*sender\)\.(send|write|dial)",
    "peer receive": r"internal/peer\.\(\*TCP\)\.receive",
    "WAL total": r"internal/wal\.",
    "file write under WAL": r"os\.\(\*File\)\.Write",
    "file fsync": r"os\.\(\*File\)\.Sync|syscall\.Fsync",
    "apply": r"internal/core\.\(\*Node\)\.apply",
    "MVCC and index": r"internal/mvcc\.|github.com/google/btree",
    "reply encode and queue": r"internal/server\.\(\*conn\)\.(complete|appendPutReply)|internal/server\.appendPutResponse",
    "frontend writer including TLS": r"internal/server\.\(\*conn\)\.writeLoop",
    "frontend reader including TLS": r"internal/server\.\(\*conn\)\.readLoop",
    "allocation and GC": r"runtime\.(mallocgc|gc[A-Z]|scan|sweep|mark|newobject|makeslice|growslice)",
    "scheduler": r"runtime\.(schedule|findRunnable|park_m|mcall|netpoll)",
}
# Assign each sample to the first matching context. This ranks execution
# contexts, not causal cost or the critical path of one request.
CONTEXTS = {
    "WAL fsync": r"os\.\(\*File\)\.Sync|syscall\.Fsync",
    "WAL other": r"internal/wal\.",
    "peer send": r"internal/peer\.\(\*sender\)\.(send|write|dial)",
    "peer receive": r"internal/peer\.\(\*TCP\)\.receive",
    "frontend reply and writer": r"internal/server\.\(\*conn\)\.(writeLoop|complete|appendPutReply)|internal/server\.appendPutResponse",
    "frontend reader": r"internal/server\.\(\*conn\)\.readLoop",
    "apply": r"internal/core\.\(\*Node\)\.apply",
    "entry encode": r"internal/core\.encodeBatch",
    "raft": r"go.etcd.io/raft/v3\.",
    "core and driver other": r"internal/core\.|internal/node\.",
    "frontend other": r"internal/server\.",
    "runtime GC": r"runtime\.(gc[A-Z]|scan|sweep|mark|bgsweep|bgscavenge)",
    "runtime scheduler": r"runtime\.(schedule|findRunnable|park_m|mcall|netpoll)",
}
SYSCALL = re.compile(MECHANISMS["all syscall samples"])
SOCKET = re.compile(MECHANISMS["socket syscall samples"])


def parse(text):
    samples_text, rest = text.split("Locations\n", 1)
    locations_text = rest.split("Mappings\n", 1)[0]
    locations = {}
    current = None
    for line in locations_text.splitlines():
        match = re.match(r"\s*(\d+): ", line)
        if match:
            current = int(match[1])
            locations[current] = line
        elif current is not None:
            locations[current] += "\n" + line
    samples = []
    for line in samples_text.splitlines():
        match = re.match(r"^\s*(\d+)\s+(\d+):\s+([\d ]+)(?:\s.*)?$", line)
        if match:
            samples.append((int(match[2]), "\n".join(locations[int(i)] for i in match[3].split())))
    if not samples:
        raise ValueError("no CPU samples")
    return samples


def attribute(samples, operations):
    total = sum(value for value, _ in samples)
    mechanisms = collections.Counter()
    contexts = collections.Counter()
    compiled = {name: re.compile(pattern) for name, pattern in MECHANISMS.items()}
    partition = {name: re.compile(pattern) for name, pattern in CONTEXTS.items()}
    for value, stack in samples:
        syscall = bool(SYSCALL.search(stack))
        socket_syscall = syscall and bool(SOCKET.search(stack))
        for name, pattern in compiled.items():
            if not pattern.search(stack):
                continue
            if name == "socket syscall samples" and not socket_syscall:
                continue
            if name == "TLS without socket syscall samples" and socket_syscall:
                continue
            if name == "file write under WAL" and "internal/wal." not in stack:
                continue
            mechanisms[name] += value
        for name, pattern in partition.items():
            if pattern.search(stack):
                contexts[name] += value
                break
        else:
            contexts["other"] += value
    assert sum(contexts.values()) == total

    def rows(values, names):
        return {name: {"cpu_us_put": values[name] / 1000 / operations,
                       "sample_percent": values[name] * 100 / total,
                       "sample_cpu_ns": values[name]}
                for name in names}

    return {"operations": operations, "sampled_cpu_us_put": total / 1000 / operations,
            "sample_cpu_ns": total,
            "inclusive": rows(mechanisms, MECHANISMS),
            "partition": rows(contexts, list(CONTEXTS) + ["other"])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path,
                        help="JSON list of {clients,variant,rep,role,operations,raw} records; raw paths relative to manifest")
    args = parser.parse_args()
    records = json.loads(args.manifest.read_text())
    groups = collections.defaultdict(lambda: {"samples": [], "operations": 0, "profiles": []})
    per_profile = []
    for record in records:
        samples = parse((args.manifest.parent / record["raw"]).read_text())
        operations = record["operations"]
        if operations <= 0:
            raise ValueError("nonpositive operation count")
        per_profile.append(record | attribute(samples, operations))
        key = f'{record["clients"]}/{record["variant"]}/{record["role"]}'
        group = groups[key]
        group["samples"].extend(samples)
        group["operations"] += operations
        group["profiles"].append(record["raw"])
    aggregate = {key: attribute(group["samples"], group["operations"]) | {"profiles": group["profiles"]}
                 for key, group in groups.items()}
    print(json.dumps({"method": "CPU sample nanoseconds divided by completed Puts. Inclusive rows overlap. The ordered context partition sums to total.",
                      "aggregate": aggregate, "profiles": per_profile}, indent=2))


if __name__ == "__main__":
    main()
