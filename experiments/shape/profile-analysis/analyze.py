#!/usr/bin/env python3
"""Offline artifact analysis only. Never execute artifact binaries or collect profiles."""
import argparse
import collections
import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

APP = "github.com/djosh34/tigerstore/internal/"
OSFS = APP + "osfs."
ENV = dict(os.environ, GOFLAGS="-p=2", GOMEMLIMIT="2GiB")
ROOTS = [
    ("node.(*Node).persist", "WAL worker"),
    ("node.(*Node).commit", "owner"),
    ("server.(*conn).readLoop", "frontend reader"),
    ("server.(*conn).writeLoop", "frontend writer"),
    ("peer.(*sender).run", "peer writer"),
    ("peer.(*TCP).receive", "peer reader"),
]


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def sync_kind(names):
    # Check directory helpers first. os.File.Sync alone cannot identify file type.
    if "syscall.Fsync" not in names and "os.(*File).Sync" not in names:
        return None
    if any(n == OSFS + "syncPath" or
           (n.startswith(OSFS) and n.endswith(".SyncDir")) for n in names):
        return "directory_fsync"
    if any(n.startswith(OSFS) and "observedFile" in n and
           n.endswith(".Sync") for n in names):
        return "observed_file_fsync"
    return "unclassified_fsync"


def write_kind(names):
    if "os.(*File).Write" not in names:
        return None
    if any(n.startswith(OSFS) and "observedFile" in n and
           n.endswith(".Write") for n in names):
        return "observed_file_write"
    return "unclassified_file_write"


def context(names):
    if any(".publishObservation" in n or ".sampleObservation" in n or
           n == APP + "core.(*Node).Observe" for n in names):
        return "observation"
    for root, tag in ROOTS:
        if APP + root in names:
            return tag
    if any(n.startswith("net/http.") or n.startswith("main.shape") for n in names):
        return "debug HTTP"
    if any(n.startswith("runtime.") for n in names):
        return "runtime background"
    return "other"


def subject_context(names):
    # A sampling call on the owner does not change that goroutine's stable role.
    for root, tag in ROOTS:
        if APP + root in names:
            return tag
    role = context(names)
    return role if role not in ["runtime background", "other"] else None


def mechanisms(names):
    out = []
    for package in ["core", "mvcc", "wal"]:
        if any(n.startswith(APP + package + ".") for n in names):
            out.append({"wal": "WAL"}.get(package, package))
    if any(n.startswith("go.etcd.io/raft/v3.") for n in names):
        out.append("raft")
    if APP + "core.encodeBatch" in names or APP + "core.walkBatch" in names:
        out.append("entry codec")
    if any(n.startswith("crypto/tls.") for n in names):
        out.append("TLS")
    if any(n.startswith("golang.org/x/net/http2.") or
           n.startswith("golang.org/x/net/http2/hpack.") for n in names):
        out.append("HTTP2/HPACK")
    if any(n in ["net.(*netFD).Read", "net.(*netFD).Write"] for n in names):
        out.append("socket calls")
    if any(n.startswith(("runtime.mallocgc", "runtime.gc", "runtime.newobject",
                         "runtime.growslice", "runtime.makeslice")) for n in names):
        out.append("alloc/GC")
    if any(n.startswith("google.golang.org/protobuf.") for n in names):
        out.append("protobuf")
    if "runtime.memmove" in names:
        out.append("memmove")
    for kind in [sync_kind(names), write_kind(names)]:
        if kind:
            out.append(kind)
    return out


def frame_name(text):
    # Preserve spaces inside generic function names and inline frames.
    m = re.match(r"^(.*)\s+\S+:\d+:\d+(?:\s+s=\d+)?$", text.strip())
    return m[1] if m else text.strip()


def parse_pprof(raw):
    locations = {}
    samples = []
    state = "header"
    current = None
    for line in raw.splitlines():
        if line in ["Locations", "Mappings"]:
            state = line.lower()
            continue
        if line == "Samples:":
            state = "samples"
            continue
        if state == "samples":
            # Empty stacks are real samples and must remain in totals.
            m = re.fullmatch(r"\s*(\d+)\s+(\d+):\s*([\d ]*)", line)
            if m:
                samples.append({"count": int(m[1]), "value_ns": int(m[2]),
                                "locations": list(map(int, m[3].split()))})
        elif state == "locations":
            m = re.match(r"\s*(\d+): 0x[\da-f]+(?: M=\d+)?(?:\s+(.*))?$", line)
            if m:
                current = int(m[1])
                locations[current] = [frame_name(m[2])] if m[2] else []
            elif line.strip() and current is not None:
                locations[current].append(frame_name(line))
    if not samples:
        raise ValueError("No samples parsed from pprof output")
    return samples, locations


def stack_names(sample, locations):
    return [name for loc in sample["locations"] for name in locations[loc]]


def rfc3339_ns(text):
    fraction = re.search(r"\.(\d+)", text)
    nanos = int((fraction[1] if fraction else "").ljust(9, "0")[:9])
    whole = re.sub(r"\.\d+", "", text)
    return int(datetime.datetime.fromisoformat(whole).timestamp()) * 1_000_000_000 + nanos


class Analysis:
    def __init__(self, root, output):
        self.root = root
        self.output = output
        self.manifest = {"inputs": {}, "commands": [], "go_environment": {
            "GOFLAGS": ENV["GOFLAGS"], "GOMEMLIMIT": ENV["GOMEMLIMIT"]},
            "analysis_script_sha256": sha256(Path(__file__)),
            "go_version": subprocess.check_output(["go", "version"], env=ENV,
                                                  text=True).strip()}

    def input(self, path):
        key = str(path.relative_to(self.root))
        if key not in self.manifest["inputs"]:
            self.manifest["inputs"][key] = {"sha256": sha256(path),
                                           "bytes": path.stat().st_size}
        return path

    def load_json(self, path):
        return json.loads(self.input(path).read_text())

    def raw_profile(self, mode, cell, binary, profile):
        self.input(binary)
        self.input(profile)
        argv = ["go", "tool", "pprof", "-raw", str(binary), str(profile)]
        self.manifest["commands"].append(argv)
        raw = subprocess.check_output(argv, env=ENV, text=True)
        path = self.output / "raw" / mode / cell / (profile.name + ".txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(raw)
        return raw, str(path.relative_to(self.output))

    def cpu(self, cell, binary, base, load):
        success = load["shape"]["success"]
        nodes = []
        for slot in range(3):
            raw, raw_path = self.raw_profile("cpu", cell, binary,
                                             base / f"n{slot}-cpu.pb.gz")
            samples, locations = parse_pprof(raw)
            contexts = collections.Counter()
            inclusive = collections.Counter()
            for sample in samples:
                names = stack_names(sample, locations)
                sample["context"] = context(names)
                sample["mechanisms"] = mechanisms(names)
                contexts[sample["context"]] += sample["value_ns"]
                for tag in sample["mechanisms"]:
                    inclusive[tag] += sample["value_ns"]
            total = sum(s["value_ns"] for s in samples)
            assert sum(contexts.values()) == total
            assert all(s["value_ns"] == s["count"] * 10_000_000 for s in samples)
            nodes.append({"slot": slot, "raw_profile": raw_path, "samples": samples,
                          "total_cpu_ns": total, "contexts_cpu_ns": dict(contexts),
                          "inclusive_cpu_ns": dict(inclusive),
                          "sampled_us_per_success": total / (1000 * success),
                          "contexts_us_per_success": {
                              k: v / (1000 * success) for k, v in contexts.items()},
                          "inclusive_us_per_success": {
                              k: v / (1000 * success) for k, v in inclusive.items()}})
        return {"success": success, "nodes": nodes}

    def trace(self, cell, binary, base, load):
        profiles = {}
        for kind in ["syscall", "sched", "sync", "net"]:
            raw, raw_path = self.raw_profile("trace", cell, binary,
                                             base / f"n0-run.trace.{kind}.pb.gz")
            samples, locations = parse_pprof(raw)
            aggregates = collections.defaultdict(lambda: {"count": 0, "value_ns": 0})
            for sample in samples:
                names = stack_names(sample, locations)
                tags = ["total"] + (["empty_stack"] if not names else [])
                if kind == "syscall":
                    tags += [t for t in [sync_kind(names), write_kind(names)] if t]
                for tag in tags:
                    aggregates[tag]["count"] += sample["count"]
                    aggregates[tag]["value_ns"] += sample["value_ns"]
            profiles[kind] = {"raw_profile": raw_path, "aggregates": dict(aggregates)}
        states = self.trace_states(base / "n0-run.trace", load)
        # Match completed classified spans against the runner's syscall profile.
        for tag in ["observed_file_fsync", "directory_fsync", "unclassified_fsync",
                    "observed_file_write", "unclassified_file_write"]:
            expected = profiles["syscall"]["aggregates"].get(tag, {"count": 0, "value_ns": 0})
            actual = states["completed_syscall_summaries"][tag]
            assert (actual["count"], actual["sum_ns"]) == (expected["count"], expected["value_ns"]), tag
        return {"success": load["shape"]["success"], "profiles": profiles, **states}

    def trace_states(self, path, load):
        self.input(path)
        argv = ["go", "tool", "trace", "-d=parsed", str(path)]
        self.manifest["commands"].append(argv)
        proc = subprocess.Popen(argv, env=ENV, stdout=subprocess.PIPE, text=True)
        current = {}
        classes = {}
        roots = {}
        intervals = collections.defaultdict(collections.Counter)
        syscall_starts = {}
        completed = []
        stacks = []
        stack_ids = {}
        start = end = wall = sync_trace = None
        events = 0

        def handle(lines):
            nonlocal start, end, wall, sync_trace, events
            if not lines:
                return
            events += 1
            header = lines[0]
            tm = re.search(r"Time=(\d+)", header)
            if tm:
                tm = int(tm[1])
                start = tm if start is None else min(start, tm)
                end = tm if end is None else max(end, tm)
            if " Sync " in header and wall is None:
                wm = re.search(r"Wall=(\S+)", header)
                trace = re.search(r"Trace=(\d+)", header)
                if wm and trace:
                    wall, sync_trace = rfc3339_ns(wm[1]), int(trace[1])
            m = re.search(r'StateTransition Time=(\d+) GoID=(\d+) (\w+)->(\w+) Reason="([^"]*)"', header)
            if not m:
                return
            t, gid, old, new, reason = int(m[1]), int(m[2]), m[3], m[4], m[5]
            names = []
            section = None
            for line in lines[1:]:
                if line.startswith("TransitionStack="):
                    section = "subject"
                elif line.startswith("Stack="):
                    section = "event"
                elif section == "subject" and " @ " in line:
                    names.append(line.strip().split(" @ ")[0])
            # Only application roots or HTTP identify stable subject roles.
            # Never use the event's Stack or G as the blocked subject's identity.
            role = subject_context(names)
            if role is not None:
                classes[gid] = role
                roots[gid] = names
            if gid in current:
                since, state, why = current[gid]
                assert t >= since
                key = state + (":" + why if state == "Waiting" else "")
                intervals[gid][key] += t - since
            current[gid] = (t, new, reason)
            if new == "Syscall" and old != "Syscall":
                tag = sync_kind(names) or write_kind(names)
                syscall_starts[gid] = (t, tag, names)
            if old == "Syscall" and new != "Syscall" and gid in syscall_starts:
                since, tag, entry_names = syscall_starts.pop(gid)
                if tag:
                    key = tuple(entry_names)
                    if key not in stack_ids:
                        stack_ids[key] = len(stacks)
                        stacks.append(entry_names)
                    completed.append({"go_id": gid, "start_trace_ns": since,
                                      "end_trace_ns": t, "duration_ns": t - since,
                                      "kind": tag, "entry_stack_id": stack_ids[key]})

        record = []
        try:
            for line in proc.stdout:
                if line.startswith("M="):
                    handle(record)
                    record = [line.rstrip()]
                else:
                    record.append(line.rstrip())
            handle(record)
        except BaseException:
            proc.kill()
            proc.wait()
            raise
        finally:
            proc.stdout.close()
        if proc.wait() != 0:
            raise RuntimeError("go tool trace failed")
        if start is None or end is None or wall is None:
            raise ValueError("Trace lacks timestamps or wall synchronization")
        for gid, (since, state, why) in current.items():
            key = state + (":" + why if state == "Waiting" else "")
            intervals[gid][key] += end - since
        by_context = collections.defaultdict(collections.Counter)
        goroutines = {}
        for gid, counts in intervals.items():
            role = classes.get(gid, "other")
            by_context[role].update(counts)
            goroutines[gid] = {"context": role, "subject_root_stack": roots.get(gid, []),
                               "state_wall_ns": dict(counts)}
        summaries = {}
        for tag in ["observed_file_fsync", "directory_fsync", "unclassified_fsync",
                    "observed_file_write", "unclassified_file_write"]:
            durations = sorted(s["duration_ns"] for s in completed if s["kind"] == tag)
            summaries[tag] = {"count": len(durations), "sum_ns": sum(durations),
                              "median_ns": durations[len(durations) // 2] if durations else None,
                              "p99_ns": durations[len(durations) * 99 // 100] if durations else None,
                              "max_ns": durations[-1] if durations else None}
        return {"parsed_event_count": events, "start_trace_ns": start, "end_trace_ns": end,
                "capture_ns": end - start,
                "capture_start_offset_from_cohort_ns": wall + start - sync_trace - load["shape"]["start_unix_ns"],
                "goroutines": goroutines,
                "context_state_wall_ns": {k: dict(v) for k, v in by_context.items()},
                "completed_syscall_entry_stacks": stacks,
                "completed_syscalls": completed, "completed_syscall_summaries": summaries}

    def counters(self, base, load):
        before, after = [self.load_json(base / name) for name in ["before.json", "after.json"]]
        nodes = []
        for a, b in zip(before["nodes"], after["nodes"]):
            fields = {}
            for kind in ["write", "fsync"]:
                fields[kind] = {k: b["files"][kind][k] - a["files"][kind][k]
                                for k in ["calls", "errors", "bytes", "wall_ns_sum"]}
            nodes.append(fields)
        samples = []
        for line in self.input(base / "samples.jsonl").read_text().splitlines():
            s = json.loads(line)
            if load["shape"]["start_unix_ns"] <= s["wall_unix_ns"] < load["shape"]["arrival_stop_unix_ns"]:
                samples.append(s)
        full = collections.Counter()
        max_inflight = 0
        queue_max = collections.Counter()
        for s in samples:
            node = s["nodes"][0]["node"]
            for key in ["inbox_events", "queued_writes", "pending_proposals",
                        "persist_backlog_batches", "persist_queue_batches"]:
                queue_max[key] = max(queue_max[key], node[key])
            for r in node["raft_progress"]:
                if r["id"] == node["leader"]:
                    continue
                max_inflight = max(max_inflight, r["inflight_count"])
                if r["inflight_full"]:
                    full[r["inflight_count"]] += 1
        return {"file_only_fenced_deltas": nodes, "arrival_sampling_polls": len(samples),
                "leader_queue_maxima": dict(queue_max), "max_follower_inflight_count": max_inflight,
                "full_observations_by_inflight_count": dict(full)}

    def run(self):
        result = {"schema_version": 1, "cpu": {}, "trace": {}}
        for mode in ["cpu", "trace"]:
            for artifact in sorted((self.root / mode).glob("shape-*")):
                cell = artifact.name.removeprefix("shape-")
                base = artifact / cell / "tigerstore-1"
                binary = artifact / "binaries/tigerstore"
                load = self.load_json(base / "load.json")
                audit = self.load_json(base / "audit.json")
                assert load["valid"] and audit["valid"] and not load["errors"]
                assert not audit["sampling_errors"] and not audit["invariant_errors"]
                data = self.cpu(cell, binary, base, load) if mode == "cpu" else self.trace(cell, binary, base, load)
                data["arrival_window_ns"] = load["shape"]["arrival_stop_unix_ns"] - load["shape"]["start_unix_ns"]
                data["audit"] = audit
                data["binary_sha256"] = sha256(binary)
                data["counters"] = self.counters(base, load)
                result[mode][cell] = data
        hashes = {v["binary_sha256"] for mode in ["cpu", "trace"] for v in result[mode].values()}
        assert len(hashes) == 1, "Artifact binaries differ; report identity separately"
        self.output.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(result, indent=2, sort_keys=True) + "\n").encode()
        (self.output / "computed.json.gz").write_bytes(gzip.compress(payload, mtime=0))
        (self.output / "input-manifest.json").write_text(json.dumps(self.manifest, indent=2, sort_keys=True) + "\n")
        (self.output / "computed-tables.md").write_text(tables(result))


def tables(data):
    lines = ["# Offline computed tables", "", "Integer nanoseconds and per-sample attribution are in `computed.json.gz`. Raw pprof outputs are in `raw/`. File fsync requires the observed-file wrapper and excludes directory and unclassified syncs.", ""]

    def table(title, headers, rows):
        lines.extend(["## " + title, "", "| " + " | ".join(headers) + " |",
                      "| " + " | ".join(["---"] * len(headers)) + " |"])
        lines.extend("| " + " | ".join(map(str, row)) + " |" for row in rows)
        lines.append("")

    rows = []
    for cell, case in data["cpu"].items():
        n = case["nodes"][0]
        c = n["contexts_us_per_success"]
        selected = ["frontend reader", "frontend writer", "owner", "WAL worker", "peer writer", "peer reader"]
        other = sum(v for k, v in c.items() if k not in selected)
        rows.append([cell, case["success"]] + [f"{c.get(k, 0):.3f}" for k in selected] +
                    [f"{other:.3f}", f"{n['sampled_us_per_success']:.3f}"])
    table("Disjoint leader CPU us per success", ["Cell", "Success", "Reader", "Writer", "Owner", "WAL", "Peer writer", "Peer reader", "Other", "Total"], rows)
    rows = []
    tags = ["TLS", "socket calls", "HTTP2/HPACK", "core", "raft", "mvcc", "WAL", "observed_file_fsync", "directory_fsync", "unclassified_fsync", "alloc/GC", "memmove", "entry codec"]
    for cell, case in data["cpu"].items():
        inc = case["nodes"][0]["inclusive_us_per_success"]
        rows.append([cell] + [f"{inc.get(k, 0):.3f}" for k in tags])
    table("Inclusive leader CPU us per success, overlapping", ["Cell"] + tags, rows)
    table("Sampled CPU us per success by node slot", ["Cell", "Leader", "Follower1", "Follower2"],
          [[cell] + [f"{n['sampled_us_per_success']:.3f}" for n in case["nodes"]] for cell, case in data["cpu"].items()])
    rows = []
    for cell, case in data["trace"].items():
        s = case["context_state_wall_ns"]
        fields = [("owner", "Runnable"), ("owner", "Waiting:select"), ("WAL worker", "Runnable"), ("WAL worker", "Syscall"), ("frontend reader", "Runnable"), ("frontend writer", "Runnable")]
        rows.append([cell, f"{case['capture_ns']/1e9:.6f}"] + [f"{s.get(c, {}).get(k, 0)/1e9:.6f}" for c, k in fields])
    table("Subject Go-state wall seconds", ["Cell", "Capture", "Owner runnable", "Owner select", "WAL runnable", "WAL syscall", "Readers runnable", "Writers runnable"], rows)
    rows = []
    for cell, case in data["trace"].items():
        for kind, s in case["completed_syscall_summaries"].items():
            rows.append([cell, kind, s["count"], f"{s['sum_ns']/1e9:.9f}"] +
                        [f"{s[k]/1e6:.6f}" if s[k] is not None else "none" for k in ["median_ns", "p99_ns", "max_ns"]])
    table("Completed syscall spans, file and directory separated", ["Cell", "Kind", "Count", "Sum s", "Median ms", "p99 ms", "Max ms"], rows)
    rows = []
    for mode in ["cpu", "trace"]:
        for cell, case in data[mode].items():
            f = case["counters"]["file_only_fenced_deltas"][0]
            rows.append([mode, cell, case["success"], f["fsync"]["calls"], f["write"]["calls"],
                         f"{case['success']/f['fsync']['calls']:.6f}" if f["fsync"]["calls"] else "none",
                         f"{f['write']['bytes']/f['write']['calls']/1024:.6f}" if f["write"]["calls"] else "none",
                         f"{f['fsync']['wall_ns_sum']/1e9:.9f}", f"{f['write']['wall_ns_sum']/1e9:.9f}"])
    table("Whole-cohort fenced file-only leader counters", ["Mode", "Cell", "Success", "Fsyncs", "Writes", "Success/fsync", "Mean write KiB", "Fsync wall s", "Write wall s"], rows)
    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root, output = args.results_root.resolve(), args.output_dir.resolve()
    if output == root or root in output.parents:
        parser.error("Output must be outside the read-only input artifact tree")
    Analysis(root, output).run()


if __name__ == "__main__":
    main()
