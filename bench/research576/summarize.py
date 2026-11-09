#!/usr/bin/env python3
"""Aggregate key=value C bench records to CSV; median/min/max, never pooled reps.
Usage: python3 bench/research576/summarize.py /path/to/artifact/research576 > summary.csv
"""
import csv
import pathlib
import statistics
import sys
from collections import defaultdict

root = pathlib.Path(sys.argv[1])
records = defaultdict(list)
keys = {
    "NET": ("mode", "size", "batch", "flags", "tls", "rx"),
    "DISK": ("size", "mode", "direct"),
    "AES": ("mode", "bits", "size"),
    "NOP": ("batch",),
    "GETPID": (),
}
files = sorted(root.glob("io-*.txt")) + sorted(root.glob("*-cpu-*.txt"))
for path in files:
    for line in path.read_text().splitlines():
        kind = line.split(" ")[0]
        if kind not in keys:
            continue
        fields = dict(item.split("=", 1) for item in line.split()[1:] if "=" in item)
        if kind == "NET":
            fields["total_ns"] = str(float(fields["tx_ns"]) + float(fields["rx_ns"]))
            fields["tx_ns_byte"] = str(float(fields["tx_ns"]) / int(fields["size"]))
        key = (path.name.rsplit("-", 1)[0], kind) + tuple(fields[k] for k in keys[kind])
        records[key].append(fields)
writer = csv.writer(sys.stdout)
writer.writerow(["source", "kind", "case", "metric", "n", "median", "min", "max"])
for key, reps in records.items():
    for metric in reps[0]:
        if metric in keys[key[1]] or metric in ("messages", "groups", "tx_pid", "rx_pid"):
            continue
        try:
            values = [float(rep[metric]) for rep in reps]
        except (ValueError, KeyError):
            continue
        writer.writerow([key[0], key[1], "/".join(key[2:]), metric, len(values),
                         statistics.median(values), min(values), max(values)])
