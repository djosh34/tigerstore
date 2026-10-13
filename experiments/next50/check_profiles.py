#!/usr/bin/env python3
"""Check pprof's actual sampler timestamps, not HTTP request/response bounds."""
import argparse
import datetime
import gzip
import json
from pathlib import Path


def varint(data, pos):
    value = 0
    for shift in range(0, 70, 7):
        if pos >= len(data):
            raise ValueError("truncated varint")
        b = data[pos]
        pos += 1
        value |= (b & 127) << shift
        if b < 128:
            return value, pos
    raise ValueError("oversized varint")


def bounds(data):
    # profile.proto fields 9 and 10 are time_nanos and duration_nanos.
    # Skip other top-level fields without interpreting samples or locations.
    pos = 0
    found = {}
    while pos < len(data):
        tag, pos = varint(data, pos)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = varint(data, pos)
            if field in (9, 10):
                found[field] = value
        elif wire == 1:
            pos += 8
        elif wire == 2:
            size, pos = varint(data, pos)
            pos += size
        elif wire == 5:
            pos += 4
        else:
            raise ValueError(f"unexpected profile wire type {wire}")
        if pos > len(data):
            raise ValueError("truncated profile field")
    if found.get(9, 0) <= 0 or found.get(10, 0) <= 0:
        raise ValueError("missing profile timestamps")
    return found[9], found[9] + found[10]


def check(root):
    manifest = json.loads((root / "profile-manifest.json").read_text())
    if len(manifest) != 15:
        raise ValueError(f"expected five profiles of each of three nodes, got {len(manifest)}")
    records = []
    for record in manifest:
        folder = (root / record["raw"]).parent
        raw_name = Path(record["raw"]).name
        profile = folder / raw_name.replace("-cpu-raw.txt", "-cpu.pb.gz")
        start, end = bounds(gzip.decompress(profile.read_bytes()))
        put = json.loads((folder / "put.json").read_text())
        load_start = datetime.datetime.fromisoformat(put["start_utc"].replace("Z", "+00:00")).timestamp() * 1e9
        load_end = load_start + put["elapsed_s"] * 1e9
        if not start <= load_start < load_end <= end:
            raise ValueError(f"load exceeds actual sampler bounds: {profile}")
        records.append(record | {"sampler_start_ns": start, "sampler_end_ns": end,
                                 "sampler_seconds": (end - start) / 1e9,
                                 "lead_seconds": (load_start - start) / 1e9,
                                 "tail_seconds": (end - load_end) / 1e9})
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(args.root), indent=2))
