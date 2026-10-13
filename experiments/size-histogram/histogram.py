#!/usr/bin/env python3
"""Summarize length-only CSV. Exact size buckets permit empirical resampling."""
import collections
import csv
import pathlib
import sys

FIELDS = ["row", "resource", "count", "value_bytes", "object_share", "value_byte_share",
          "min", "p10", "p50", "p90", "p99", "max", "size_bytes", "resource_bucket_share"]


def quantile(counts, percent):
    # Nearest rank, calculated from integer counts without floating-point error.
    rank = (sum(counts.values()) * percent + 99) // 100
    cumulative = 0
    for size, count in sorted(counts.items()):
        cumulative += count
        if cumulative >= rank:
            return size
    raise ValueError("empty histogram")


def rows(hist):
    total_count = sum(sum(c.values()) for c in hist.values())
    total_bytes = sum(sum(size * n for size, n in c.items()) for c in hist.values())
    if not total_count:
        raise ValueError("empty inventory")
    for resource, counts in sorted(hist.items()):
        count = sum(counts.values())
        value_bytes = sum(size * n for size, n in counts.items())
        yield dict(row="summary", resource=resource, count=count, value_bytes=value_bytes,
                   object_share=f"{count / total_count:.12f}",
                   value_byte_share=f"{value_bytes / total_bytes:.12f}" if total_bytes else "0",
                   min=min(counts), max=max(counts),
                   **{f"p{p}": quantile(counts, p) for p in [10, 50, 90, 99]})
        for size, n in sorted(counts.items()):
            yield dict(row="bucket", resource=resource, count=n, value_bytes=size * n,
                       size_bytes=size, resource_bucket_share=f"{n / count:.12f}")


def main(source, dest):
    hist = collections.defaultdict(collections.Counter)
    with open(source, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != ["resource", "value_bytes"]:
            raise ValueError("unexpected length schema")
        for entry in reader:
            # The exporter only emits allowlisted names, never key paths.
            resource = entry["resource"]
            if not resource or not all(c.isalpha() for c in resource):
                raise ValueError("invalid resource label")
            size = int(entry["value_bytes"])
            if size < 0:
                raise ValueError("negative size")
            hist[resource][size] += 1
    output = list(rows(hist))
    temp = pathlib.Path(str(dest) + ".tmp")
    with temp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(output)
    temp.replace(dest)


if __name__ == "__main__":
    main(*sys.argv[1:])
