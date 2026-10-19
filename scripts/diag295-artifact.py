#!/usr/bin/env python3
"""Check the WAL bytes saved by run 39316154692, seed 20233900302.

Download chaos-campaign-lazyfs-4, then pass its directory to this script.
This reads the evidence without changing it.
"""
import struct
import sys
from pathlib import Path


def crc32c(data, seed=0):
    crc = seed ^ 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def records(data, seq, crc):
    pos = 24
    bad = []
    count = 0
    while pos + 9 <= len(data):
        size, stored = struct.unpack_from("<II", data, pos)
        end = pos + 9 + size
        if end > len(data):
            break
        computed = crc32c(data[pos + 8:end], crc32c(seq.to_bytes(8, "little"), crc))
        if stored != computed:
            bad.append(pos)
        crc = stored
        pos = end
        count += 1
    return crc, pos, count, bad


root = Path(sys.argv[1]) / "seed-20233900302" / "data"
node3 = bytearray((root / "node3/wal/0000000000000001.wal").read_bytes())
_, end, count, bad = records(node3, 1, 0)
assert len(node3) == 33116 and bad == [16352]
print(f"Node 3: {len(node3)} bytes, {count} complete records, bad record at {bad}.")
at = len(node3) // 2
node3[at] ^= 0x10
_, end, count, bad = records(node3, 1, 0)
assert end == len(node3) and not bad
print(f"Undo flipByte at byte {at}: all {count} record checksums verify, no other byte changed.")

previous = (root / "node2/wal/000000000000000e.wal").read_bytes()
crc, end, _, bad = records(previous, 14, int.from_bytes(previous[20:24], "little"))
assert end == len(previous) and not bad
node2 = (root / "node2/wal/000000000000000f.wal").read_bytes()
assert node2[:24] == bytes(24)
_, end, count, bad = records(node2, 15, crc)
assert not bad and count == 26 and end == 24399 and len(node2) == 25351
print(f"Node 2: 24 zero header bytes, then {count} valid records chained from segment 14, then a torn tail at {end}.")
