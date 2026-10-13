#!/usr/bin/env python3
import unittest

from attribute import attribute, parse


class AttributionTest(unittest.TestCase):
    def test_raw_inline_frames_and_overlaps(self):
        raw = """PeriodType: cpu nanoseconds
Period: 10000000
Samples:
samples/count cpu/nanoseconds
          1   10000000: 1 2 3
          2   20000000: 4 5
Locations
     1: 0x10 M=1 internal/runtime/syscall/linux.Syscall6 /go/syscall.go:1:1 s=1
     2: 0x20 M=1 internal/poll.(*FD).Write /go/fd.go:2:1 s=1
                net.(*netFD).Write /go/netfd.go:2:1 s=1
                crypto/tls.(*Conn).writeRecordLocked /go/tls.go:3:1 s=1
     3: 0x30 M=1 github.com/djosh34/tigerstore/internal/peer.(*sender).write /peer.go:4:1 s=1
     4: 0x40 M=1 runtime.memmove /memmove.s:1:1 s=1
     5: 0x50 M=1 github.com/djosh34/tigerstore/internal/core.encodeBatch /codec.go:1:1 s=1
Mappings
1: 0x0/0xff/0x0 /binary
"""
        samples = parse(raw)
        result = attribute(samples, 1000)
        self.assertEqual(result["sampled_cpu_us_put"], 30)
        self.assertEqual(result["inclusive"]["TLS with socket descendants"]["cpu_us_put"], 10)
        self.assertEqual(result["inclusive"]["TLS without socket syscall samples"]["cpu_us_put"], 0)
        self.assertEqual(result["partition"]["peer send"]["cpu_us_put"], 10)
        self.assertEqual(result["partition"]["entry encode"]["cpu_us_put"], 20)
        self.assertEqual(sum(r["sample_percent"] for r in result["partition"].values()), 100)

    def test_cpu_not_wall_and_missing_category_is_zero(self):
        result = attribute([(10_000_000, "runtime.gcBgMarkWorker")], 100)
        self.assertEqual(result["sampled_cpu_us_put"], 100)
        self.assertEqual(result["inclusive"]["file fsync"]["cpu_us_put"], 0)
        self.assertEqual(result["partition"]["runtime GC"]["cpu_us_put"], 100)

    def test_file_syscall_not_socket(self):
        result = attribute([(10_000_000, "internal/runtime/syscall/linux.Syscall6\ninternal/poll.(*FD).Write\nos.(*File).Write\ninternal/wal.(*WAL).Flush")], 100)
        self.assertEqual(result["inclusive"]["socket syscall samples"]["cpu_us_put"], 0)
        self.assertEqual(result["inclusive"]["file write under WAL"]["cpu_us_put"], 100)

    def test_empty_profile_rejected(self):
        with self.assertRaises(ValueError):
            parse("Samples:\nLocations\nMappings\n")


if __name__ == "__main__":
    unittest.main()
