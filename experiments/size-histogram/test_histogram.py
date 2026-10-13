import collections
import csv
import pathlib
import tempfile
import unittest

import histogram


class HistogramTest(unittest.TestCase):
    def test_nearest_rank(self):
        counts = collections.Counter(range(1, 101))
        for percent in [10, 50, 90, 99]:
            self.assertEqual(histogram.quantile(counts, percent), percent)
        self.assertEqual(histogram.quantile(collections.Counter({0: 1}), 99), 0)

    def test_counts_bytes_and_shares(self):
        hist = {"pods": collections.Counter({10: 2, 30: 1}),
                "secrets": collections.Counter({100: 1})}
        result = list(histogram.rows(hist))
        summaries = [r for r in result if r["row"] == "summary"]
        self.assertEqual(sum(r["count"] for r in summaries), 4)
        self.assertEqual(sum(r["value_bytes"] for r in summaries), 150)
        self.assertAlmostEqual(sum(float(r["object_share"]) for r in summaries), 1)
        self.assertAlmostEqual(sum(float(r["value_byte_share"]) for r in summaries), 1)
        for summary in summaries:
            buckets = [r for r in result if r["row"] == "bucket" and r["resource"] == summary["resource"]]
            self.assertEqual(sum(r["count"] for r in buckets), summary["count"])
            self.assertEqual(sum(r["size_bytes"] * r["count"] for r in buckets), summary["value_bytes"])
            self.assertAlmostEqual(sum(float(r["resource_bucket_share"]) for r in buckets), 1)

    def test_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = pathlib.Path(tmp) / "lengths.csv"
            dest = pathlib.Path(tmp) / "histogram.csv"
            source.write_text("resource,value_bytes\npods,0\npods,100\nsecrets,2\n")
            histogram.main(source, dest)
            with dest.open() as f:
                result = list(csv.DictReader(f))
            self.assertEqual(len(result), 5)
            self.assertEqual(result[0]["p50"], "0")
            self.assertEqual(result[0]["p99"], "100")
            source.write_text("resource,value_bytes\nprivate/path,3\n")
            with self.assertRaises(ValueError):
                histogram.main(source, dest)


if __name__ == "__main__":
    unittest.main()
