"""Synthetic-only tests of the offline gate; no stores or measurements are run."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("fetch_gate", Path(__file__).with_name("fetch-measure-gate.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
REFS = dict(baseline="a" * 40, candidate="b" * 40)


def pair(index=0, ratio=1.05, case="range-1k", cache="evicted-cache", rtt_ms=0):
    cohort = "measured-workload"
    if case.startswith("reuse/"):
        _, name, delay, clients, size = case.split("/")
        cohort = dict(name=name, delay_ms=int(delay[:-2]), clients=int(clients[:-7]),
                      value_bytes=int(size[:-5]), same_member=True, same_version=True)
    settings = dict(seed=index + 1, member=1, offered_load="max", cache_preparation=dict(mode=cache,confirmed=True,dataset_sha256="c"*64),
                    cohort=cohort, go_memory_bytes=(3 << 30) if cache == "resident-3g" else (560 << 20),
                    memory_budget_bytes=3 << 30, cache=cache, rtt_ms=rtt_ms)
    base = dict(ref=REFS["baseline"], correct=True, generator_limited=False, ran_out=False,
                evidence=["artifact/synthetic-baseline.json"], throughput=100,
                latency_ms=dict(p95=1, p99=2, max=3), refusals=dict(total=0, max_per_range=0),
                fetch=dict(jobs=10, origin_fulfillment=10, same_consumer=2, distinct_request=1),
                settings=settings, ceiling_share=0.9)
    candidate = copy.deepcopy(base)
    candidate.update(ref=REFS["candidate"], throughput=100 * ratio,
                     evidence=["artifact/synthetic-candidate.json"], routing=dict(ranges=True, watches=True))
    return dict(id=f"{case}/{cache}/{rtt_ms}/{index}", case=case, cache=cache, rtt_ms=rtt_ms,
                order="AB" if index % 2 == 0 else "BA", baseline=base, candidate=candidate)


class FetchMeasurementGateTests(unittest.TestCase):
    def pairs(self, ratio=1.05, count=5, **kwargs):
        return [pair(i, ratio, **kwargs) for i in range(count)]

    def test_disabled_or_missing_routing_never_qualifies(self):
        for field in ("ranges", "watches"):
            row = pair()
            row["candidate"]["routing"][field] = False
            self.assertIn("routing", gate.qualify(row, REFS))
        row = pair()
        del row["candidate"]["routing"]
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_malformed_nested_shapes_are_incomplete_not_exceptions(self):
        for field in ("settings", "fetch", "latency_ms", "refusals", "routing"):
            for bad in (None, [], True, "invalid"):
                row = pair()
                row["candidate"][field] = bad
                self.assertIsNotNone(gate.qualify(row, REFS), (field, bad))
        for field in ("seed", "member"):
            row = pair()
            for run in (row["baseline"], row["candidate"]):
                run["settings"][field] = True
            self.assertIsNotNone(gate.qualify(row, REFS), field)

    def test_origin_and_same_consumer_hits_are_not_distinct_reuse(self):
        row = pair()
        row["baseline"]["fetch"].update(origin_fulfillment=999, same_consumer=999, distinct_request=0)
        self.assertIn("distinct-request", gate.qualify(row, REFS))
        row["baseline"]["fetch"].update(jobs=0, distinct_request=99)
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_matched_settings_and_immutable_refs(self):
        for name in gate.MATCHED:
            row = pair()
            row["candidate"]["settings"][name] = "different"
            self.assertIsNotNone(gate.qualify(row, REFS), name)
        row = pair()
        row["candidate"]["ref"] = "main"
        self.assertIsNotNone(gate.qualify(row, REFS))
        row = pair()
        row["candidate"]["evidence"] = []
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_controls_do_not_require_read_cache_reuse(self):
        row = pair(case="put-1k")
        row["baseline"]["fetch"] = dict(jobs=0, origin_fulfillment=0, same_consumer=0, distinct_request=0)
        self.assertIsNone(gate.qualify(row, REFS))
        del row["candidate"]["ceiling_share"]
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_resident_is_control_not_cold_evidence(self):
        row = pair(cache="resident-3g")
        row["baseline"]["fetch"]["distinct_request"] = 0
        self.assertIsNone(gate.qualify(row, REFS))
        for run in (row["baseline"], row["candidate"]):
            run["settings"]["go_memory_bytes"] = 560 << 20
        self.assertIsNotNone(gate.qualify(row, REFS))
        row = pair(cache="past-ram-560m")
        for run in (row["baseline"], row["candidate"]):
            run["settings"]["go_memory_bytes"] = 3 << 30
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_probe_settings_are_not_just_case_labels(self):
        row = pair(case=gate.PROBES[-1])
        self.assertIsNone(gate.qualify(row, REFS))
        for run in (row["baseline"], row["candidate"]):
            run["settings"]["cohort"]["same_version"] = False
        self.assertIsNotNone(gate.qualify(row, REFS))
        row = pair(case=gate.PROBES[-1])
        for run in (row["baseline"], row["candidate"]):
            run["settings"]["cohort"]["clients"] = 1
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_nonfinite_negative_and_boolean_metrics_are_invalid(self):
        for value in (float("nan"), float("inf"), -1, 0, True,10**500):
            row = pair()
            row["candidate"]["throughput"] = value
            self.assertIsNotNone(gate.qualify(row, REFS))
        row = pair()
        row["candidate"]["refusals"]["total"] = -1
        self.assertIsNotNone(gate.qualify(row, REFS))
        row = pair()
        row["candidate"]["latency_ms"]["max"] = 0.1
        self.assertIsNotNone(gate.qualify(row, REFS))

    def test_unconfirmed_cache_and_malformed_input(self):
        row=pair()
        for run in (row["baseline"],row["candidate"]):
            run["settings"]["cache_preparation"]["confirmed"]=False
        self.assertIsNotNone(gate.qualify(row,REFS))
        row=pair();row["rtt_ms"]=False
        self.assertIsNotNone(gate.qualify(row,REFS))
        for document in ([],dict(schema=1,refs=REFS,pairs=[False]),dict(schema=1,refs=REFS,pairs={})):
            with self.assertRaises(ValueError):
                gate.evaluate(document)

    def test_all_threshold_boundaries(self):
        self.assertEqual(gate.decide(self.pairs(0.979))["status"], "RULING")
        for ratio in (0.98, 1, 1.02):
            self.assertEqual(gate.decide(self.pairs(ratio))["status"], "MORE_PAIRS")
        self.assertEqual(gate.decide(self.pairs(1.021))["status"], "PASS")
        self.assertEqual(gate.decide(self.pairs(1, 10))["status"], "PASS")
        self.assertEqual(gate.decide(self.pairs(0.999, 10))["status"], "RULING")

    def test_no_cherry_picking_or_averaging_away_first_gate(self):
        rows = self.pairs(0.97) + [pair(i, 2) for i in range(5, 10)]
        self.assertEqual(gate.decide(rows)["status"], "RULING")
        self.assertEqual(gate.decide(self.pairs(1.05, 4))["status"], "INCOMPLETE")
        self.assertEqual(gate.decide(self.pairs(1.05, 6))["status"], "INCOMPLETE")
        self.assertEqual(gate.decide(self.pairs(1.05, 11))["status"], "INCOMPLETE")
        rows = self.pairs()
        rows[1]["order"] = "AB"
        self.assertEqual(gate.decide(rows)["status"], "INCOMPLETE")

    def test_tails_refusals_and_ceiling_override_throughput(self):
        for ratio in (1, 1.2):
            rows = self.pairs(ratio)
            for row in rows:
                row["candidate"]["latency_ms"]["max"] = 4
            self.assertEqual(gate.decide(rows)["status"], "RULING")
        rows = self.pairs(1.2)
        rows[0]["candidate"]["refusals"] = dict(total=1, max_per_range=1)
        self.assertEqual(gate.decide(rows)["status"], "RULING")
        rows = self.pairs(1.2, case="k8s/small")
        for row in rows:
            row["candidate"]["ceiling_share"] = 0.799
        self.assertEqual(gate.decide(rows)["status"], "RULING")
        for row in rows:
            row["candidate"]["ceiling_share"] = 0.8
        self.assertEqual(gate.decide(rows)["status"], "PASS")

    def test_empty_partial_invalid_and_failed_campaigns_never_pass(self):
        document = dict(schema=1, refs=REFS, pairs=[])
        self.assertEqual(gate.evaluate(document)["status"], "INCOMPLETE")
        document["pairs"] = self.pairs()
        result = gate.evaluate(document)
        self.assertEqual(result["status"], "INCOMPLETE")
        row = pair(5)
        row["candidate"]["correct"] = False
        document["pairs"].append(row)
        self.assertEqual(gate.evaluate(document)["status"], "RULING")
        document["pairs"] = [pair()]
        document["pairs"][0]["baseline"]["fetch"]["distinct_request"] = 0
        self.assertEqual(len(gate.evaluate(document)["invalid_pairs"]), 1)
        document["pairs"].append(copy.deepcopy(document["pairs"][0]))
        with self.assertRaises(ValueError):
            gate.evaluate(document)

    def test_complete_synthetic_coverage_is_required(self):
        rows = [pair(i, **cell) for cell in gate.plan() for i in range(5)]
        document = dict(schema=1, refs=REFS, pairs=rows)
        result = gate.evaluate(document)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["scope"],"named-and-reuse-comparisons-only")
        self.assertEqual(result["original_workload_gate"],"NOT_EVALUATED")
        self.assertEqual(result["required_cells"], len(gate.plan()))
        document["pairs"] = rows[:-1]
        self.assertEqual(gate.evaluate(document)["status"], "INCOMPLETE")

    def test_cli_dry_plan_and_rejection(self):
        tool = Path(__file__).with_name("fetch-measure-gate.py")
        result = subprocess.run([sys.executable, str(tool), "--plan"], check=True, capture_output=True, text=True)
        self.assertEqual(len(json.loads(result.stdout)["required_cells"]), len(gate.plan()))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text('{"schema":1,"refs":NaN}')
            result = subprocess.run([sys.executable, str(tool), str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout)["status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
