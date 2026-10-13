import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("audit", Path(__file__).with_name("audit-evidence.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class EvidenceAuditTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run = self.root / "run"
        self.run.mkdir()
        self.dump("faults", {"required": ["cut"], "faults": {"cut": {phase: {} for phase in audit.PHASES}}})
        self.dump("result", {"controller_pass": True})
        names = ["original", "cold-missing-n1", "cold-missing-n2", "cold-missing-n3"]
        self.dump("oracles", [{"name": name} for name in names])
        self.dump("offline-results", [{"oracle": name, "mode": mode, "returncode": 0}
                                       for name in names for mode in ("log", "check")])
        for name in names:
            self.dump("check-" + name, [{"name": checker, "passed": True, "seconds": .01}
                                      for checker in sorted(audit.CHECKERS)])
        (self.root / "manifest.json").write_text('[{"path":"placeholder"}]')

    def dump(self, name, obj):
        (self.run / (name + ".json")).write_text(json.dumps(obj))

    def report(self):
        return audit.audit_case(self.root, verify_hashes=False)

    def test_complete(self):
        self.assertTrue(self.report()["audit_pass"])
        self.assertEqual(self.report()["checker_verdicts"], 16)

    def test_missing_report(self):
        (self.run / "check-original.json").unlink()
        self.assertFalse(self.report()["audit_pass"])

    def test_missing_fault_hit(self):
        self.dump("faults", {"required": ["cut"], "faults": {"cut": {"installed": {}}}})
        self.assertFalse(self.report()["audit_pass"])

    def test_checker_timeout(self):
        rows = [{"name": checker, "passed": checker != "Linearizable", "seconds": 30,
                 "error": "timed out" if checker == "Linearizable" else ""}
                for checker in sorted(audit.CHECKERS)]
        self.dump("check-original", rows)
        self.assertFalse(self.report()["audit_pass"])
        self.assertEqual(self.report()["checker_timeouts"], 1)

    def test_empty_offline_results_not_success(self):
        self.dump("offline-results", [])
        self.assertFalse(self.report()["audit_pass"])

    def test_failed_log_not_hidden_by_passing_checks(self):
        self.dump("offline-results", [{"oracle": "original", "mode": "log", "returncode": 1}])
        self.assertFalse(self.report()["audit_pass"])


if __name__ == "__main__":
    unittest.main()
