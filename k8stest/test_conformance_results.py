import unittest

from conformance_results import summarize


class ResultsTest(unittest.TestCase):
    name = "[sig-api-machinery] example [Conformance]"

    def report(self, children="", name=None):
        name = self.name if name is None else name
        return f'<testcase name="[It] {name}" time="1">{children}</testcase>'

    def summary(self, cases, code=0):
        return summarize(f"<testsuite>{cases}</testsuite>", {self.name}, code)

    def test_complete_pass(self):
        self.assertTrue(self.summary(self.report())["passed"])

    def test_non_conformance_skips_are_allowed(self):
        cases = self.report() + self.report("<skipped/>", "not selected")
        self.assertTrue(self.summary(cases)["passed"])

    def test_fail_closed(self):
        for cases, code in [
            ("", 0),
            (self.report(), 1),
            (self.report(), 124),
            (self.report() * 2, 0),
            (self.report("<skipped/>"), 0),
            (self.report("<failure>timeout</failure>"), 0),
            (self.report("<error/>"), 0),
            (self.report() + self.report(name="unexpected [Conformance]"), 0),
            (self.report() + self.report("<failure/>", "ReportAfterSuite"), 0),
        ]:
            with self.subTest(cases=cases, code=code):
                self.assertFalse(self.summary(cases, code)["passed"])


if __name__ == "__main__":
    unittest.main()
