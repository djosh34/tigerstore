#!/usr/bin/env python3
"""Round 2 ordinary faults with mandatory, checked in-fault LIST/watch probes."""
import datetime as dt
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('round2', ROOT / 'correct-round2/runner.py')
round2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(round2)
base = round2.base
base.DEADLINE = dt.datetime(2026, 10, 13, 21, 50, tzinfo=dt.timezone.utc)


class Controller(round2.Controller):
    def workload_extra_args(self):
        return super().workload_extra_args() + ['--list-checks']

    def phase(self, name, phase, **evidence):
        super().phase(name, phase, **evidence)
        if phase != 'exercised':
            return
        # The fault remains installed throughout this fresh stream and watch.
        # Choose a live, nonpaused member outside the outgoing cut if possible.
        candidates = []
        for i, node in self.nodes.items():
            state = base.proc_snapshot(node['p'].pid)
            if state['tasks'] and not any('\tT ' in s for s in state['tasks'].values()):
                candidates.append(i)
        candidates.sort(key=lambda i: (i in self.cuts, i))
        base.require(candidates, 'no live unpaused member for in-fault checker')
        i = candidates[0]
        self.event('fault-list-watch-start', fault=name, member=i,
                   semantics='serializable pinned LIST and fresh revision-1 watch replay during fault; concurrent linearizable LISTs run separately')
        path = self.out / f'probe-{name}.json'
        self.cmd(self.client_args('list-watch-probe', [i]) +
                 ['--serializable', '--out', str(path)], timeout=8)
        result = json.loads(path.read_text())
        base.require(result['passed'] and result['list']['passed'] and result['target'] > 1,
                     f'in-fault LIST/watch assertion failed: {result}')
        self.event('fault-list-watch-pass', fault=name, member=i, evidence=str(path),
                   target=result['target'], chunks=len(result['list']['chunks']),
                   watch_responses=len(result['watch_responses']))

    def workload_finished(self):
        super().workload_finished()
        lists = json.loads((self.out / 'workload/list-responses.json').read_text())
        base.require(lists and all(any(row['passed'] and row['endpoint'] == self.endpoint(i)
                                     for row in lists) for i in (1, 2, 3)),
                     'missing successful checked concurrent linearizable LIST on a member')
        # UTC aligns separate processes. All relative times are monotonic within
        # their process. Preserve both clock origins in the permanent evidence.
        origin = int(base.dt.datetime.fromisoformat(self.utc_origin).timestamp()*1e9)
        overlap = []
        for name in self.required:
            phases = self.faults[name]
            installed = origin + phases['installed']['monotonic_ns']
            healed = origin + phases['healed']['monotonic_ns']
            probe = json.loads((self.out / f'probe-{name}.json').read_text())
            base.require(installed <= probe['call_utc_ns'] < probe['return_utc_ns'] <= healed,
                         f'{name}: successful LIST/watch probe not wholly inside installed fault')
            linear = [row for row in lists if row['passed'] and
                      installed <= row['call_utc_ns'] < row['return_utc_ns'] <= healed]
            overlap.append({'fault': name, 'probe_pass': probe['passed'],
                            'serializable_probe_chunks': len(probe['list']['chunks']),
                            'linearizable_lists_inside_fault': len(linear),
                            'watch_target': probe['target']})
        base.dump(self.out / 'list-watch-overlap.json', overlap)
        self.event('list-watch-coverage-pass', overlaps=overlap, concurrent_lists=len(lists),
                   successful_concurrent_lists=sum(row['passed'] for row in lists))


if __name__ == '__main__':
    base.Controller = Controller
    sys.exit(base.main())
