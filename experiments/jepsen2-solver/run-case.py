#!/usr/bin/env python3
"""One isolated Python solve and two independent verifiers per key."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time

from history_io import sha256
from projections import materialize

BASE = Path(__file__).resolve().parent
DEADLINE = dt.datetime.fromisoformat('2026-10-13T22:00:00+00:00').timestamp()


def before_deadline():
    return time.time() < DEADLINE - 5


def bounded_solver():
    # Only runner children, not host processes. A resource failure stays unknown.
    resource.setrlimit(resource.RLIMIT_AS, (4 * 1024**3, 4 * 1024**3))


def run(command, logfile, seconds, memory_limit=False):
    if not before_deadline():
        return 'deadline-not-started'
    timeout = min(seconds, max(1, DEADLINE - time.time() - 5))
    with logfile.open('w') as log:
        try:
            return subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                timeout=timeout, preexec_fn=bounded_solver if memory_limit else None).returncode
        except subprocess.TimeoutExpired:
            return 'external-timeout'


def main(args):
    assert os.environ.get('GITHUB_ACTIONS') == 'true', 'Runner required'
    assert before_deadline(), 'Deadline reached'
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if args.full:
        full = args.full.resolve()
        src_independent = args.independent.resolve() if args.independent else out / 'generated'
        name = full.parent.name
        expected_keys = None
        source = {'full': str(full), 'input_mode': 'full-history'}
    else:
        manifest = json.loads((args.saved / 'manifest.json').read_text())
        item = manifest[args.case]
        root = (args.saved / item['directory']).resolve()
        full = root / 'history.edn'
        src_independent = root / 'independent'
        name = item['case']
        expected_keys = item['keys']
        source = item
    (out / 'source.json').write_text(json.dumps(source, indent=2) + '\n')
    records = materialize(full, src_independent)
    keys = [r['key'] for r in records]
    if expected_keys is not None:
        assert keys == expected_keys, 'Manifest key mismatch'
    (out / 'projections.json').write_text(json.dumps(records, indent=2) + '\n')
    inputs = out / 'input'
    inputs.mkdir(exist_ok=True)
    shutil.copyfile(full, inputs / 'history.edn')
    for r in records:
        dest = inputs / 'independent' / str(r['key']) / 'history.edn'
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(r['path'], dest)
    full = inputs / 'history.edn'
    cp = args.classpath.read_text().strip()
    results = {}
    priority = [k for k in (3,) if k in keys]
    for key in priority + [k for k in keys if k not in priority]:
        directory = out / str(key)
        directory.mkdir(exist_ok=True)
        saved = inputs / 'independent' / str(key) / 'history.edn'
        result = {'key': key, 'valid': 'unknown', 'full_sha256': sha256(full),
                  'independent_sha256': sha256(saved)}
        if not before_deadline():
            result['cause'] = 'deadline-not-started'
        else:
            command = [sys.executable, str(BASE / 'solve.py'), str(full), str(saved),
                       str(key), str(directory), '--seconds', str(args.seconds),
                       '--restricted-seconds', str(args.restricted_seconds)]
            rc = run(command, directory / 'solve.log', args.seconds + args.restricted_seconds + 30, True)
            result['solver_exit'] = rc
            if (directory / 'solve.json').exists():
                result['solve'] = json.loads((directory / 'solve.json').read_text())
            if rc == 0 and (directory / 'witness.json').is_file():
                py_rc = run([sys.executable, str(BASE / 'verify.py'), str(full), str(saved),
                             str(directory / 'witness.json'), str(directory / 'python-validation.json')],
                            directory / 'python-validation.log', 60)
                clj_rc = run(['java', '-Xmx512m', '-cp', cp, 'clojure.main', '-m', 'offline.verify',
                              str(full), str(saved), str(directory / 'witness.edn'),
                              str(directory / 'clojure-validation.edn')],
                             directory / 'clojure-validation.log', 60)
                result['python_verifier_exit'] = py_rc
                result['clojure_verifier_exit'] = clj_rc
                if py_rc == clj_rc == 0 and (directory / 'python-validation.json').is_file() and (directory / 'clojure-validation.edn').is_file():
                    validation = json.loads((directory / 'python-validation.json').read_text())
                    assert validation['valid'] is True
                    result['valid'] = True
                    result['validation'] = validation
        results[str(key)] = result
        (directory / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        summary = {'case': name, 'valid': True if len(results) == len(keys) and all(r['valid'] is True for r in results.values()) else 'unknown',
                   'expected_keys': keys, 'results': results,
                   'coverage_complete': len(results) == len(keys)}
        (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps({'key': key, 'valid': result['valid'], 'solver_exit': result.get('solver_exit'),
                          'python_exit': result.get('python_verifier_exit'),
                          'clojure_exit': result.get('clojure_verifier_exit')}), flush=True)
    return 0 if summary['valid'] is True else 2


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--saved', type=Path)
    p.add_argument('--case', type=int, default=0)
    p.add_argument('--full', type=Path)
    p.add_argument('--independent', type=Path)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--classpath', type=Path, required=True)
    p.add_argument('--seconds', type=int, default=240)
    p.add_argument('--restricted-seconds', type=int, default=20)
    a = p.parse_args()
    assert a.saved or a.full
    sys.exit(main(a))
