#!/usr/bin/env python3
"""Actions runner for the manager's separate #897 option-A relative gate.

Uses the original adapter/overlays and the same ruling-A decision function.
The unchanged 1 s / 3 s design figures are #913 information, not a stop.
Five fresh AB/BA pairs; stop on any pair's throughput ratio <0.98.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HERE = Path(__file__).resolve().parent
original = load('original_adapter', HERE / 'fetch-original-measure.py')
policy = original.policy


def run(args):
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise ValueError('campaign execution is Actions-only')
    if not original.SHA.fullmatch(args.baseline) or not original.SHA.fullmatch(args.candidate) or args.baseline == args.candidate:
        raise ValueError('distinct immutable full refs are required')
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    pairs = []
    manifest = dict(schema=1, scope='original-workload-PR897-relative-only', policy='PR897-option-A',
                    noise_method=policy.NOISE_METHOD,
                    refs=dict(baseline=args.baseline, candidate=args.candidate),
                    race=args.race, sync_delay_ms=args.sync_delay_ms, pairs=pairs,
                    run_url=os.environ['FETCH_MEASURE_RUN_URL'], status='INCOMPLETE',
                    design_figure_policy='information only; #913 owns the 1 s / 3 s figures',
                    remaining_named_reuse_ceiling_gates='NOT_EVALUATED')
    def save():
        (out / 'manifest.json').write_text(json.dumps(manifest, indent=2, allow_nan=False)+'\n')
    save()
    for name in ('fetch-pr897-gate.py', 'fetch-pr897-measure.py', 'fetch-original-measure.py'):
        (out / name).write_bytes((HERE / name).read_bytes())
    paths = {}
    for side, ref in manifest['refs'].items():
        checkout = Path(os.environ['RUNNER_TEMP']) / f'fetch-{side}'
        manifest['stage'] = f'prepare/build {side}'
        save()
        original.command('git', 'worktree', 'add', '--detach', str(checkout), ref, cwd=args.root)
        original.prepare(args.root, checkout, out, side, args.sync_delay_ms)
        binary = out / f'{side}.test'
        with (out / f'{side}-build.log').open('w') as log:
            result = subprocess.run(['go', 'test', '-c', *(['-race'] if args.race else []), '-o', str(binary), './internal/wl'],
                                    cwd=checkout, stdout=log, stderr=subprocess.STDOUT, check=False)
        if result.returncode:
            manifest.update(stage=f'failed build {side}', reasons=[f'{side} overlay compilation failed'], failed_log=f'{side}-build.log')
            save()
            return 2
        paths[side] = binary
    for number in range(1, 6):
        order = 'AB' if number % 2 else 'BA'
        pair = dict(pair=number, order=order)
        manifest.update(stage=f'pair {number}', partial_pair=pair)
        save()
        sides = ('baseline', 'candidate') if order == 'AB' else ('candidate', 'baseline')
        for side in sides:
            name = f'{number:02}-{side}.log'
            env = dict(os.environ, FETCH_MEASURE_NOWAIT_REFUSED='1', GOMEMLIMIT='1536MiB')
            with (out / name).open('w') as log:
                result = subprocess.run([str(paths[side]), '-test.run', '^TestFetchOriginalMeasurement$', '-test.v', '-test.timeout', '6m'],
                                        stdout=log, stderr=subprocess.STDOUT, env=env, check=False)
            raw = (out / name).read_bytes()
            if result.returncode:
                manifest.update(status='RULING', reasons=[f'{side} correctness/measurement test failed in pair {number}'], failed_log=name)
                save()
                return 1
            try:
                pair[side] = original.adapt(raw.decode(), manifest['refs'][side], side, number,
                                            args.race, args.sync_delay_ms, name, hashlib.sha256(raw).hexdigest())
            except (KeyError, TypeError, ValueError) as error:
                manifest.update(status='INCOMPLETE', reasons=[str(error)], failed_log=name)
                save()
                return 2
            print(json.dumps(dict(pair=number, side=side, wall_ms=pair[side]['wall_ms'],
                                  throughput=pair[side]['throughput'], latency_ms=pair[side]['latency_ms'],
                                  refusals=pair[side]['refusals'])), flush=True)
            save()
        pair['design_figure'] = original.design_figure(pair, args.race)
        print(pair['design_figure']['message'], flush=True)
        pairs.append(pair)
        del manifest['partial_pair']
        report = original.decide(pairs, args.race)
        manifest.update(status=report['status'], report=report)
        save()
        print('PR897_GATE '+json.dumps(report), flush=True)
        if report['status'] == 'RULING':
            return 1
    return 0 if manifest['status'] == 'PASS' else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=HERE.parent)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--sync-delay-ms', type=int, choices=(0, 5), default=0)
    args = parser.parse_args()
    try:
        return run(args)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'PR897 measurement blocked: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
