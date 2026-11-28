#!/usr/bin/env python3
"""Pre-correction paired leader profiles. Diagnostic only; never a gate attempt."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('profiles', HERE / 'fetch-final-profiles.py')
profiles = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profiles)
BASE = '55b32bb9e6ef692cdb9d1b0e3cdb16542811dec4'
HEAD = '0c41156a5dc285c1cffc79f80c11b725235031fd'
CASES = {
    'watch': dict(bench='watch-1k', seed=94125874, rtt=0),
    'txn': dict(bench='txn-1k', seed=94125874, rtt=2),
    'k8s': dict(bench='k8s', seed=613206478, rtt=0),
}
FUNCTIONS = 'serveRange|rangeKVs|resident|Resident|watchWire|linkedWatchWire|writeEvent|flushEvents|replyRoom|direct|Direct|Stage|stage|eachRevEvent|stateAt'


def run(case_name, out):
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise ValueError('Actions only')
    case = CASES[case_name]
    out = out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    refs = dict(baseline=BASE, candidate=HEAD)
    manifest = dict(scope='pre-correction-leader-CPU-DIAGNOSTIC-NOT-GATE',
                    status='INCOMPLETE', refs=refs, case=case, layout='3 0 1 2',
                    tooling_ref=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                    run_url=os.environ['FETCH_MEASURE_RUN_URL'], sides={})

    def save():
        (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

    save()
    bins = out / 'bins'
    bins.mkdir()
    subprocess.run(['go', 'build', '-o', str(bins / 'loadgen'), './cmd/loadgen'], check=True)
    for side, ref in refs.items():
        checkout = Path(os.environ['RUNNER_TEMP']) / f'profile-{side}'
        subprocess.run(['git', 'worktree', 'add', '--detach', str(checkout), ref], check=True)
        profiles.resident.prepare(checkout, out, side)
        subprocess.run(['go', 'build', '-o', str(bins / side), './cmd/tigerstore'], cwd=checkout, check=True)
    shutil.copyfile(os.environ['FETCH_ETCDCTL'], bins / 'etcdctl')
    (bins / 'etcdctl').chmod(0o755)
    certs = out / 'certs'
    subprocess.run(['bash', 'scripts/dev-certs.sh', str(certs)], check=True)
    env = dict(os.environ, CERTS=str(certs), BINS=str(bins), LOADGEN=str(bins / 'loadgen'),
               BENCH_GOMEMLIMIT='3GiB', BENCH_FLAGS='--memory-budget-bytes 3221225472 --enable-pprof',
               ETCD_UNSAFE_LOG_RING_BYTES='536870912', FETCH_MEASURE_FIXED_LOAD='1',
               FETCH_MEASURE_BUSY_REPORT='1', MODES='max', SEEDS=str(case['seed']), REPS='1',
               LAYOUT='3 0 1 2', LAYOUTS='', LEAD='1', DATA=str(out / 'data'))
    subprocess.run(['sudo', 'bash', 'bench/netem.sh', 'on', str(case['rtt'])], check=True)
    try:
        for side in refs:
            profile = out / f'{side}.cpu.pprof'
            with (out / f'{side}.log').open('w') as log:
                process = subprocess.run(['bash', 'bench/run.sh', case['bench'], str(out / side)],
                                         env=dict(env, TARGETS=side, FETCH_PROFILE_LEADER=str(profile)),
                                         stdout=log, stderr=subprocess.STDOUT)
            row = dict(bench_exit=process.returncode,
                       failed_steps=[str(p.relative_to(out)) for p in (out / side).rglob('*.failed')])
            rawpath = out / side / case['bench'] / f'{side}-max-{case["seed"]}.1.json'
            if rawpath.exists():
                raw = json.loads(rawpath.read_text())
                row.update(raw_file=str(rawpath.relative_to(out)),
                           raw_sha256=hashlib.sha256(rawpath.read_bytes()).hexdigest(),
                           native_valid=raw.get('valid'), errors=raw.get('errors'),
                           invalid_causes=raw.get('invalid_causes'), throughput=raw.get('throughput'),
                           client_cores=raw.get('client_cores'), source_samples=raw.get('fetch_measure'))
            manifest['sides'][side] = row
            if profile.exists() and profile.stat().st_size:
                row['profile_sha256'] = hashlib.sha256(profile.read_bytes()).hexdigest()
                shutil.copyfile(bins / side, out / f'{side}.profile-bin')
                for name, flags in [('top', ['-top', '-nodefraction=0']),
                                    ('cum', ['-top', '-cum', '-nodefraction=0']),
                                    ('functions', ['-list=' + FUNCTIONS])]:
                    with (out / f'{side}-{name}.txt').open('w') as log:
                        listing = subprocess.run(['go', 'tool', 'pprof', *flags, str(bins / side), str(profile)],
                                                 stdout=log, stderr=subprocess.STDOUT)
                    if listing.returncode:
                        # A fetch-free baseline can have no samples in this optional
                        # source-list regex. Preserve that evidence; still profile B.
                        text = (out / f'{side}-{name}.txt').read_text()
                        if name != 'functions' or not text.startswith('no matches found for regexp:'):
                            listing.check_returncode()
                        row['function_listing'] = 'no matching sampled functions'
            save()
        if all('profile_sha256' in row for row in manifest['sides'].values()):
            manifest['status'] = 'DIAGNOSTIC_COMPLETE_NOT_GATE'
            save()
            return 0
        return 2
    finally:
        subprocess.run(['sudo', 'bash', 'bench/netem.sh', 'off'], check=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=CASES, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.case, args.out))
